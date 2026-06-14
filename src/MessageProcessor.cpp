#include "MessageProcessor.hpp"
#include "JsonUtils.hpp"
#include "Shift.hpp"
#include "TimeUtils.hpp"
#include <memory>
#include <sstream>
#include <mutex>
#include <atomic>
#include <iostream>
using json = nlohmann::json;

static std::atomic<int> g_last_global_shift { -1 };

bool detect_global_shift_change(int currentShift)
{
    int prev = g_last_global_shift.load(std::memory_order_relaxed);
    if (prev == currentShift)
        return false;

    // First run or shift change
    g_last_global_shift.store(currentShift, std::memory_order_relaxed);
    return true;
}

// Firmware note: the previous Arduino firmware read pseudo-I2C words with a
// 1-bit right shift, halving all values. A workaround masked bit 15 and used
// 15-bit rollover (32768). With the corrected firmware, counters are full
// uint16_t — no masking, rollover at 65536.
// Example: prev=65530, curr=12 → delta=(uint16_t)(12-65530)=18  ✓
static uint16_t diff_counter(uint16_t curr, uint16_t prev, uint16_t max_valid = 5000) {
    // Full 16-bit unsigned subtraction handles rollover at 65536 automatically.
    uint16_t delta = static_cast<uint16_t>(curr - prev);

    if (delta > max_valid) {
        return 0;  // Implausible jump — likely counter reset or corruption
    }

    return delta;
}

// FIX: Safe wrapper that only updates 'last' when delta is valid.
// When LoRa devices intermittently send data from alternate PLC register banks,
// the raw value jumps to a completely different range. diff_counter correctly
// rejects these (delta > max_valid), but if we unconditionally update 'last'
// to the anomalous value, the NEXT valid reading also produces a huge delta
// and gets rejected too -- losing two intervals of real counts per anomaly.
//
// FIX v2: Added stale recovery. If max_rejects consecutive messages produce
// rejected deltas (delta > max_valid), force-reset prev_ref to curr.
// This prevents permanent lockout where a single corrupted reading poisons
// prev_ref into a range that makes ALL subsequent valid readings appear as
// huge deltas (via 16-bit wraparound), freezing the accumulator forever.
static uint16_t diff_counter_safe(uint16_t curr, uint16_t &prev_ref,
                                   uint8_t &reject_count,
                                   uint16_t max_valid = 5000,
                                   uint8_t max_rejects = 3) {
    uint16_t d = diff_counter(curr, prev_ref, max_valid);
    if (d > 0) {
        prev_ref = curr;
        reject_count = 0;
        return d;
    }
    // d == 0: either genuinely unchanged, or rejected by max_valid check
    // Distinguish: compute raw delta to see if it was a rejection.
    // Same unsigned-wrap arithmetic as diff_counter — no bit masking needed.
    uint16_t raw_delta = static_cast<uint16_t>(curr - prev_ref);
    if (raw_delta > max_valid) {
        // This was a rejection, not a genuine zero
        reject_count++;
        if (reject_count >= max_rejects) {
            // Stale recovery: prev_ref is stuck in an unrecoverable range.
            // Force-reset so next message can compute a valid delta.
            prev_ref = curr;
            reject_count = 0;
        }
    }
    // else: raw_delta == 0, genuinely no change -- don't increment reject_count
    return 0;
}


static Publication make_pub(const std::string &topic, const json &j)
{
    return Publication{topic, j.dump()};
}

/** Default processor: lightly normalize and forward a summary. */
class DefaultProcessor : public IMessageProcessor
{
public:
    std::vector<Publication> process(const json &msg, const std::string &isa95_prefix, int shift_mode = 3) override
    {
        json out;
        out["source"] = "celima/data";
        out["observed"] = msg;

        // Put some commonly useful fields if present
        if (auto dev = jsonu::get_opt<std::string>(msg, "devEUI"))
            out["devEUI"] = *dev;
        if (auto dn = jsonu::get_opt<std::string>(msg, "deviceName"))
            out["deviceName"] = *dn;
        if (auto dt = jsonu::get_opt<int>(msg, "deviceType"))
            out["deviceType"] = *dt;

        // Example: publish to a "production" topic
        auto t1 = isa95_prefix + "/production/line/quantity";
        json p1;
        p1["quantity"] = jsonu::get_opt<int>(msg, "cantidad").value_or(0);
        p1["ts"] = std::time(nullptr);

        // Example: publish to a "quality/alarms" topic
        auto t2 = isa95_prefix + "/quality/alarms";
        json p2;
        p2["alarms"] = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        p2["ts"] = std::time(nullptr);

        return {make_pub(t1, p1), make_pub(t2, p2)};
    }
};

/**
 * CalidadShiftAccumulatorProcessor (Updated for 3-minute accumulated data):
 * - Receives accumulated counts every 3 minutes (boxesQ1, boxesQ2, boxesQ6, totalBroken)
 * - Maintains per-shift accumulators for qualities 1, 2, 6 and discarded ("quebrados")
 * - Resets accumulators when the shift changes (S1->S2->S3->S1...)
 * - Thread-safe via a static mutex (safe even if processor instances are recreated)
 * 
 * Input JSON format (new):
 * {
 *   "lineID": 1,
 *   "deviceType": 8,
 *   "boxesQ1": 10,
 *   "boxesQ2": 5,
 *   "boxesQ6": 2,
 *   "totalBroken": 3
 * }
 * 
 * Output JSON format (unchanged):
 * {
 *   "maquina_id": 8,
 *   "timestamp_device": "2024-12-21T20:30:00Z",
 *   "shift": 1,
 *   "lineID": 1,
 *   "extra_c1": 150,    // accumulated Q1 for current shift
 *   "extra_c2": 80,     // accumulated Q2 for current shift
 *   "comercial": 40,    // accumulated Q6 for current shift
 *   "quebrados": 25     // accumulated broken for current shift
 * }
 */
class CalidadProcessor final : public IMessageProcessor {
    struct LineState {
        uint64_t acc_q1  = 0;
        uint64_t acc_q2  = 0;
        uint64_t acc_q6  = 0;
        uint64_t acc_discarded = 0;
        int shift = -1;
        bool initialized = false;
        // Duplicate-frame detection: Arduino retries after 30 s when ACK is lost.
        // Store the last accepted payload values + timestamp; reject identical
        // payloads arriving within DEDUP_WINDOW_SECS of the previous acceptance.
        uint64_t    last_delta_q1     = 0;
        uint64_t    last_delta_q2     = 0;
        uint64_t    last_delta_q6     = 0;
        uint64_t    last_delta_broken = 0;
        std::time_t last_accepted_time = 0;   // 0 = never accepted
    };
    
    static std::mutex mtx_;
    static std::unordered_map<int, LineState> states_;

public:
    static void reset_states();
    
    std::vector<Publication> process(const json& msg,
                                     const std::string& isa95_prefix,
                                     int shift_mode = 3) override {
        const int shift_now = static_cast<int>(current_shift_localtime(shift_mode));
        const int line_id   = msg.value("lineID", 0);
        
        // Extract accumulated counts from new payload format
        // Support both field names for backward compatibility
        uint64_t delta_q1 = 0;
        uint64_t delta_q2 = 0;
        uint64_t delta_q6 = 0;
        uint64_t delta_broken = 0;
        
        // NEW FORMAT: accumulated counts (3-minute intervals)
        if (msg.contains("boxesQ1")) {
            delta_q1 = msg.value("boxesQ1", 0);
            delta_q2 = msg.value("boxesQ2", 0);
            delta_q6 = msg.value("boxesQ6", 0);
            delta_broken = msg.value("totalBroken", 0);
        }
        // OLD FORMAT: single box event (backward compatibility)
        else if (msg.contains("cajaCalidad")) {
            const int cajaCalidad = msg.value("cajaCalidad", 0);
            if      (cajaCalidad == 1) delta_q1 = 1;
            else if (cajaCalidad == 2) delta_q2 = 1;
            else if (cajaCalidad == 6) delta_q6 = 1;
            
            const int quebrados = msg.contains("quebrados")
                                    ? msg.value("quebrados", 0)
                                    : msg.value("quebrado", 0);
            if (quebrados > 0) {
                delta_broken = static_cast<uint64_t>(quebrados);
            }
        }
        
        uint64_t q1, q2, q6, disc;
        {
            std::lock_guard<std::mutex> lock(mtx_);
            auto& st = states_[line_id];

            // First time or shift changed
            if (!st.initialized || st.shift != shift_now) {
                st = LineState();      // reset for this line
                st.initialized = true;
                st.shift = shift_now;
            }

            // Duplicate-frame rejection: Arduino retries the same LoRa frame after
            // ~30 s when the gateway ACK is lost. Identical payload within 120 s of
            // the last accepted frame is treated as a retry and silently discarded.
            constexpr int DEDUP_WINDOW_SECS = 120;
            const auto now = std::time(nullptr);
            if (st.last_accepted_time != 0 &&
                delta_q1     == st.last_delta_q1 &&
                delta_q2     == st.last_delta_q2 &&
                delta_q6     == st.last_delta_q6 &&
                delta_broken == st.last_delta_broken &&
                (now - st.last_accepted_time) < DEDUP_WINDOW_SECS) {
                std::cout << "[Calidad] Trama repetida descartada (lineID=" << line_id
                          << " dt=" << (now - st.last_accepted_time) << "s)\n";
                return {};
            }
            st.last_delta_q1     = delta_q1;
            st.last_delta_q2     = delta_q2;
            st.last_delta_q6     = delta_q6;
            st.last_delta_broken = delta_broken;
            st.last_accepted_time = now;

            // Add the deltas (accumulated counts from this message)
            st.acc_q1 += delta_q1;
            st.acc_q2 += delta_q2;
            st.acc_q6 += delta_q6;
            st.acc_discarded += delta_broken;
            
            // Snapshot current shift totals
            q1 = st.acc_q1;
            q2 = st.acc_q2;
            q6 = st.acc_q6;
            disc = st.acc_discarded;
        }
        
        // Output format remains unchanged
        json out;
        out["maquina_id"]       = 8;
        out["timestamp_device"] = device_timestamp(msg);
        out["shift"]            = shift_now;
        out["lineID"]           = line_id;
        out["extra_c1"]   = q1;
        out["extra_c2"]   = q2;
        out["comercial"]  = q6;
        out["quebrados"]  = disc;
        
        const auto t1 = isa95_prefix + std::to_string(line_id) + "/calidad/production";
        return { make_pub(t1, out) };
    }
};

// Static definitions
std::mutex CalidadProcessor::mtx_;
std::unordered_map<int, CalidadProcessor::LineState> CalidadProcessor::states_;

void CalidadProcessor::reset_states() {
    std::lock_guard<std::mutex> lock(mtx_);
    states_.clear();
}

// ============================================================================
// PrensaHidraulica1Processor - Fixed with correct counter handling
// ============================================================================
/**
 * PLC Register Mapping (from salida_prensa_1.pdf Sección12):
 * 
 * Input fields (from decoder, TODO: update decoder field names):
 * - "cantidadProductos" = D29005 = PISADAS (press stroke count, NOT products!)
 * - "tiempoProduccion_ds" = D29006 = Metric time accumulator (DECISECONDS)
 * - "paradas" = D29003 = Stop event COUNT
 * - "tiempoParadas_s" = D29004 = Stop duration (SECONDS)
 * - "alarms" = D29002 = Status Lento (status bits)
 * 
 * The PLC calculates: Products = PISADAS × Fila × Pac
 * We apply: Products = PISADAS × factor_pisadas (per line)
 */
class PrensaHidraulica1Processor : public IMessageProcessor
{
    struct PH1State {
        bool initialized = false;
        int  shift       = -1;

        uint16_t    last_dd_pisadas        = 0;
        uint16_t    last_dd_tiempo         = 0;
        uint16_t    last_dd_paradas        = 0;
        uint16_t    last_dd_tiempo_paradas = 0;
        std::time_t last_accepted_time     = 0;

        // Counters are 16-bit with bit-15 validation
        uint16_t last_pisadas = 0;        // D29005 - PISADAS (press strokes)
        uint8_t  rc_pisadas = 0;
        uint32_t acc_pisadas = 0;

        uint16_t last_metrica_tiempo = 0; // D29006 - Metric time (deciseconds)
        uint8_t  rc_metrica_tiempo = 0;
        double   acc_metrica_tiempo_s = 0.0;

        uint16_t last_paradas_count = 0;  // D29003 - Stop count
        uint8_t  rc_paradas_count = 0;
        uint32_t acc_paradas_count = 0;

        uint16_t last_paradas_tiempo = 0; // D29004 - Stop time (seconds)
        uint8_t  rc_paradas_tiempo = 0;
        uint32_t acc_paradas_tiempo_s = 0;
    };

    static std::mutex mtx_;
    static std::unordered_map<int, PH1State> states_;

public:
    static void reset_states() {
        std::lock_guard<std::mutex> lock(mtx_);
        states_.clear();
    }

    std::vector<Publication> process(const json &msg,
                                     const std::string &isa95_prefix,
                                     int shift_mode = 3) override
    {
        auto sh = current_shift_localtime(shift_mode);
        int shiftNum = (sh == Shift::S1 ? 1 : (sh == Shift::S2 ? 2 : 3));

        // Read inputs from decoder
        // NOTE: Field names will change when decoder is updated
        // Current: cantidadProductos → Should be: pisadas
        int line          = jsonu::get_opt<int>(msg, "lineID").value_or(0);
        int alarms        = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        
        // D29005 - PISADAS (decoder currently calls this "cantidadProductos")
        int raw_pisadas   = jsonu::get_opt<int>(msg, "cantidadProductos").value_or(0);
        
        // D29006 - Metric time in deciseconds
        int raw_tiempo    = jsonu::get_opt<int>(msg, "tiempoProduccion_ds").value_or(0);
        
        // D29003 - Stop count
        int raw_paradas   = jsonu::get_opt<int>(msg, "paradas").value_or(0);
        
        // D29004 - Stop time in seconds
        int raw_tiempo_paradas = jsonu::get_opt<int>(msg, "tiempoParadas_s").value_or(0);

        // Cast to uint16_t
        uint16_t pisadas = static_cast<uint16_t>(raw_pisadas);
        uint16_t metrica_tiempo = static_cast<uint16_t>(raw_tiempo);
        uint16_t paradas_count = static_cast<uint16_t>(raw_paradas);
        uint16_t paradas_tiempo = static_cast<uint16_t>(raw_tiempo_paradas);

        // Output accumulators
        uint32_t acc_pisadas_out = 0;
        double   acc_metrica_tiempo_s_out = 0.0;
        uint32_t acc_paradas_count_out = 0;
        uint32_t acc_paradas_tiempo_s_out = 0;
        double   pisadas_min = 0.0;

        {
            std::lock_guard<std::mutex> lock(mtx_);
            PH1State &st = states_[line];

            if (!st.initialized || st.shift != shiftNum) {
                // New shift - initialize
                st = PH1State();
                st.initialized = true;
                st.shift = shiftNum;
                st.last_pisadas = pisadas;
                st.last_metrica_tiempo = metrica_tiempo;
                st.last_paradas_count = paradas_count;
                st.last_paradas_tiempo = paradas_tiempo;
                st.last_dd_pisadas        = pisadas;
                st.last_dd_tiempo         = metrica_tiempo;
                st.last_dd_paradas        = paradas_count;
                st.last_dd_tiempo_paradas = paradas_tiempo;
                st.last_accepted_time     = std::time(nullptr);
            }
            else {
                // Duplicate-frame rejection: no timer1Hz available; compare all
                // four counter values + 120 s window (retry @ 30 s, interval @ 180 s).
                constexpr int DEDUP_WINDOW_SECS = 120;
                const auto now = std::time(nullptr);
                if (pisadas        == st.last_dd_pisadas        &&
                    metrica_tiempo == st.last_dd_tiempo         &&
                    paradas_count  == st.last_dd_paradas        &&
                    paradas_tiempo == st.last_dd_tiempo_paradas &&
                    (now - st.last_accepted_time) < DEDUP_WINDOW_SECS) {
                    std::cout << "[PH1] Trama repetida descartada (lineID=" << line << ")\n";
                    return {};
                }
                st.last_dd_pisadas        = pisadas;
                st.last_dd_tiempo         = metrica_tiempo;
                st.last_dd_paradas        = paradas_count;
                st.last_dd_tiempo_paradas = paradas_tiempo;
                st.last_accepted_time     = now;

                // Accumulate deltas using PLC-compatible validation

                // D29005 - PISADAS counter (use diff_counter with bit-15 validation)
                uint16_t delta_pisadas = diff_counter_safe(pisadas, st.last_pisadas, st.rc_pisadas);
                st.acc_pisadas += delta_pisadas;

                // D29006 - Metric time (deciseconds, bit-15 masked)
                uint16_t delta_tiempo = diff_counter_safe(metrica_tiempo, st.last_metrica_tiempo, st.rc_metrica_tiempo);
                st.acc_metrica_tiempo_s += delta_tiempo * 0.1;  // CF100 P_0_1s: 0.1s per tick (validated)

                // D29003 - Stop count counter
                uint16_t delta_paradas = diff_counter_safe(paradas_count, st.last_paradas_count, st.rc_paradas_count);
                st.acc_paradas_count += delta_paradas;

                // D29004 - Stop time counter (seconds)
                uint16_t delta_tiempo_paradas = diff_counter_safe(paradas_tiempo, st.last_paradas_tiempo, st.rc_paradas_tiempo);
                st.acc_paradas_tiempo_s += delta_tiempo_paradas;  // firmware Arduino ya corrige alineamiento de bit
            }

            // Copy out accumulated values
            acc_pisadas_out = st.acc_pisadas;
            acc_metrica_tiempo_s_out = st.acc_metrica_tiempo_s;
            acc_paradas_count_out = st.acc_paradas_count;
            acc_paradas_tiempo_s_out = st.acc_paradas_tiempo_s;

            // Calculate rate (pisadas per minute)
            if (acc_metrica_tiempo_s_out > 1.0) {
                pisadas_min = acc_pisadas_out / (acc_metrica_tiempo_s_out / 60.0);
            }
        }

        // Calculate products from pisadas using line-specific factor
        int factor_pisadas;
        switch (line) {
            case 1: factor_pisadas = L1_PIEZAS_PISADA; break;
            case 2: factor_pisadas = L2_PIEZAS_PISADA; break;
            case 3: factor_pisadas = L3_PIEZAS_PISADA; break;
            case 4: factor_pisadas = L4_PIEZAS_PISADA; break;
            case 5: factor_pisadas = L5_PIEZAS_PISADA; break;
            default: factor_pisadas = 3; break;
        }

        // Build output JSON
        json qual;
        qual["alarms"] = alarms;
        qual["timestamp_device"] = device_timestamp(msg);

        json prod;
        prod["maquina_id"] = 1;
        prod["turno"] = shiftNum;

        // Pisadas (primary counter from D29005)
        prod["cantidadProductos_raw"] = raw_pisadas;         // Raw value for debugging
        prod["cantidadProductos_instantaneo"] = pisadas;     // Current instantaneous
        prod["cantidadPisadas_turno"] = acc_pisadas_out;     // Accumulated pisadas
        prod["cantidadPisadas_min"] = static_cast<uint32_t>(pisadas_min);
        prod["cantidadProductos_turno"] = acc_pisadas_out * factor_pisadas;  // Calculated products

        // Production time (from D29006 - deciseconds converted to seconds)
        prod["tiempoProduccion_ds_instantaneo"] = metrica_tiempo;
        prod["tiempoProduccion_turno_s"] = static_cast<uint32_t>(acc_metrica_tiempo_s_out);

        // Paradas count (from D29003)
        prod["paradas_raw"] = raw_paradas;
        prod["paradas_instantaneo"] = paradas_count;
        prod["paradas_turno"] = acc_paradas_count_out;

        // Paradas time (from D29004 - already in seconds)
        prod["tiempoParadas_raw"] = raw_tiempo_paradas;
        prod["tiempoParadas_instantaneo"] = paradas_tiempo;
        prod["tiempoParadas_turno_s"] = acc_paradas_tiempo_s_out;

        prod["timestamp_device"] = device_timestamp(msg);

        auto t1 = isa95_prefix + std::to_string(line) + "/prensa_hidraulica1/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/prensa_hidraulica1/production";

        return {make_pub(t1, qual), make_pub(t2, prod)};
    }
};

// Static definitions
std::mutex PrensaHidraulica1Processor::mtx_;
std::unordered_map<int, PrensaHidraulica1Processor::PH1State>
    PrensaHidraulica1Processor::states_;

// ============================================================================
// PrensaHidraulica2Processor - Fixed with correct counter handling
// ============================================================================

class PrensaHidraulica2Processor : public IMessageProcessor
{
    struct PH2State {
        bool initialized = false;
        int  shift       = -1;

        uint16_t    last_dd_pisadas        = 0;
        uint16_t    last_dd_tiempo         = 0;
        uint16_t    last_dd_paradas        = 0;
        uint16_t    last_dd_tiempo_paradas = 0;
        std::time_t last_accepted_time     = 0;

        uint16_t last_pisadas = 0;
        uint8_t  rc_pisadas = 0;
        uint32_t acc_pisadas = 0;

        uint16_t last_metrica_tiempo = 0;
        uint8_t  rc_metrica_tiempo = 0;
        double   acc_metrica_tiempo_s = 0.0;

        uint16_t last_paradas_count = 0;
        uint8_t  rc_paradas_count = 0;
        uint32_t acc_paradas_count = 0;

        uint16_t last_paradas_tiempo = 0;
        uint8_t  rc_paradas_tiempo = 0;
        uint32_t acc_paradas_tiempo_s = 0;
    };

    static std::mutex mtx_;
    static std::unordered_map<int, PH2State> states_;

public:
    static void reset_states() {
        std::lock_guard<std::mutex> lock(mtx_);
        states_.clear();
    }

    std::vector<Publication> process(const json &msg,
                                     const std::string &isa95_prefix,
                                     int shift_mode = 3) override
    {
        auto sh = current_shift_localtime(shift_mode);
        int shiftNum = (sh == Shift::S1 ? 1 : (sh == Shift::S2 ? 2 : 3));

        int line          = jsonu::get_opt<int>(msg, "lineID").value_or(0);
        int alarms        = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int raw_pisadas   = jsonu::get_opt<int>(msg, "cantidadProductos").value_or(0);
        int raw_tiempo    = jsonu::get_opt<int>(msg, "tiempoProduccion_ds").value_or(0);
        int raw_paradas   = jsonu::get_opt<int>(msg, "paradas").value_or(0);
        int raw_tiempo_paradas = jsonu::get_opt<int>(msg, "tiempoParadas_s").value_or(0);

        uint16_t pisadas = static_cast<uint16_t>(raw_pisadas);
        uint16_t metrica_tiempo = static_cast<uint16_t>(raw_tiempo);
        uint16_t paradas_count = static_cast<uint16_t>(raw_paradas);
        uint16_t paradas_tiempo = static_cast<uint16_t>(raw_tiempo_paradas);

        uint32_t acc_pisadas_out = 0;
        double   acc_metrica_tiempo_s_out = 0.0;
        uint32_t acc_paradas_count_out = 0;
        uint32_t acc_paradas_tiempo_s_out = 0;
        double   pisadas_min = 0.0;

        {
            std::lock_guard<std::mutex> lock(mtx_);
            PH2State &st = states_[line];

            if (!st.initialized || st.shift != shiftNum) {
                st = PH2State();
                st.initialized = true;
                st.shift = shiftNum;
                st.last_pisadas = pisadas;
                st.last_metrica_tiempo = metrica_tiempo;
                st.last_paradas_count = paradas_count;
                st.last_paradas_tiempo = paradas_tiempo;
                st.last_dd_pisadas        = pisadas;
                st.last_dd_tiempo         = metrica_tiempo;
                st.last_dd_paradas        = paradas_count;
                st.last_dd_tiempo_paradas = paradas_tiempo;
                st.last_accepted_time     = std::time(nullptr);
            }
            else {
                // Duplicate-frame rejection
                constexpr int DEDUP_WINDOW_SECS = 120;
                const auto now = std::time(nullptr);
                if (pisadas        == st.last_dd_pisadas        &&
                    metrica_tiempo == st.last_dd_tiempo         &&
                    paradas_count  == st.last_dd_paradas        &&
                    paradas_tiempo == st.last_dd_tiempo_paradas &&
                    (now - st.last_accepted_time) < DEDUP_WINDOW_SECS) {
                    std::cout << "[PH2] Trama repetida descartada (lineID=" << line << ")\n";
                    return {};
                }
                st.last_dd_pisadas        = pisadas;
                st.last_dd_tiempo         = metrica_tiempo;
                st.last_dd_paradas        = paradas_count;
                st.last_dd_tiempo_paradas = paradas_tiempo;
                st.last_accepted_time     = now;

                // Use PLC-compatible counter validation
                uint16_t delta_pisadas = diff_counter_safe(pisadas, st.last_pisadas, st.rc_pisadas);
                st.acc_pisadas += delta_pisadas;

                uint16_t delta_tiempo = diff_counter_safe(metrica_tiempo, st.last_metrica_tiempo, st.rc_metrica_tiempo);
                st.acc_metrica_tiempo_s += delta_tiempo * 0.1;  // CF100 P_0_1s: 0.1s per tick (validated)

                uint16_t delta_paradas = diff_counter_safe(paradas_count, st.last_paradas_count, st.rc_paradas_count);
                st.acc_paradas_count += delta_paradas;

                uint16_t delta_tiempo_paradas = diff_counter_safe(paradas_tiempo, st.last_paradas_tiempo, st.rc_paradas_tiempo);
                st.acc_paradas_tiempo_s += delta_tiempo_paradas;  // firmware Arduino ya corrige alineamiento de bit
            }

            acc_pisadas_out = st.acc_pisadas;
            acc_metrica_tiempo_s_out = st.acc_metrica_tiempo_s;
            acc_paradas_count_out = st.acc_paradas_count;
            acc_paradas_tiempo_s_out = st.acc_paradas_tiempo_s;

            if (acc_metrica_tiempo_s_out > 1.0) {
                pisadas_min = acc_pisadas_out / (acc_metrica_tiempo_s_out / 60.0);
            }
        }

        int factor_pisadas;
        switch (line) {
            case 1: factor_pisadas = L1_PIEZAS_PISADA; break;
            case 2: factor_pisadas = L2_PIEZAS_PISADA; break;
            case 3: factor_pisadas = L3_PIEZAS_PISADA; break;
            case 4: factor_pisadas = L4_PIEZAS_PISADA; break;
            case 5: factor_pisadas = L5_PIEZAS_PISADA; break;
            default: factor_pisadas = 3; break;
        }

        json qual;
        qual["alarms"] = alarms;
        qual["timestamp_device"] = device_timestamp(msg);

        json prod;
        prod["maquina_id"] = 2;
        prod["turno"] = shiftNum;

        prod["cantidadProductos_raw"] = raw_pisadas;
        prod["cantidadProductos_instantaneo"] = pisadas;
        prod["cantidadPisadas_turno"] = acc_pisadas_out;
        prod["cantidadPisadas_min"] = static_cast<uint32_t>(pisadas_min);
        prod["cantidadProductos_turno"] = acc_pisadas_out * factor_pisadas;

        prod["tiempoProduccion_ds_instantaneo"] = metrica_tiempo;
        prod["tiempoProduccion_turno_s"] = static_cast<uint32_t>(acc_metrica_tiempo_s_out);

        prod["paradas_raw"] = raw_paradas;
        prod["paradas_instantaneo"] = paradas_count;
        prod["paradas_turno"] = acc_paradas_count_out;

        prod["tiempoParadas_raw"] = raw_tiempo_paradas;
        prod["tiempoParadas_instantaneo"] = paradas_tiempo;
        prod["tiempoParadas_turno_s"] = acc_paradas_tiempo_s_out;

        prod["timestamp_device"] = device_timestamp(msg);

        auto t1 = isa95_prefix + std::to_string(line) + "/prensa_hidraulica2/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/prensa_hidraulica2/production";

        return {make_pub(t1, qual), make_pub(t2, prod)};
    }
};

// Static definitions
std::mutex PrensaHidraulica2Processor::mtx_;
std::unordered_map<int, PrensaHidraulica2Processor::PH2State>
    PrensaHidraulica2Processor::states_;

/**
 * EntradaSecadorProcessor - CORRECTED VERSION
 * 
 * Changes from original:
 * 1. Reads NEW semantic field names from LoRaWAN decoder v2
 * 2. ALL registers use bit-15 flag — diff_counter for everything
 * 3. Tracks ALL fields from PLC (not just 2)
 * 4. Output JSON uses correct semantic names
 * 
 * Input fields (from decoder v2):
 *   checksum, timer1Hz, alarms, paradas_cantidad, paradas_tempo_s,
 *   ingreso_elevador_cantidad, ingreso_elevador_tiempo_ds,
 *   bancalino_l1_cantidad, bancalino_l1_tiempo_ds,
 *   bancalino_l2_cantidad, bancalino_l2_tiempo_ds
 * 
 * PLC Register mapping:
 *   D29003 = paradas_cantidad (stop events)
 *   D29004 = paradas_tempo_s (stop time in seconds)
 *   D29005 = ingreso_elevador_cantidad (elevator entry events)
 *   D29006 = ingreso_elevador_tiempo_ds (elevator time in deciseconds)
 *   D29007 = bancalino_l1_cantidad (bancalino L1 movements)
 *   D29008 = bancalino_l1_tiempo_ds (bancalino L1 time in deciseconds)
 *   D29009 = bancalino_l2_cantidad (bancalino L2 movements)
 *   D29010 = bancalino_l2_tiempo_ds (bancalino L2 time in deciseconds)
 */

class EntradaSecadorProcessor : public IMessageProcessor
{
private:
    struct State
    {
        bool initialized = false;
        int shift = 0;

        uint16_t last_accepted_timer1Hz = 0;

        // All 16-bit counters - last values and accumulators
        uint16_t last_timer1Hz = 0;
        uint8_t  rc_timer1Hz = 0;
        uint32_t acc_timer1Hz = 0;

        uint16_t last_paradas_cantidad = 0;
        uint8_t  rc_paradas_cantidad = 0;
        uint32_t acc_paradas_cantidad = 0;

        uint16_t last_paradas_tempo = 0;
        uint8_t  rc_paradas_tempo = 0;
        uint32_t acc_paradas_tempo_s = 0;

        uint16_t last_ingreso_elevador_cantidad = 0;
        uint8_t  rc_ingreso_elevador_cantidad = 0;
        uint32_t acc_ingreso_elevador_cantidad = 0;

        uint16_t last_ingreso_elevador_tiempo = 0;
        uint8_t  rc_ingreso_elevador_tiempo = 0;
        uint32_t acc_ingreso_elevador_tiempo_ds = 0;

        uint16_t last_bancalino_l1_cantidad = 0;
        uint8_t  rc_bancalino_l1_cantidad = 0;
        uint32_t acc_bancalino_l1_cantidad = 0;

        uint16_t last_bancalino_l1_tiempo = 0;
        uint8_t  rc_bancalino_l1_tiempo = 0;
        uint32_t acc_bancalino_l1_tiempo_ds = 0;

        uint16_t last_bancalino_l2_cantidad = 0;
        uint8_t  rc_bancalino_l2_cantidad = 0;
        uint32_t acc_bancalino_l2_cantidad = 0;

        uint16_t last_bancalino_l2_tiempo = 0;
        uint8_t  rc_bancalino_l2_tiempo = 0;
        uint32_t acc_bancalino_l2_tiempo_ds = 0;
    };

    static std::mutex mtx_;
    static std::unordered_map<int, State> states_;

public:
    static void reset_states() {
        std::lock_guard<std::mutex> lock(mtx_);
        states_.clear();
    }

    std::vector<Publication> process(const json &msg,
                                     const std::string &isa95_prefix,
                                     int shift_mode = 3) override
    {
        auto sh = current_shift_localtime(shift_mode);
        int shiftNum = (sh == Shift::S1 ? 1 : (sh == Shift::S2 ? 2 : 3));

        // === Read header fields ===
        int line = jsonu::get_opt<int>(msg, "lineID").value_or(0);
        int alarms = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int checksum = jsonu::get_opt<int>(msg, "checksum").value_or(0);
        int deviceType = jsonu::get_opt<int>(msg, "deviceType").value_or(0);

        // === Read all 16-bit counters (new semantic names from decoder v2) ===
        uint16_t timer1Hz = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "timer1Hz").value_or(0));

        uint16_t paradas_cantidad = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "paradas_cantidad").value_or(0));

        uint16_t paradas_tempo = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "paradas_tempo_s").value_or(0));

        uint16_t ingreso_elevador_cantidad = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "ingreso_elevador_cantidad").value_or(0));

        uint16_t ingreso_elevador_tiempo = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "ingreso_elevador_tiempo_ds").value_or(0));

        uint16_t bancalino_l1_cantidad = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "bancalino_l1_cantidad").value_or(0));

        uint16_t bancalino_l1_tiempo = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "bancalino_l1_tiempo_ds").value_or(0));

        uint16_t bancalino_l2_cantidad = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "bancalino_l2_cantidad").value_or(0));

        uint16_t bancalino_l2_tiempo = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "bancalino_l2_tiempo_ds").value_or(0));

        // === Output accumulators ===
        uint32_t acc_timer1Hz_out = 0;
        uint32_t acc_paradas_cantidad_out = 0;
        uint32_t acc_paradas_tempo_s_out = 0;
        uint32_t acc_ingreso_elevador_cantidad_out = 0;
        uint32_t acc_ingreso_elevador_tiempo_ds_out = 0;
        uint32_t acc_bancalino_l1_cantidad_out = 0;
        uint32_t acc_bancalino_l1_tiempo_ds_out = 0;
        uint32_t acc_bancalino_l2_cantidad_out = 0;
        uint32_t acc_bancalino_l2_tiempo_ds_out = 0;

        {
            std::lock_guard<std::mutex> lock(mtx_);
            State &st = states_[line];

            if (!st.initialized || st.shift != shiftNum) {
                // New shift - reset all accumulators and store initial values
                st = State();
                st.initialized = true;
                st.shift = shiftNum;

                st.last_accepted_timer1Hz = timer1Hz;
                st.last_timer1Hz = timer1Hz;
                st.last_paradas_cantidad = paradas_cantidad;
                st.last_paradas_tempo = paradas_tempo;
                st.last_ingreso_elevador_cantidad = ingreso_elevador_cantidad;
                st.last_ingreso_elevador_tiempo = ingreso_elevador_tiempo;
                st.last_bancalino_l1_cantidad = bancalino_l1_cantidad;
                st.last_bancalino_l1_tiempo = bancalino_l1_tiempo;
                st.last_bancalino_l2_cantidad = bancalino_l2_cantidad;
                st.last_bancalino_l2_tiempo = bancalino_l2_tiempo;
            }
            else {
                // Duplicate-frame rejection: timer1Hz is a free-running 1 Hz counter;
                // identical value guarantees same LoRa frame was retransmitted.
                if (timer1Hz == st.last_accepted_timer1Hz) {
                    std::cout << "[EntradaSecador] Trama repetida descartada (lineID=" << line
                              << " timer1Hz=" << timer1Hz << ")\n";
                    return {};
                }
                st.last_accepted_timer1Hz = timer1Hz;

                // Accumulate deltas — ALL PLC registers have bit-15 flag, use diff_counter for everything
                st.acc_timer1Hz += diff_counter_safe(timer1Hz, st.last_timer1Hz, st.rc_timer1Hz);

                st.acc_paradas_cantidad += diff_counter_safe(paradas_cantidad, st.last_paradas_cantidad, st.rc_paradas_cantidad);

                st.acc_paradas_tempo_s += diff_counter_safe(paradas_tempo, st.last_paradas_tempo, st.rc_paradas_tempo);

                st.acc_ingreso_elevador_cantidad += diff_counter_safe(ingreso_elevador_cantidad, st.last_ingreso_elevador_cantidad, st.rc_ingreso_elevador_cantidad);

                st.acc_ingreso_elevador_tiempo_ds += diff_counter_safe(ingreso_elevador_tiempo, st.last_ingreso_elevador_tiempo, st.rc_ingreso_elevador_tiempo);

                {
                    uint16_t d = diff_counter_safe(bancalino_l1_cantidad, st.last_bancalino_l1_cantidad, st.rc_bancalino_l1_cantidad);
                    st.acc_bancalino_l1_cantidad += (line == 2) ? d / 4 : d;
                }

                st.acc_bancalino_l1_tiempo_ds += diff_counter_safe(bancalino_l1_tiempo, st.last_bancalino_l1_tiempo, st.rc_bancalino_l1_tiempo);

                {
                    uint16_t d = diff_counter_safe(bancalino_l2_cantidad, st.last_bancalino_l2_cantidad, st.rc_bancalino_l2_cantidad);
                    st.acc_bancalino_l2_cantidad += (line == 2) ? d / 4 : d;
                }

                st.acc_bancalino_l2_tiempo_ds += diff_counter_safe(bancalino_l2_tiempo, st.last_bancalino_l2_tiempo, st.rc_bancalino_l2_tiempo);
            }

            // Copy accumulated values to output
            acc_timer1Hz_out = st.acc_timer1Hz;
            acc_paradas_cantidad_out = st.acc_paradas_cantidad;
            acc_paradas_tempo_s_out = st.acc_paradas_tempo_s;
            acc_ingreso_elevador_cantidad_out = st.acc_ingreso_elevador_cantidad;
            acc_ingreso_elevador_tiempo_ds_out = st.acc_ingreso_elevador_tiempo_ds;
            acc_bancalino_l1_cantidad_out = st.acc_bancalino_l1_cantidad;
            acc_bancalino_l1_tiempo_ds_out = st.acc_bancalino_l1_tiempo_ds;
            acc_bancalino_l2_cantidad_out = st.acc_bancalino_l2_cantidad;
            acc_bancalino_l2_tiempo_ds_out = st.acc_bancalino_l2_tiempo_ds;
        }

        // === Build output JSON with CORRECT semantic field names ===
        json prod;
        prod["maquina_id"] = 3;
        prod["turno"] = shiftNum;
        prod["deviceType"] = deviceType;
        prod["lineID"] = line;
        prod["checksum"] = checksum;

        // Timer/validation
        prod["timer1Hz_instantaneo"] = timer1Hz;
        prod["timer1Hz_turno"] = acc_timer1Hz_out;  // firmware Arduino ya corrige alineamiento de bit

        // Paradas (stops) - D29003, D29004
        prod["paradas_instantaneo"] = paradas_cantidad;
        prod["paradas_turno"] = acc_paradas_cantidad_out;
        prod["paradas_tiempo_instantaneo_s"] = paradas_tempo;
        prod["paradas_tiempo_turno_s"] = acc_paradas_tempo_s_out;  // firmware Arduino ya corrige alineamiento de bit

        // Ingreso Elevador - D29005, D29006
        prod["ingreso_elevador_instantaneo"] = ingreso_elevador_cantidad;
        prod["ingreso_elevador_turno"] = acc_ingreso_elevador_cantidad_out;
        prod["ingreso_elevador_tiempo_instantaneo_ds"] = ingreso_elevador_tiempo;
        prod["ingreso_elevador_tiempo_turno_ds"] = acc_ingreso_elevador_tiempo_ds_out;  // CF100: 1 tick = 0.1s = 1ds (validated, no ×2)

        // Bancalino Linea 1 - D29007, D29008
        prod["bancalino_l1_instantaneo"] = bancalino_l1_cantidad;
        prod["bancalino_l1_turno"] = acc_bancalino_l1_cantidad_out;  // firmware Arduino ya corrige alineamiento de bit
        prod["bancalino_l1_tiempo_instantaneo_ds"] = bancalino_l1_tiempo;
        prod["bancalino_l1_tiempo_turno_ds"] = acc_bancalino_l1_tiempo_ds_out;  // CF100: 1 tick = 0.1s = 1ds (validated, no ×2)

        // Bancalino Linea 2 - D29009, D29010
        prod["bancalino_l2_instantaneo"] = bancalino_l2_cantidad;
        prod["bancalino_l2_turno"] = acc_bancalino_l2_cantidad_out;  // firmware Arduino ya corrige alineamiento de bit
        prod["bancalino_l2_tiempo_instantaneo_ds"] = bancalino_l2_tiempo;
        prod["bancalino_l2_tiempo_turno_ds"] = acc_bancalino_l2_tiempo_ds_out;  // CF100: 1 tick = 0.1s = 1ds (validated, no ×2)

        prod["timestamp_device"] = device_timestamp(msg);

        // Alarms
        json qual;
        qual["alarms"] = alarms;
        qual["timestamp_device"] = device_timestamp(msg);

        auto t1 = isa95_prefix + std::to_string(line) + "/entrada_secador/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/entrada_secador/production";

        return { make_pub(t1, qual), make_pub(t2, prod) };
    }
};

// Static definitions
std::mutex EntradaSecadorProcessor::mtx_;
std::unordered_map<int, EntradaSecadorProcessor::State> EntradaSecadorProcessor::states_;



/**
 * SalidaSecadorProcessor - CORRECTED VERSION
 * 
 * Changes from original:
 * 1. Reads NEW semantic field names from LoRaWAN decoder v2
 * 2. ALL registers use bit-15 flag — diff_counter for everything
 * 3. Uses diff_counter() for ALL fields (bit-15 mask + 15-bit rollover)
 * 4. Output JSON uses correct semantic names
 * 
 * Input fields (from decoder v2):
 *   checksum, timer1Hz, alarms, parada_mds_cantidad, parada_mds_tiempo_s,
 *   metrica_mds_cantidad, metrica_mds_tiempo_ds
 * 
 * PLC Register mapping (Sección6):
 *   D29003 = parada_mds_cantidad (stop events)
 *   D29004 = parada_mds_tiempo_s (stop time in seconds)
 *   D29005 = metrica_mds_cantidad (MDS cycles - NOT products!)
 *   D29006 = metrica_mds_tiempo_ds (MDS cycle time in deciseconds)
 * 
 * MDS = Mesa de Descarga Secador (Dryer Discharge Table)
 */

class SalidaSecadorProcessor : public IMessageProcessor
{
private:
    struct State
    {
        bool initialized = false;
        int shift = 0;

        uint16_t last_accepted_timer1Hz = 0;

        // All 16-bit counters - last values and accumulators
        uint16_t last_timer1Hz = 0;
        uint8_t  rc_timer1Hz = 0;
        uint32_t acc_timer1Hz = 0;

        uint16_t last_parada_mds_cantidad = 0;
        uint8_t  rc_parada_mds_cantidad = 0;
        uint32_t acc_parada_mds_cantidad = 0;

        uint16_t last_parada_mds_tiempo = 0;
        uint8_t  rc_parada_mds_tiempo = 0;
        uint32_t acc_parada_mds_tiempo_s = 0;

        uint16_t last_metrica_mds_cantidad = 0;
        uint8_t  rc_metrica_mds_cantidad = 0;
        uint32_t acc_metrica_mds_cantidad = 0;

        uint16_t last_metrica_mds_tiempo = 0;
        uint8_t  rc_metrica_mds_tiempo = 0;
        uint32_t acc_metrica_mds_tiempo_ds = 0;
    };

    static std::mutex mtx_;
    static std::unordered_map<int, State> states_;

public:
    static void reset_states() {
        std::lock_guard<std::mutex> lock(mtx_);
        states_.clear();
    }

    std::vector<Publication> process(const json &msg,
                                     const std::string &isa95_prefix,
                                     int shift_mode = 3) override
    {
        auto sh = current_shift_localtime(shift_mode);
        int shiftNum = (sh == Shift::S1 ? 1 : (sh == Shift::S2 ? 2 : 3));

        // === Read header fields ===
        int line = jsonu::get_opt<int>(msg, "lineID").value_or(0);
        int alarms = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int checksum = jsonu::get_opt<int>(msg, "checksum").value_or(0);
        int deviceType = jsonu::get_opt<int>(msg, "deviceType").value_or(0);

        // === Read all 16-bit counters (new semantic names from decoder v2) ===
        uint16_t timer1Hz = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "timer1Hz").value_or(0));

        uint16_t parada_mds_cantidad = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "parada_mds_cantidad").value_or(0));

        uint16_t parada_mds_tiempo = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "parada_mds_tiempo_s").value_or(0));

        uint16_t metrica_mds_cantidad = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "metrica_mds_cantidad").value_or(0));

        uint16_t metrica_mds_tiempo = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "metrica_mds_tiempo_ds").value_or(0));

        // === Output accumulators ===
        uint32_t acc_timer1Hz_out = 0;
        uint32_t acc_parada_mds_cantidad_out = 0;
        uint32_t acc_parada_mds_tiempo_s_out = 0;
        uint32_t acc_metrica_mds_cantidad_out = 0;
        uint32_t acc_metrica_mds_tiempo_ds_out = 0;

        {
            std::lock_guard<std::mutex> lock(mtx_);
            State &st = states_[line];

            if (!st.initialized || st.shift != shiftNum) {
                // New shift - reset all accumulators and store initial values
                st = State();
                st.initialized = true;
                st.shift = shiftNum;

                st.last_accepted_timer1Hz = timer1Hz;
                st.last_timer1Hz = timer1Hz;
                st.last_parada_mds_cantidad = parada_mds_cantidad;
                st.last_parada_mds_tiempo = parada_mds_tiempo;
                st.last_metrica_mds_cantidad = metrica_mds_cantidad;
                st.last_metrica_mds_tiempo = metrica_mds_tiempo;
            }
            else {
                // Duplicate-frame rejection
                if (timer1Hz == st.last_accepted_timer1Hz) {
                    std::cout << "[SalidaSecador] Trama repetida descartada (lineID=" << line
                              << " timer1Hz=" << timer1Hz << ")\n";
                    return {};
                }
                st.last_accepted_timer1Hz = timer1Hz;

                // Accumulate deltas — ALL PLC registers have bit-15 flag, use diff_counter for everything
                st.acc_timer1Hz += diff_counter_safe(timer1Hz, st.last_timer1Hz, st.rc_timer1Hz);

                st.acc_parada_mds_cantidad += diff_counter_safe(parada_mds_cantidad, st.last_parada_mds_cantidad, st.rc_parada_mds_cantidad);

                st.acc_parada_mds_tiempo_s += diff_counter_safe(parada_mds_tiempo, st.last_parada_mds_tiempo, st.rc_parada_mds_tiempo);

                st.acc_metrica_mds_cantidad += diff_counter_safe(metrica_mds_cantidad, st.last_metrica_mds_cantidad, st.rc_metrica_mds_cantidad);

                st.acc_metrica_mds_tiempo_ds += diff_counter_safe(metrica_mds_tiempo, st.last_metrica_mds_tiempo, st.rc_metrica_mds_tiempo);
            }

            // Copy accumulated values to output
            acc_timer1Hz_out = st.acc_timer1Hz;
            acc_parada_mds_cantidad_out = st.acc_parada_mds_cantidad;
            acc_parada_mds_tiempo_s_out = st.acc_parada_mds_tiempo_s;
            acc_metrica_mds_cantidad_out = st.acc_metrica_mds_cantidad;
            acc_metrica_mds_tiempo_ds_out = st.acc_metrica_mds_tiempo_ds;
        }

        // === Build output JSON with CORRECT semantic field names ===
        json prod;
        prod["maquina_id"] = 4;
        prod["turno"] = shiftNum;
        prod["deviceType"] = deviceType;
        prod["lineID"] = line;
        prod["checksum"] = checksum;

        // Timer/validation
        prod["timer1Hz_instantaneo"] = timer1Hz;
        prod["timer1Hz_turno"] = acc_timer1Hz_out;  // firmware Arduino ya corrige alineamiento de bit

        // Parada MDS (stops) - D29003, D29004
        prod["parada_mds_instantaneo"] = parada_mds_cantidad;
        prod["parada_mds_turno"] = acc_parada_mds_cantidad_out;
        prod["parada_mds_tiempo_instantaneo_s"] = parada_mds_tiempo;
        prod["parada_mds_tiempo_turno_s"] = acc_parada_mds_tiempo_s_out;  // firmware Arduino ya corrige alineamiento de bit

        // Métrica MDS (cycles) - D29005, D29006
        // NOTE: These are MDS machine CYCLES, NOT product count!
        prod["metrica_mds_instantaneo"] = metrica_mds_cantidad;
        prod["metrica_mds_turno"] = acc_metrica_mds_cantidad_out;  // firmware Arduino ya corrige alineamiento de bit
        prod["metrica_mds_tiempo_instantaneo_ds"] = metrica_mds_tiempo;
        prod["metrica_mds_tiempo_turno_ds"] = acc_metrica_mds_tiempo_ds_out;  // CF100: 1 tick = 0.1s = 1ds (validated, no ×2)
        
        // Convert deciseconds to seconds for convenience
        prod["metrica_mds_tiempo_turno_s"] = static_cast<double>(acc_metrica_mds_tiempo_ds_out) * 0.1;  // CF100: each tick = 0.1s (validated)

        prod["timestamp_device"] = device_timestamp(msg);

        // Alarms
        json qual;
        qual["alarms"] = alarms;
        qual["timestamp_device"] = device_timestamp(msg);

        auto t1 = isa95_prefix + std::to_string(line) + "/salida_secador/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/salida_secador/production";

        return { make_pub(t1, qual), make_pub(t2, prod) };
    }
};

// Static definitions
std::mutex SalidaSecadorProcessor::mtx_;
std::unordered_map<int, SalidaSecadorProcessor::State> SalidaSecadorProcessor::states_;

/**
 * EsmalteProcessor - CORRECTED VERSION
 * 
 * Changes from original:
 * 1. Reads NEW semantic field names from LoRaWAN decoder v2
 * 2. ALL registers use bit-15 flag — diff_counter for everything
 * 3. Uses diff_counter() for ALL fields (bit-15 mask + 15-bit rollover)
 * 4. Output JSON uses correct semantic names
 * 
 * Input fields (from decoder v2):
 *   checksum, timer1Hz, alarms, parada_esm_cantidad, parada_esm_tiempo_s,
 *   metrica_esm_cantidad, metrica_esm_tiempo_ds
 * 
 * PLC Register mapping (Sección1):
 *   D29003 = parada_esm_cantidad (stop events)
 *   D29004 = parada_esm_tiempo_s (stop time in seconds)
 *   D29005 = metrica_esm_cantidad (ESM cycles - NOT products!)
 *   D29006 = metrica_esm_tiempo_ds (ESM cycle time in deciseconds)
 * 
 * ESM = Esmaltadora (Glazing Machine)
 */

class EsmalteProcessor : public IMessageProcessor
{
private:
    struct State
    {
        bool initialized = false;
        int shift = 0;

        uint16_t last_accepted_timer1Hz = 0;

        // All 16-bit counters - last values and accumulators
        uint16_t last_timer1Hz = 0;
        uint8_t  rc_timer1Hz = 0;
        uint32_t acc_timer1Hz = 0;

        uint16_t last_parada_esm_cantidad = 0;
        uint8_t  rc_parada_esm_cantidad = 0;
        uint32_t acc_parada_esm_cantidad = 0;

        uint16_t last_parada_esm_tiempo = 0;
        uint8_t  rc_parada_esm_tiempo = 0;
        uint32_t acc_parada_esm_tiempo_s = 0;

        uint16_t last_metrica_esm_cantidad = 0;
        uint8_t  rc_metrica_esm_cantidad = 0;
        uint32_t acc_metrica_esm_cantidad = 0;

        uint16_t last_metrica_esm_tiempo = 0;
        uint8_t  rc_metrica_esm_tiempo = 0;
        uint32_t acc_metrica_esm_tiempo_ds = 0;
    };

    static std::mutex mtx_;
    static std::unordered_map<int, State> states_;

public:
    static void reset_states() {
        std::lock_guard<std::mutex> lock(mtx_);
        states_.clear();
    }

    std::vector<Publication> process(const json &msg,
                                     const std::string &isa95_prefix,
                                     int shift_mode = 3) override
    {
        auto sh = current_shift_localtime(shift_mode);
        int shiftNum = (sh == Shift::S1 ? 1 : (sh == Shift::S2 ? 2 : 3));

        // === Read header fields ===
        int line = jsonu::get_opt<int>(msg, "lineID").value_or(0);
        int alarms = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int checksum = jsonu::get_opt<int>(msg, "checksum").value_or(0);
        int deviceType = jsonu::get_opt<int>(msg, "deviceType").value_or(0);

        // === Read all 16-bit counters (new semantic names from decoder v2) ===
        uint16_t timer1Hz = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "timer1Hz").value_or(0));

        uint16_t parada_esm_cantidad = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "parada_esm_cantidad").value_or(0));

        uint16_t parada_esm_tiempo = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "parada_esm_tiempo_s").value_or(0));

        uint16_t metrica_esm_cantidad = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "metrica_esm_cantidad").value_or(0));

        uint16_t metrica_esm_tiempo = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "metrica_esm_tiempo_ds").value_or(0));

        // === Output accumulators ===
        uint32_t acc_timer1Hz_out = 0;
        uint32_t acc_parada_esm_cantidad_out = 0;
        uint32_t acc_parada_esm_tiempo_s_out = 0;
        uint32_t acc_metrica_esm_cantidad_out = 0;
        uint32_t acc_metrica_esm_tiempo_ds_out = 0;

        {
            std::lock_guard<std::mutex> lock(mtx_);
            State &st = states_[line];

            if (!st.initialized || st.shift != shiftNum) {
                // New shift - reset all accumulators and store initial values
                st = State();
                st.initialized = true;
                st.shift = shiftNum;

                st.last_accepted_timer1Hz = timer1Hz;
                st.last_timer1Hz = timer1Hz;
                st.last_parada_esm_cantidad = parada_esm_cantidad;
                st.last_parada_esm_tiempo = parada_esm_tiempo;
                st.last_metrica_esm_cantidad = metrica_esm_cantidad;
                st.last_metrica_esm_tiempo = metrica_esm_tiempo;
            }
            else {
                // Duplicate-frame rejection
                if (timer1Hz == st.last_accepted_timer1Hz) {
                    std::cout << "[Esmalte] Trama repetida descartada (lineID=" << line
                              << " timer1Hz=" << timer1Hz << ")\n";
                    return {};
                }
                st.last_accepted_timer1Hz = timer1Hz;

                // Accumulate deltas — ALL PLC registers have bit-15 flag, use diff_counter for everything
                st.acc_timer1Hz += diff_counter_safe(timer1Hz, st.last_timer1Hz, st.rc_timer1Hz);

                st.acc_parada_esm_cantidad += diff_counter_safe(parada_esm_cantidad, st.last_parada_esm_cantidad, st.rc_parada_esm_cantidad);

                st.acc_parada_esm_tiempo_s += diff_counter_safe(parada_esm_tiempo, st.last_parada_esm_tiempo, st.rc_parada_esm_tiempo);

                st.acc_metrica_esm_cantidad += diff_counter_safe(metrica_esm_cantidad, st.last_metrica_esm_cantidad, st.rc_metrica_esm_cantidad);

                st.acc_metrica_esm_tiempo_ds += diff_counter_safe(metrica_esm_tiempo, st.last_metrica_esm_tiempo, st.rc_metrica_esm_tiempo);
            }

            // Copy accumulated values to output
            acc_timer1Hz_out = st.acc_timer1Hz;
            acc_parada_esm_cantidad_out = st.acc_parada_esm_cantidad;
            acc_parada_esm_tiempo_s_out = st.acc_parada_esm_tiempo_s;
            acc_metrica_esm_cantidad_out = st.acc_metrica_esm_cantidad;
            acc_metrica_esm_tiempo_ds_out = st.acc_metrica_esm_tiempo_ds;
        }

        // === Build output JSON with CORRECT semantic field names ===
        json prod;
        prod["maquina_id"] = 5;
        prod["turno"] = shiftNum;
        prod["deviceType"] = deviceType;
        prod["lineID"] = line;
        prod["checksum"] = checksum;

        // Timer/validation (D29001)
        prod["timer1Hz_instantaneo"] = timer1Hz;
        prod["timer1Hz_turno"] = acc_timer1Hz_out;  // firmware Arduino ya corrige alineamiento de bit

        // Parada ESM (stops) - D29003, D29004
        prod["parada_esm_instantaneo"] = parada_esm_cantidad;
        prod["parada_esm_turno"] = acc_parada_esm_cantidad_out;
        prod["parada_esm_tiempo_instantaneo_s"] = parada_esm_tiempo;
        prod["parada_esm_tiempo_turno_s"] = acc_parada_esm_tiempo_s_out;  // firmware Arduino ya corrige alineamiento de bit

        // Métrica ESM (cycles) - D29005, D29006
        // NOTE: These are ESM machine CYCLES, NOT product count!
        prod["metrica_esm_instantaneo"] = metrica_esm_cantidad;
        prod["metrica_esm_turno"] = acc_metrica_esm_cantidad_out;  // firmware Arduino ya corrige alineamiento de bit
        prod["metrica_esm_tiempo_instantaneo_ds"] = metrica_esm_tiempo;
        prod["metrica_esm_tiempo_turno_ds"] = acc_metrica_esm_tiempo_ds_out;  // CF100: 1 tick = 0.1s = 1ds (validated, no ×2)

        // Convert deciseconds to seconds for convenience
        prod["metrica_esm_tiempo_turno_s"] = static_cast<double>(acc_metrica_esm_tiempo_ds_out) * 0.1;  // CF100: each tick = 0.1s (validated)

        // Diagnostic log: confirm delta semantics after Arduino firmware fix
        std::cerr << "[ESM diag] line=" << line
                  << " metrica_esm_raw=" << metrica_esm_cantidad
                  << " acc_metrica_turno=" << acc_metrica_esm_cantidad_out
                  << " timer1Hz_raw=" << timer1Hz
                  << " acc_timer1Hz_turno=" << acc_timer1Hz_out
                  << " parada_esm_raw=" << parada_esm_cantidad
                  << " acc_parada_turno=" << acc_parada_esm_cantidad_out
                  << "\n";

        prod["timestamp_device"] = device_timestamp(msg);

        // Alarms
        json qual;
        qual["alarms"] = alarms;
        qual["timestamp_device"] = device_timestamp(msg);

        auto t1 = isa95_prefix + std::to_string(line) + "/esmalte/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/esmalte/production";

        return { make_pub(t1, qual), make_pub(t2, prod) };
    }
};

// Static definitions
std::mutex EsmalteProcessor::mtx_;
std::unordered_map<int, EsmalteProcessor::State> EsmalteProcessor::states_;

/**
 * EntradaHornoProcessor - CORRECTED VERSION v2
 * 
 * CRITICAL FIX: Previous decoder had wrong register mapping!
 * This processor reads from the CORRECTED decoder v2 field names.
 * 
 * Input fields (from decoder v2):
 *   checksum, timer1Hz, alarms, parada_mcf_cantidad, parada_mcf_tiempo_s,
 *   metrica_mcf_cantidad, metrica_mcf_tiempo_ds, numero_grades,
 *   metrica_formador_cantidad, metrica_formador_tiempo_ds,
 *   falha_forno_cantidad, falha_forno_tiempo_s
 * 
 * PLC Register mapping (Sección14):
 *   D29003 = parada_mcf_cantidad (MCF stop count)
 *   D29004 = parada_mcf_tiempo_s (MCF stop time, seconds)
 *   D29005 = metrica_mcf_cantidad (MCF cycle count)
 *   D29006 = metrica_mcf_tiempo_ds (MCF time, deciseconds)
 *   D29007 = numero_grades (PRODUCTION COUNT! BCD from D410)
 *   D29008 = metrica_formador_cantidad (formador cycle count)
 *   D29009 = metrica_formador_tiempo_ds (formador time, deciseconds)
 *   D29013 = falha_forno_cantidad (furnace failure count)
 *   D29014 = falha_forno_tiempo_s (furnace failure time, seconds)
 * 
 * MCF = Mesa de Carga Forno (Kiln Loading Table)
 * FORMADOR = Formador de Grades (Rack Former)
 */

class EntradaHornoProcessor : public IMessageProcessor
{
private:
    struct State
    {
        bool initialized = false;
        int shift = 0;

        uint16_t last_accepted_timer1Hz = 0;

        // Timer
        uint16_t last_timer1Hz = 0;
        uint8_t  rc_timer1Hz = 0;
        uint32_t acc_timer1Hz = 0;

        // Production: Número de Grades (D29007)
        uint16_t last_numero_grades = 0;
        uint8_t  rc_numero_grades = 0;
        uint32_t acc_numero_grades = 0;

        // Parada MCF - D29003, D29004
        uint16_t last_parada_mcf_cantidad = 0;
        uint8_t  rc_parada_mcf_cantidad = 0;
        uint32_t acc_parada_mcf_cantidad = 0;

        uint16_t last_parada_mcf_tiempo = 0;
        uint8_t  rc_parada_mcf_tiempo = 0;
        uint32_t acc_parada_mcf_tiempo_s = 0;

        // Métrica MCF - D29005, D29006
        uint16_t last_metrica_mcf_cantidad = 0;
        uint8_t  rc_metrica_mcf_cantidad = 0;
        uint32_t acc_metrica_mcf_cantidad = 0;

        uint16_t last_metrica_mcf_tiempo = 0;
        uint8_t  rc_metrica_mcf_tiempo = 0;
        uint32_t acc_metrica_mcf_tiempo_ds = 0;

        // Métrica Formador - D29008, D29009
        uint16_t last_metrica_formador_cantidad = 0;
        uint8_t  rc_metrica_formador_cantidad = 0;
        uint32_t acc_metrica_formador_cantidad = 0;

        uint16_t last_metrica_formador_tiempo = 0;
        uint8_t  rc_metrica_formador_tiempo = 0;
        uint32_t acc_metrica_formador_tiempo_ds = 0;

        // Falha Forno - D29013, D29014
        uint16_t last_falha_forno_cantidad = 0;
        uint8_t  rc_falha_forno_cantidad = 0;
        uint32_t acc_falha_forno_cantidad = 0;

        uint16_t last_falha_forno_tiempo = 0;
        uint8_t  rc_falha_forno_tiempo = 0;
        uint32_t acc_falha_forno_tiempo_s = 0;

        // Void detection: accumulated seconds where no units entered the furnace
        uint32_t acc_sin_entrada_s = 0;
    };

    static std::mutex mtx_;
    static std::unordered_map<int, State> states_;

public:
    static void reset_states() {
        std::lock_guard<std::mutex> lock(mtx_);
        states_.clear();
    }

    std::vector<Publication> process(const json &msg,
                                     const std::string &isa95_prefix,
                                     int shift_mode = 3) override
    {
        auto sh = current_shift_localtime(shift_mode);
        int shiftNum = (sh == Shift::S1 ? 1 : (sh == Shift::S2 ? 2 : 3));

        // === Read header fields ===
        int line = jsonu::get_opt<int>(msg, "lineID").value_or(0);
        int alarms = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int checksum = jsonu::get_opt<int>(msg, "checksum").value_or(0);
        int deviceType = jsonu::get_opt<int>(msg, "deviceType").value_or(0);

        // === Read all 16-bit counters (CORRECTED field names from decoder v2) ===
        
        uint16_t timer1Hz = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "timer1Hz").value_or(0));

        // PRODUCTION COUNT - D29007 (BCD converted from D410)
        uint16_t numero_grades = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "numero_grades").value_or(0));

        // Parada MCF - D29003, D29004
        uint16_t parada_mcf_cantidad = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "parada_mcf_cantidad").value_or(0));
        uint16_t parada_mcf_tiempo = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "parada_mcf_tiempo_s").value_or(0));

        // Métrica MCF - D29005, D29006
        uint16_t metrica_mcf_cantidad = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "metrica_mcf_cantidad").value_or(0));
        uint16_t metrica_mcf_tiempo = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "metrica_mcf_tiempo_ds").value_or(0));

        // Métrica Formador - D29008, D29009
        uint16_t metrica_formador_cantidad = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "metrica_formador_cantidad").value_or(0));
        uint16_t metrica_formador_tiempo = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "metrica_formador_tiempo_ds").value_or(0));

        // Falha Forno - D29013, D29014
        uint16_t falha_forno_cantidad = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "falha_forno_cantidad").value_or(0));
        uint16_t falha_forno_tiempo = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "falha_forno_tiempo_s").value_or(0));

        // === Output accumulators ===
        uint32_t acc_timer1Hz_out = 0;
        uint32_t acc_numero_grades_out = 0;
        uint32_t acc_parada_mcf_cantidad_out = 0;
        uint32_t acc_parada_mcf_tiempo_s_out = 0;
        uint32_t acc_metrica_mcf_cantidad_out = 0;
        uint32_t acc_metrica_mcf_tiempo_ds_out = 0;
        uint32_t acc_metrica_formador_cantidad_out = 0;
        uint32_t acc_metrica_formador_tiempo_ds_out = 0;
        uint32_t acc_falha_forno_cantidad_out = 0;
        uint32_t acc_falha_forno_tiempo_s_out = 0;
        uint32_t acc_sin_entrada_s_out = 0;

        {
            std::lock_guard<std::mutex> lock(mtx_);
            State &st = states_[line];

            if (!st.initialized || st.shift != shiftNum) {
                // New shift - reset all accumulators and store initial values
                st = State();
                st.initialized = true;
                st.shift = shiftNum;

                st.last_accepted_timer1Hz = timer1Hz;
                st.last_timer1Hz = timer1Hz;
                st.last_numero_grades = numero_grades;
                st.last_parada_mcf_cantidad = parada_mcf_cantidad;
                st.last_parada_mcf_tiempo = parada_mcf_tiempo;
                st.last_metrica_mcf_cantidad = metrica_mcf_cantidad;
                st.last_metrica_mcf_tiempo = metrica_mcf_tiempo;
                st.last_metrica_formador_cantidad = metrica_formador_cantidad;
                st.last_metrica_formador_tiempo = metrica_formador_tiempo;
                st.last_falha_forno_cantidad = falha_forno_cantidad;
                st.last_falha_forno_tiempo = falha_forno_tiempo;
            }
            else {
                // Duplicate-frame rejection
                if (timer1Hz == st.last_accepted_timer1Hz) {
                    std::cout << "[EntradaHorno] Trama repetida descartada (lineID=" << line
                              << " timer1Hz=" << timer1Hz << ")\n";
                    return {};
                }
                st.last_accepted_timer1Hz = timer1Hz;

                // Accumulate deltas — ALL PLC registers have bit-15 flag, use diff_counter for everything
                uint32_t delta_timer = diff_counter_safe(timer1Hz, st.last_timer1Hz, st.rc_timer1Hz);
                st.acc_timer1Hz += delta_timer;

                st.acc_numero_grades += diff_counter_safe(numero_grades, st.last_numero_grades, st.rc_numero_grades);

                st.acc_parada_mcf_cantidad += diff_counter_safe(parada_mcf_cantidad, st.last_parada_mcf_cantidad, st.rc_parada_mcf_cantidad);

                st.acc_parada_mcf_tiempo_s += diff_counter_safe(parada_mcf_tiempo, st.last_parada_mcf_tiempo, st.rc_parada_mcf_tiempo);

                uint32_t delta_mcf = diff_counter_safe(metrica_mcf_cantidad, st.last_metrica_mcf_cantidad, st.rc_metrica_mcf_cantidad);
                st.acc_metrica_mcf_cantidad += delta_mcf;

                st.acc_metrica_mcf_tiempo_ds += diff_counter_safe(metrica_mcf_tiempo, st.last_metrica_mcf_tiempo, st.rc_metrica_mcf_tiempo);

                st.acc_metrica_formador_cantidad += diff_counter_safe(metrica_formador_cantidad, st.last_metrica_formador_cantidad, st.rc_metrica_formador_cantidad);

                st.acc_metrica_formador_tiempo_ds += diff_counter_safe(metrica_formador_tiempo, st.last_metrica_formador_tiempo, st.rc_metrica_formador_tiempo);

                st.acc_falha_forno_cantidad += diff_counter_safe(falha_forno_cantidad, st.last_falha_forno_cantidad, st.rc_falha_forno_cantidad);

                st.acc_falha_forno_tiempo_s += diff_counter_safe(falha_forno_tiempo, st.last_falha_forno_tiempo, st.rc_falha_forno_tiempo);

                // Void detection: if no units entered this interval, count elapsed time as void
                // delta_mcf == 0 means sensor saw zero new pieces since last message
                if (delta_mcf == 0 && delta_timer > 0) {
                    st.acc_sin_entrada_s += delta_timer;  // firmware Arduino ya corrige alineamiento de bit
                }
            }

            // Copy accumulated values to output
            acc_timer1Hz_out = st.acc_timer1Hz;
            acc_numero_grades_out = st.acc_numero_grades;
            acc_parada_mcf_cantidad_out = st.acc_parada_mcf_cantidad;
            acc_parada_mcf_tiempo_s_out = st.acc_parada_mcf_tiempo_s;
            acc_metrica_mcf_cantidad_out = st.acc_metrica_mcf_cantidad;
            acc_metrica_mcf_tiempo_ds_out = st.acc_metrica_mcf_tiempo_ds;
            acc_metrica_formador_cantidad_out = st.acc_metrica_formador_cantidad;
            acc_metrica_formador_tiempo_ds_out = st.acc_metrica_formador_tiempo_ds;
            acc_falha_forno_cantidad_out = st.acc_falha_forno_cantidad;
            acc_falha_forno_tiempo_s_out = st.acc_falha_forno_tiempo_s;
            acc_sin_entrada_s_out = st.acc_sin_entrada_s;
        }

        // === Build output JSON with CORRECT semantic field names ===
        json prod;
        prod["maquina_id"] = 6;
        prod["turno"] = shiftNum;
        prod["deviceType"] = deviceType;
        prod["lineID"] = line;
        prod["checksum"] = checksum;

        // Timer/validation
        prod["timer1Hz_instantaneo"] = timer1Hz;
        prod["timer1Hz_turno"] = acc_timer1Hz_out;  // firmware Arduino ya corrige alineamiento de bit

        // PRODUCTION COUNT - numero_grades (D29007)
        prod["numero_grades_instantaneo"] = numero_grades;
        prod["numero_grades_turno"] = acc_numero_grades_out;

        // Parada MCF - D29003, D29004
        prod["parada_mcf_instantaneo"] = parada_mcf_cantidad;
        prod["parada_mcf_turno"] = acc_parada_mcf_cantidad_out;
        prod["parada_mcf_tiempo_instantaneo_s"] = parada_mcf_tiempo;
        prod["parada_mcf_tiempo_turno_s"] = acc_parada_mcf_tiempo_s_out;  // firmware Arduino ya corrige alineamiento de bit

        // Métrica MCF - D29005, D29006
        prod["metrica_mcf_instantaneo"] = metrica_mcf_cantidad;
        prod["metrica_mcf_turno"] = acc_metrica_mcf_cantidad_out;  // firmware Arduino ya corrige alineamiento de bit
        prod["metrica_mcf_tiempo_instantaneo_ds"] = metrica_mcf_tiempo;
        prod["metrica_mcf_tiempo_turno_ds"] = acc_metrica_mcf_tiempo_ds_out;  // CF100: 1 tick = 0.1s = 1ds (validated, no ×2)
        prod["metrica_mcf_tiempo_turno_s"] = static_cast<double>(acc_metrica_mcf_tiempo_ds_out) * 0.1;  // CF100: each tick = 0.1s (validated)

        // Métrica Formador - D29008, D29009
        prod["metrica_formador_instantaneo"] = metrica_formador_cantidad;
        prod["metrica_formador_turno"] = acc_metrica_formador_cantidad_out;  // firmware Arduino ya corrige alineamiento de bit
        prod["metrica_formador_tiempo_instantaneo_ds"] = metrica_formador_tiempo;
        prod["metrica_formador_tiempo_turno_ds"] = acc_metrica_formador_tiempo_ds_out;  // CF100: 1 tick = 0.1s = 1ds (validated, no ×2)
        prod["metrica_formador_tiempo_turno_s"] = static_cast<double>(acc_metrica_formador_tiempo_ds_out) * 0.1;  // CF100: each tick = 0.1s (validated)

        // Falha Forno - D29013, D29014
        prod["falha_forno_instantaneo"] = falha_forno_cantidad;
        prod["falha_forno_turno"] = acc_falha_forno_cantidad_out;
        prod["falha_forno_tiempo_instantaneo_s"] = falha_forno_tiempo;
        prod["falha_forno_tiempo_turno_s"] = acc_falha_forno_tiempo_s_out;  // firmware Arduino ya corrige alineamiento de bit

        // Void time: accumulated seconds where metrica_mcf_cantidad delta was 0
        // (no new units detected entering the furnace during that message interval)
        // Resets on shift change along with all other accumulators
        prod["sin_entrada_turno_s"] = acc_sin_entrada_s_out;

        prod["timestamp_device"] = device_timestamp(msg);

        // Alarms
        json qual;
        qual["alarms"] = alarms;
        qual["timestamp_device"] = device_timestamp(msg);

        auto t1 = isa95_prefix + std::to_string(line) + "/entrada_horno/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/entrada_horno/production";

        return { make_pub(t1, qual), make_pub(t2, prod) };
    }
};

// Static definitions
std::mutex EntradaHornoProcessor::mtx_;
std::unordered_map<int, EntradaHornoProcessor::State> EntradaHornoProcessor::states_;

/**
 * SalidaHornoProcessor - CORRECTED VERSION
 * 
 * Changes from original:
 * 1. Reads NEW semantic field names from LoRaWAN decoder
 * 2. ALL registers use bit-15 flag — diff_counter for everything
 * 3. Outputs OLD field names in JSON for backward compatibility
 * 
 * Input fields (from decoder v3):
 *   checksum, timer1Hz, alarms, paradas_cantidad, paradas_tempo,
 *   metrica_mdf_ciclos, metrica_mdf_tiempo, bancalinos_q301, bancalinos_q300,
 *   bancalinos_comb1, parada_escolha_cantidad, parada_escolha_tempo,
 *   sentido_escolha_cantidad, sentido_escolha_tiempo, barreira1_cantidad,
 *   barreira1_tiempo, bancalinos_comb2, bancalinos_total
 * 
 * Output fields (backward compatible):
 *   cantidad_*, bancalinos0_*, bancalinos1_*, paradas_1_*, paradas_2_*, etc.
 */

class SalidaHornoProcessor : public IMessageProcessor
{
private:
    struct State
    {
        bool initialized = false;
        int shift = 0;

        uint16_t last_accepted_timer1Hz = 0;

        // All 16-bit counters - last values and accumulators
        uint16_t last_timer1Hz = 0;
        uint8_t  rc_timer1Hz = 0;
        uint32_t acc_timer1Hz = 0;
        uint32_t acc_tiempo_operacion_s = 0;

        uint16_t last_paradas_cantidad = 0;
        uint8_t  rc_paradas_cantidad = 0;
        uint32_t acc_paradas_cantidad = 0;

        uint16_t last_paradas_tempo = 0;
        uint8_t  rc_paradas_tempo = 0;
        uint32_t acc_paradas_tempo = 0;

        uint16_t last_metrica_ciclos = 0;
        uint8_t  rc_metrica_ciclos = 0;
        uint32_t acc_metrica_ciclos = 0;

        uint16_t last_metrica_tiempo = 0;
        uint8_t  rc_metrica_tiempo = 0;
        uint32_t acc_metrica_tiempo = 0;

        uint16_t last_bancalinos_q301 = 0;
        uint8_t  rc_bancalinos_q301 = 0;
        uint32_t acc_bancalinos_q301 = 0;

        uint16_t last_bancalinos_q300 = 0;
        uint8_t  rc_bancalinos_q300 = 0;
        uint32_t acc_bancalinos_q300 = 0;

        uint16_t last_bancalinos_comb1 = 0;
        uint8_t  rc_bancalinos_comb1 = 0;
        uint32_t acc_bancalinos_comb1 = 0;

        uint16_t last_bancalinos_comb2 = 0;
        uint8_t  rc_bancalinos_comb2 = 0;
        uint32_t acc_bancalinos_comb2 = 0;

        uint16_t last_bancalinos_total = 0;
        uint8_t  rc_bancalinos_total = 0;
        uint32_t acc_bancalinos_total = 0;

        uint16_t last_parada_escolha_cantidad = 0;
        uint8_t  rc_parada_escolha_cantidad = 0;
        uint32_t acc_parada_escolha_cantidad = 0;

        uint16_t last_parada_escolha_tempo = 0;
        uint8_t  rc_parada_escolha_tempo = 0;
        uint32_t acc_parada_escolha_tempo = 0;

        uint16_t last_sentido_escolha_cantidad = 0;
        uint8_t  rc_sentido_escolha_cantidad = 0;
        uint32_t acc_sentido_escolha_cantidad = 0;

        uint16_t last_sentido_escolha_tiempo = 0;
        uint8_t  rc_sentido_escolha_tiempo = 0;
        uint32_t acc_sentido_escolha_tiempo = 0;

        uint16_t last_barreira1_cantidad = 0;
        uint8_t  rc_barreira1_cantidad = 0;
        uint32_t acc_barreira1_cantidad = 0;

        uint16_t last_barreira1_tiempo = 0;
        uint8_t  rc_barreira1_tiempo = 0;
        uint32_t acc_barreira1_tiempo = 0;
    };

    static std::mutex mtx_;
    static std::unordered_map<int, State> states_;

public:
    static void reset_states() {
        std::lock_guard<std::mutex> lock(mtx_);
        states_.clear();
    }

    std::vector<Publication> process(const json &msg,
                                     const std::string &isa95_prefix,
                                     int shift_mode = 3) override
    {
        auto sh = current_shift_localtime(shift_mode);
        int shiftNum = (sh == Shift::S1 ? 1 : (sh == Shift::S2 ? 2 : 3));

        // === Read header fields ===
        int line = jsonu::get_opt<int>(msg, "lineID").value_or(0);
        int alarms = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int checksum = jsonu::get_opt<int>(msg, "checksum").value_or(0);
        int deviceType = jsonu::get_opt<int>(msg, "deviceType").value_or(0);

        // === Read all 16-bit counters (new semantic names from decoder v3) ===
        uint16_t timer1Hz = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "timer1Hz").value_or(0));
        
        uint16_t paradas_cantidad = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "paradas_cantidad").value_or(0));
        
        uint16_t paradas_tempo = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "paradas_tempo").value_or(0));
        
        uint16_t metrica_ciclos = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "metrica_mdf_ciclos").value_or(0));
        
        uint16_t metrica_tiempo = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "metrica_mdf_tiempo").value_or(0));
        
        uint16_t bancalinos_q301 = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "bancalinos_q301").value_or(0));
        
        uint16_t bancalinos_q300 = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "bancalinos_q300").value_or(0));
        
        uint16_t bancalinos_comb1 = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "bancalinos_comb1").value_or(0));
        
        uint16_t bancalinos_comb2 = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "bancalinos_comb2").value_or(0));
        
        uint16_t bancalinos_total = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "bancalinos_total").value_or(0));
        
        uint16_t parada_escolha_cantidad = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "parada_escolha_cantidad").value_or(0));
        
        uint16_t parada_escolha_tempo = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "parada_escolha_tempo").value_or(0));
        
        uint16_t sentido_escolha_cantidad = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "sentido_escolha_cantidad").value_or(0));
        
        uint16_t sentido_escolha_tiempo = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "sentido_escolha_tiempo").value_or(0));
        
        uint16_t barreira1_cantidad = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "barreira1_cantidad").value_or(0));
        
        uint16_t barreira1_tiempo = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "barreira1_tiempo").value_or(0));

        // === Output accumulators ===
        uint32_t acc_timer1Hz_out = 0;
        uint32_t acc_tiempo_operacion_s_out = 0;
        uint32_t acc_paradas_cantidad_out = 0;
        uint32_t acc_paradas_tempo_out = 0;
        uint32_t acc_metrica_ciclos_out = 0;
        uint32_t acc_metrica_tiempo_out = 0;
        uint32_t acc_bancalinos_q301_out = 0;
        uint32_t acc_bancalinos_q300_out = 0;
        uint32_t acc_bancalinos_comb1_out = 0;
        uint32_t acc_bancalinos_comb2_out = 0;
        uint32_t acc_bancalinos_total_out = 0;
        uint32_t acc_parada_escolha_cantidad_out = 0;
        uint32_t acc_parada_escolha_tempo_out = 0;
        uint32_t acc_sentido_escolha_cantidad_out = 0;
        uint32_t acc_sentido_escolha_tiempo_out = 0;
        uint32_t acc_barreira1_cantidad_out = 0;
        uint32_t acc_barreira1_tiempo_out = 0;

        {
            std::lock_guard<std::mutex> lock(mtx_);
            State &st = states_[line];

            if (!st.initialized || st.shift != shiftNum) {
                // New shift - reset all accumulators and store initial values
                st = State();
                st.initialized = true;
                st.shift = shiftNum;

                st.last_accepted_timer1Hz = timer1Hz;
                st.last_timer1Hz = timer1Hz;
                st.last_paradas_cantidad = paradas_cantidad;
                st.last_paradas_tempo = paradas_tempo;
                st.last_metrica_ciclos = metrica_ciclos;
                st.last_metrica_tiempo = metrica_tiempo;
                st.last_bancalinos_q301 = bancalinos_q301;
                st.last_bancalinos_q300 = bancalinos_q300;
                st.last_bancalinos_comb1 = bancalinos_comb1;
                st.last_bancalinos_comb2 = bancalinos_comb2;
                st.last_bancalinos_total = bancalinos_total;
                st.last_parada_escolha_cantidad = parada_escolha_cantidad;
                st.last_parada_escolha_tempo = parada_escolha_tempo;
                st.last_sentido_escolha_cantidad = sentido_escolha_cantidad;
                st.last_sentido_escolha_tiempo = sentido_escolha_tiempo;
                st.last_barreira1_cantidad = barreira1_cantidad;
                st.last_barreira1_tiempo = barreira1_tiempo;
            }
            else {
                // Duplicate-frame rejection
                if (timer1Hz == st.last_accepted_timer1Hz) {
                    std::cout << "[SalidaHorno] Trama repetida descartada (lineID=" << line
                              << " timer1Hz=" << timer1Hz << ")\n";
                    return {};
                }
                st.last_accepted_timer1Hz = timer1Hz;

                // Accumulate deltas — ALL PLC registers have bit-15 flag, use diff_counter for everything
                uint16_t delta_timer = diff_counter_safe(timer1Hz, st.last_timer1Hz, st.rc_timer1Hz);
                st.acc_timer1Hz += delta_timer;
                st.acc_tiempo_operacion_s += delta_timer;

                st.acc_paradas_cantidad += diff_counter_safe(paradas_cantidad, st.last_paradas_cantidad, st.rc_paradas_cantidad);

                st.acc_paradas_tempo += diff_counter_safe(paradas_tempo, st.last_paradas_tempo, st.rc_paradas_tempo);

                st.acc_metrica_ciclos += diff_counter_safe(metrica_ciclos, st.last_metrica_ciclos, st.rc_metrica_ciclos);

                st.acc_metrica_tiempo += diff_counter_safe(metrica_tiempo, st.last_metrica_tiempo, st.rc_metrica_tiempo);

                st.acc_bancalinos_q301 += diff_counter_safe(bancalinos_q301, st.last_bancalinos_q301, st.rc_bancalinos_q301);

                st.acc_bancalinos_q300 += diff_counter_safe(bancalinos_q300, st.last_bancalinos_q300, st.rc_bancalinos_q300);

                st.acc_bancalinos_comb1 += diff_counter_safe(bancalinos_comb1, st.last_bancalinos_comb1, st.rc_bancalinos_comb1);

                st.acc_bancalinos_comb2 += diff_counter_safe(bancalinos_comb2, st.last_bancalinos_comb2, st.rc_bancalinos_comb2);

                st.acc_bancalinos_total += diff_counter_safe(bancalinos_total, st.last_bancalinos_total, st.rc_bancalinos_total);

                st.acc_parada_escolha_cantidad += diff_counter_safe(parada_escolha_cantidad, st.last_parada_escolha_cantidad, st.rc_parada_escolha_cantidad);

                st.acc_parada_escolha_tempo += diff_counter_safe(parada_escolha_tempo, st.last_parada_escolha_tempo, st.rc_parada_escolha_tempo);

                st.acc_sentido_escolha_cantidad += diff_counter_safe(sentido_escolha_cantidad, st.last_sentido_escolha_cantidad, st.rc_sentido_escolha_cantidad);

                st.acc_sentido_escolha_tiempo += diff_counter_safe(sentido_escolha_tiempo, st.last_sentido_escolha_tiempo, st.rc_sentido_escolha_tiempo);

                st.acc_barreira1_cantidad += diff_counter_safe(barreira1_cantidad, st.last_barreira1_cantidad, st.rc_barreira1_cantidad);

                st.acc_barreira1_tiempo += diff_counter_safe(barreira1_tiempo, st.last_barreira1_tiempo, st.rc_barreira1_tiempo);
            }

            // Copy accumulated values to output
            acc_timer1Hz_out = st.acc_timer1Hz;
            acc_tiempo_operacion_s_out = st.acc_tiempo_operacion_s;
            acc_paradas_cantidad_out = st.acc_paradas_cantidad;
            acc_paradas_tempo_out = st.acc_paradas_tempo;
            acc_metrica_ciclos_out = st.acc_metrica_ciclos;
            acc_metrica_tiempo_out = st.acc_metrica_tiempo;
            acc_bancalinos_q301_out = st.acc_bancalinos_q301;
            acc_bancalinos_q300_out = st.acc_bancalinos_q300;
            acc_bancalinos_comb1_out = st.acc_bancalinos_comb1;
            acc_bancalinos_comb2_out = st.acc_bancalinos_comb2;
            acc_bancalinos_total_out = st.acc_bancalinos_total;
            acc_parada_escolha_cantidad_out = st.acc_parada_escolha_cantidad;
            acc_parada_escolha_tempo_out = st.acc_parada_escolha_tempo;
            acc_sentido_escolha_cantidad_out = st.acc_sentido_escolha_cantidad;
            acc_sentido_escolha_tiempo_out = st.acc_sentido_escolha_tiempo;
            acc_barreira1_cantidad_out = st.acc_barreira1_cantidad;
            acc_barreira1_tiempo_out = st.acc_barreira1_tiempo;
        }

        // === Build output JSON with BACKWARD COMPATIBLE field names ===
        json prod;
        prod["maquina_id"] = 7;
        prod["turno"] = shiftNum;
        prod["deviceType"] = deviceType;
        prod["lineID"] = line;
        prod["checksum"] = checksum;

        // Timer/operation time
        prod["timer1Hz_instantaneo"] = timer1Hz;
        prod["tiempo_operacion_turno_s"] = acc_tiempo_operacion_s_out;  // firmware Arduino ya corrige alineamiento de bit

        // Main production counter (D25005 - metrica MDF ciclos)
        // OLD: cantidad → NEW: metrica_mdf_ciclos
        prod["cantidad_instantanea"] = metrica_ciclos;
        prod["cantidad_produccion_turno"] = acc_metrica_ciclos_out;  // firmware Arduino ya corrige alineamiento de bit

        // Cycle time accumulator (D25006 - metrica MDF tiempo)
        // OLD: cantidad_total → NEW: metrica_mdf_tiempo
        prod["cantidad_total_instantanea"] = metrica_tiempo;
        prod["cantidad_total_turno"] = acc_metrica_tiempo_out;  // CF100: 1 tick = 0.1s = 1ds (validated, no ×2)

        // Paradas cantidad (D25003)
        // OLD: paradas_1 → NEW: paradas_cantidad
        prod["paradas_1_instantaneo"] = paradas_cantidad;
        prod["paradas_1_turno"] = acc_paradas_cantidad_out;

        // Paradas tempo (D25004)
        // OLD: paradas_2 → NEW: paradas_tempo
        prod["paradas_2_instantaneo"] = paradas_tempo;
        prod["paradas_2_turno"] = acc_paradas_tempo_out;  // firmware Arduino ya corrige alineamiento de bit

        // Bancalinos Q:3.01 (D25007)
        // OLD: bancalinos0 → NEW: bancalinos_q301
        prod["bancalinos0_instantaneo"] = bancalinos_q301;
        prod["bancalinos0_turno"] = acc_bancalinos_q301_out;  // firmware Arduino ya corrige alineamiento de bit

        // Bancalinos Q:3.00 (D25008)
        // OLD: bancalinos1 → NEW: bancalinos_q300
        prod["bancalinos1_instantaneo"] = bancalinos_q300;
        prod["bancalinos1_turno"] = acc_bancalinos_q300_out;  // firmware Arduino ya corrige alineamiento de bit

        // Bancalinos Comb1: Q:3.01 AND I:1.09 (D25009)
        prod["bancalinosComb1_instantaneo"] = bancalinos_comb1;
        prod["bancalinosComb1_turno"] = acc_bancalinos_comb1_out;  // firmware Arduino ya corrige alineamiento de bit

        // Bancalinos Comb2: Q:3.01 AND Q:2.10 (D25016) - NOW INCLUDED!
        prod["bancalinosComb2_instantaneo"] = bancalinos_comb2;
        prod["bancalinosComb2_turno"] = acc_bancalinos_comb2_out;  // firmware Arduino ya corrige alineamiento de bit

        // Bancalinos Total: Q:3.00 AND Q:2.10 (D25017) - NOW INCLUDED!
        prod["bancalinosTotal_instantaneo"] = bancalinos_total;
        prod["bancalinosTotal_turno"] = acc_bancalinos_total_out;  // firmware Arduino ya corrige alineamiento de bit

        // Sentido Escolha (D25012, D25013)
        // OLD: cambioSentido → NEW: sentido_escolha
        prod["cambioSentido_instantaneo"] = sentido_escolha_cantidad;
        prod["cambioSentido_turno"] = acc_sentido_escolha_cantidad_out;
        prod["cambioSentidoTotal_instantaneo"] = sentido_escolha_tiempo;
        prod["cambioSentidoTotal_turno"] = acc_sentido_escolha_tiempo_out;  // CF100: 1 tick = 0.1s = 1ds (validated, no ×2)

        // Barreira 1 (D25014, D25015)
        // OLD: cambioBarrera → NEW: barreira1
        prod["cambioBarrera_instantaneo"] = barreira1_cantidad;
        prod["cambioBarrera_turno"] = acc_barreira1_cantidad_out;  // firmware Arduino ya corrige alineamiento de bit
        prod["cambioBarreraTotal_instantaneo"] = barreira1_tiempo;
        prod["cambioBarreraTotal_turno"] = acc_barreira1_tiempo_out;  // CF100: 1 tick = 0.1s = 1ds (validated, no ×2)

        // Parada Escolha (D25010, D25011) - NEW FIELDS
        prod["paradaEscolha_instantaneo"] = parada_escolha_cantidad;
        prod["paradaEscolha_turno"] = acc_parada_escolha_cantidad_out;
        prod["paradaEscolhaTempo_instantaneo"] = parada_escolha_tempo;
        prod["paradaEscolhaTempo_turno"] = acc_parada_escolha_tempo_out;  // firmware Arduino ya corrige alineamiento de bit

        prod["timestamp_device"] = device_timestamp(msg);

        // Alarms
        json qual;
        qual["alarms"] = alarms;
        qual["timestamp_device"] = device_timestamp(msg);

        auto t1 = isa95_prefix + std::to_string(line) + "/salida_horno/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/salida_horno/production";

        return { make_pub(t1, qual), make_pub(t2, prod) };
    }
};

// Static definitions
std::mutex SalidaHornoProcessor::mtx_;
std::unordered_map<int, SalidaHornoProcessor::State> SalidaHornoProcessor::states_;

// ============================================================================
// Factory functions
// ============================================================================

std::unique_ptr<IMessageProcessor> createDefaultProcessor()
{
    return std::make_unique<DefaultProcessor>();
}

std::unique_ptr<IMessageProcessor> createProcessor(DeviceType dt)
{
    switch (dt)
    {
    case DeviceType::PH_1:
        return std::make_unique<PrensaHidraulica1Processor>();
    case DeviceType::PH_2:
        return std::make_unique<PrensaHidraulica2Processor>();
    case DeviceType::Calidad:
        return std::make_unique<CalidadProcessor>();
    case DeviceType::Entrada_secador:
        return std::make_unique<EntradaSecadorProcessor>();
    case DeviceType::Salida_secador:
        return std::make_unique<SalidaSecadorProcessor>();
    case DeviceType::Esmalte:
        return std::make_unique<EsmalteProcessor>();
    case DeviceType::Entrada_horno:
        return std::make_unique<EntradaHornoProcessor>();
    case DeviceType::Salida_horno:
        return std::make_unique<SalidaHornoProcessor>();
    default:
        return std::make_unique<DefaultProcessor>();
    }
}

void reset_all_processor_states()
{
    PrensaHidraulica1Processor::reset_states();
    PrensaHidraulica2Processor::reset_states();
    SalidaSecadorProcessor::reset_states();
    EntradaSecadorProcessor::reset_states();
    EsmalteProcessor::reset_states();
    EntradaHornoProcessor::reset_states();
    SalidaHornoProcessor::reset_states();
    CalidadProcessor::reset_states();
}