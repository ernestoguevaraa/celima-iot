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

/**
 * Calculate counter delta with PLC-compatible validation.
 *
 * Based on PLC ladder analysis (Update_Qt_Ciclo function block):
 * - Counters are 16-bit (0-65535) with standard rollover
 * - PLC validates deltas using TST/TSTN on bit 15
 * - If bit 15 is SET in delta (value >= 32768) → invalid/overflow
 * - Additional sanity checks: Dados_Metricas uses <= 4000, Dados_Tempos uses <= 1200
 *
 * PLC Register Semantics (from ladder analysis):
 * - D29005 = PISADAS (press strokes), NOT products directly
 * - D29006 = Metric time accumulator in DECISECONDS (0.1s)
 * - D29003 = Parada COUNT (stop events)
 * - D29004 = Parada TIME in SECONDS
 *
 * @param curr Current counter value (16-bit)
 * @param prev Previous counter value (16-bit)
 * @param max_valid Maximum valid delta (default 20000, conservative for ~3 min intervals)
 * @return Delta to accumulate (0 if invalid)
 */
static uint16_t diff_counter(uint16_t curr, uint16_t prev, uint16_t max_valid = 20000) {
    // Standard 16-bit rollover-aware subtraction
    uint16_t delta;
    if (curr >= prev) {
        delta = curr - prev;
    } else {
        // Rollover occurred: curr wrapped past 65535
        delta = static_cast<uint16_t>(65536 - prev + curr);
    }
    
    // PLC validation: bit 15 must NOT be set (delta < 32768)
    // This matches TSTN(delta, &15) in the PLC Update_Qt_Ciclo function
    if (delta >= 32768) {
        return 0;  // Invalid - likely corruption or counter reset
    }
    
    // Sanity check: delta should be reasonable for message interval
    // PLC uses stricter limits (4000 for metrics, 1200 for times)
    // We use 20000 as a safe upper bound for ~3 minute intervals
    if (delta > max_valid) {
        return 0;  // Suspicious jump - ignore
    }
    
    return delta;
}

/**
 * Timer delta calculation for time accumulators.
 * 
 * Timers don't have the bit-15 validity issue since they increment
 * monotonically with time. Just handle 16-bit rollover.
 *
 * @param curr Current timer value (16-bit)
 * @param prev Previous timer value (16-bit)
 * @return Delta in timer units
 */
static uint16_t diff_timer(uint16_t curr, uint16_t prev) {
    if (curr >= prev) {
        return curr - prev;
    } else {
        // Rollover at 65536
        return static_cast<uint16_t>(65536 - prev + curr);
    }
}



static Publication make_pub(const std::string &topic, const json &j)
{
    return Publication{topic, j.dump()};
}

/** Default processor: lightly normalize and forward a summary. */
class DefaultProcessor : public IMessageProcessor
{
public:
    std::vector<Publication> process(const json &msg, const std::string &isa95_prefix) override
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
    };
    
    static std::mutex mtx_;
    static std::unordered_map<int, LineState> states_;

public:
    static void reset_states();
    
    std::vector<Publication> process(const json& msg,
                                     const std::string& isa95_prefix) override {
        const int shift_now = static_cast<int>(current_shift_localtime());
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
        out["timestamp_device"] = iso8601_utc_now();
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

        // Counters are 16-bit with bit-15 validation
        uint16_t last_pisadas = 0;        // D29005 - PISADAS (press strokes)
        uint32_t acc_pisadas = 0;

        uint16_t last_metrica_tiempo = 0; // D29006 - Metric time (deciseconds)
        double   acc_metrica_tiempo_s = 0.0;

        uint16_t last_paradas_count = 0;  // D29003 - Stop count
        uint32_t acc_paradas_count = 0;

        uint16_t last_paradas_tiempo = 0; // D29004 - Stop time (seconds)
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
                                     const std::string &isa95_prefix) override
    {
        auto sh = current_shift_localtime();
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
            }
            else {
                // Accumulate deltas using PLC-compatible validation

                // D29005 - PISADAS counter (use diff_counter with bit-15 validation)
                uint16_t delta_pisadas = diff_counter(pisadas, st.last_pisadas);
                st.acc_pisadas += delta_pisadas;
                st.last_pisadas = pisadas;

                // D29006 - Metric time is a timer (deciseconds → seconds)
                uint16_t delta_tiempo = diff_timer(metrica_tiempo, st.last_metrica_tiempo);
                st.acc_metrica_tiempo_s += delta_tiempo * 0.1;
                st.last_metrica_tiempo = metrica_tiempo;

                // D29003 - Stop count counter
                uint16_t delta_paradas = diff_counter(paradas_count, st.last_paradas_count);
                st.acc_paradas_count += delta_paradas;
                st.last_paradas_count = paradas_count;

                // D29004 - Stop time counter (seconds)
                uint16_t delta_tiempo_paradas = diff_counter(paradas_tiempo, st.last_paradas_tiempo);
                st.acc_paradas_tiempo_s += delta_tiempo_paradas;
                st.last_paradas_tiempo = paradas_tiempo;
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
        qual["timestamp_device"] = iso8601_utc_now();

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

        prod["timestamp_device"] = iso8601_utc_now();

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

        uint16_t last_pisadas = 0;
        uint32_t acc_pisadas = 0;

        uint16_t last_metrica_tiempo = 0;
        double   acc_metrica_tiempo_s = 0.0;

        uint16_t last_paradas_count = 0;
        uint32_t acc_paradas_count = 0;

        uint16_t last_paradas_tiempo = 0;
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
                                     const std::string &isa95_prefix) override
    {
        auto sh = current_shift_localtime();
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
            }
            else {
                // Use PLC-compatible counter validation
                uint16_t delta_pisadas = diff_counter(pisadas, st.last_pisadas);
                st.acc_pisadas += delta_pisadas;
                st.last_pisadas = pisadas;

                uint16_t delta_tiempo = diff_timer(metrica_tiempo, st.last_metrica_tiempo);
                st.acc_metrica_tiempo_s += delta_tiempo * 0.1;
                st.last_metrica_tiempo = metrica_tiempo;

                uint16_t delta_paradas = diff_counter(paradas_count, st.last_paradas_count);
                st.acc_paradas_count += delta_paradas;
                st.last_paradas_count = paradas_count;

                uint16_t delta_tiempo_paradas = diff_counter(paradas_tiempo, st.last_paradas_tiempo);
                st.acc_paradas_tiempo_s += delta_tiempo_paradas;
                st.last_paradas_tiempo = paradas_tiempo;
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
        qual["timestamp_device"] = iso8601_utc_now();

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

        prod["timestamp_device"] = iso8601_utc_now();

        auto t1 = isa95_prefix + std::to_string(line) + "/prensa_hidraulica2/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/prensa_hidraulica2/production";

        return {make_pub(t1, qual), make_pub(t2, prod)};
    }
};

// Static definitions
std::mutex PrensaHidraulica2Processor::mtx_;
std::unordered_map<int, PrensaHidraulica2Processor::PH2State>
    PrensaHidraulica2Processor::states_;

// ============================================================================
// EntradaSecadorProcessor - Fixed with correct counter handling
// ============================================================================

class EntradaSecadorProcessor : public IMessageProcessor
{
    struct State {
        bool initialized = false;
        int  shift = -1;

        uint16_t last_arranques = 0;
        uint32_t acc_arranques  = 0;

        uint16_t last_t_operacion = 0;
        uint32_t acc_t_operacion_s = 0;
    };

    static std::mutex mtx_;
    static std::unordered_map<int, State> states_;

public:
    static void reset_states();
    
    std::vector<Publication> process(const json &msg,
                                     const std::string &isa95_prefix) override
    {
        auto sh = current_shift_localtime();
        int shiftNum = (sh == Shift::S1 ? 1 : (sh == Shift::S2 ? 2 : 3));

        int lineID        = msg.value("lineID", 0);
        int alarms        = msg.value("alarms", 0);
        int arr_in        = msg.value("arranques", 0);
        int t_oper_s_in   = msg.value("tiempoOperacion_s", 0);

        uint32_t out_arranques = 0;
        uint32_t out_t_oper    = 0;

        uint16_t raw_arr    = static_cast<uint16_t>(arr_in);
        uint16_t raw_t_oper = static_cast<uint16_t>(t_oper_s_in);

        {
            std::lock_guard<std::mutex> lock(mtx_);
            State &st = states_[lineID];

            if (!st.initialized || st.shift != shiftNum) {
                st = State();
                st.initialized     = true;
                st.shift           = shiftNum;
                st.last_arranques     = raw_arr;
                st.last_t_operacion   = raw_t_oper;
            }
            else {
                // Use PLC-compatible counter validation
                st.acc_arranques += diff_counter(raw_arr, st.last_arranques);
                st.last_arranques = raw_arr;

                st.acc_t_operacion_s += diff_counter(raw_t_oper, st.last_t_operacion);
                st.last_t_operacion = raw_t_oper;
            }

            out_arranques = st.acc_arranques;
            out_t_oper    = st.acc_t_operacion_s;
        }

        json j_alarms;
        j_alarms["alarms"] = alarms;
        j_alarms["ts"] = iso8601_utc_now();

        json prod;
        prod["maquina_id"] = 3;
        prod["turno"] = shiftNum;
        prod["cantidad_arranques"] = out_arranques;
        prod["tiempo_operacion"]   = out_t_oper;
        prod["timestamp_device"]   = iso8601_utc_now();

        auto t1 = isa95_prefix + std::to_string(lineID) + "/entrada_secador/alarms";
        auto t2 = isa95_prefix + std::to_string(lineID) + "/entrada_secador/production";

        return {make_pub(t1, j_alarms), make_pub(t2, prod)};
    }
};

std::mutex EntradaSecadorProcessor::mtx_;
std::unordered_map<int, EntradaSecadorProcessor::State>
    EntradaSecadorProcessor::states_;

void EntradaSecadorProcessor::reset_states()
{
    std::lock_guard<std::mutex> lk(mtx_);
    states_.clear();
}

// ============================================================================
// SalidaSecadorProcessor - Fixed with correct counter handling
// ============================================================================

class SalidaSecadorProcessor : public IMessageProcessor
{
    struct State {
        bool initialized = false;
        int  shift       = -1;

        uint16_t last_prod_q = 0;
        uint32_t acc_prod_q = 0;

        uint16_t last_stop_q = 0;
        uint32_t acc_stop_q = 0;

        uint16_t last_raw_prod_t = 0;
        double   acc_prod_t_s = 0.0;

        uint16_t last_stop_t = 0;
        uint32_t acc_stop_t_s = 0;
    };

    static std::mutex mtx_;
    static std::unordered_map<int, State> states_;

public:
    static void reset_states();
    
    std::vector<Publication> process(const json &msg,
                                     const std::string &isa95_prefix) override
    {
        auto sh       = current_shift_localtime();
        int  shiftNum = (sh == Shift::S1 ? 1 : (sh == Shift::S2 ? 2 : 3));

        int alarms = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int prod_q_raw = jsonu::get_opt<int>(msg, "cantidadProductos").value_or(0);
        int prod_t_raw = jsonu::get_opt<int>(msg, "tiempoProduccion_ds").value_or(0);
        int line   = jsonu::get_opt<int>(msg, "lineID").value_or(0);
        int stop_q_raw = jsonu::get_opt<int>(msg, "paradas").value_or(0);
        int stop_t_raw = jsonu::get_opt<int>(msg, "tiempoParadas_s").value_or(0);

        uint16_t prod_q = static_cast<uint16_t>(prod_q_raw);
        uint16_t prod_t = static_cast<uint16_t>(prod_t_raw);
        uint16_t stop_q = static_cast<uint16_t>(stop_q_raw);
        uint16_t stop_t = static_cast<uint16_t>(stop_t_raw);

        uint32_t prod_q_shift   = 0;
        double   prod_t_shift_s = 0.0;
        uint32_t stop_q_shift   = 0;
        uint32_t stop_t_shift_s = 0;

        {
            std::lock_guard<std::mutex> lock(mtx_);
            State &st = states_[line];

            if (!st.initialized || st.shift != shiftNum) {
                st.initialized = true;
                st.shift       = shiftNum;
                st.last_prod_q = prod_q;
                st.acc_prod_q = 0;
                st.last_stop_q = stop_q;
                st.acc_stop_q = 0;
                st.last_raw_prod_t = prod_t;
                st.acc_prod_t_s = 0.0;
                st.last_stop_t = stop_t;
                st.acc_stop_t_s = 0;
            }
            else {
                // Use PLC-compatible counter validation
                st.acc_prod_q += diff_counter(prod_q, st.last_prod_q);
                st.last_prod_q = prod_q;

                st.acc_stop_q += diff_counter(stop_q, st.last_stop_q);
                st.last_stop_q = stop_q;

                // Timer for production time (deciseconds → seconds)
                uint16_t delta_t = diff_timer(prod_t, st.last_raw_prod_t);
                st.acc_prod_t_s += delta_t * 0.1;
                st.last_raw_prod_t = prod_t;

                st.acc_stop_t_s += diff_counter(stop_t, st.last_stop_t);
                st.last_stop_t = stop_t;
            }

            prod_q_shift   = st.acc_prod_q;
            prod_t_shift_s = st.acc_prod_t_s;
            stop_q_shift   = st.acc_stop_q;
            stop_t_shift_s = st.acc_stop_t_s;
        }

        json qual;
        qual["alarms"]           = alarms;
        qual["timestamp_device"] = iso8601_utc_now();

        json prod;
        prod["maquina_id"]          = 4;
        prod["turno"]               = shiftNum;
        prod["cantidad_produccion"] = prod_q_shift;
        prod["tiempo_produccion"]   = static_cast<uint32_t>(prod_t_shift_s);
        prod["cantidad_paradas"]    = stop_q_shift;
        prod["tiempo_paradas"]      = stop_t_shift_s;
        prod["timestamp_device"]    = iso8601_utc_now();

        auto t1 = isa95_prefix + std::to_string(line) + "/salida_secador/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/salida_secador/production";

        return { make_pub(t1, qual), make_pub(t2, prod) };
    }
};

std::mutex SalidaSecadorProcessor::mtx_;
std::unordered_map<int, SalidaSecadorProcessor::State>
    SalidaSecadorProcessor::states_;

void SalidaSecadorProcessor::reset_states()
{
    std::lock_guard<std::mutex> lk(mtx_);
    states_.clear();
}

// ============================================================================
// EsmalteProcessor - Fixed with correct counter handling
// ============================================================================

class EsmalteProcessor : public IMessageProcessor
{
    struct State {
        bool initialized = false;
        int shift = -1;

        uint16_t last_raw_prod_q = 0;
        uint32_t acc_prod_q = 0;

        uint16_t last_raw_stop_q = 0;
        uint32_t acc_stop_q = 0;

        uint16_t last_raw_prod_t = 0;
        double   acc_prod_t_s = 0.0;

        uint16_t last_raw_stop_t = 0;
        uint32_t acc_stop_t_s = 0;
    };

    static std::mutex mtx_;
    static std::unordered_map<int, State> states_;

public:
    static void reset_states();
    
    std::vector<Publication> process(const json &msg,
                                     const std::string &isa95_prefix) override
    {
        auto sh = current_shift_localtime();
        int shiftNum = (sh == Shift::S1 ? 1 : sh == Shift::S2 ? 2 : 3);

        int alarms   = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int prod_q   = jsonu::get_opt<int>(msg, "cantidadProductos").value_or(0);
        int prod_t   = jsonu::get_opt<int>(msg, "tiempoProduccion_ds").value_or(0);
        int line     = jsonu::get_opt<int>(msg, "lineID").value_or(0);
        int stop_q   = jsonu::get_opt<int>(msg, "paradas").value_or(0);
        int stop_t   = jsonu::get_opt<int>(msg, "tiempoParadas_s").value_or(0);

        uint32_t prod_q_shift = 0;
        uint32_t stop_q_shift = 0;
        double   prod_t_shift_s = 0.0;
        uint32_t stop_t_shift_s = 0;

        {
            std::lock_guard<std::mutex> lock(mtx_);
            State &st = states_[line];

            uint16_t raw_prod_q = static_cast<uint16_t>(prod_q);
            uint16_t raw_stop_q = static_cast<uint16_t>(stop_q);
            uint16_t raw_prod_t = static_cast<uint16_t>(prod_t);
            uint16_t raw_stop_t = static_cast<uint16_t>(stop_t);

            if (!st.initialized || st.shift != shiftNum) {
                st = State();
                st.initialized = true;
                st.shift = shiftNum;
                st.last_raw_prod_q = raw_prod_q;
                st.last_raw_stop_q = raw_stop_q;
                st.last_raw_prod_t = raw_prod_t;
                st.last_raw_stop_t = raw_stop_t;
            }
            else {
                // Use PLC-compatible counter validation
                st.acc_prod_q += diff_counter(raw_prod_q, st.last_raw_prod_q);
                st.last_raw_prod_q = raw_prod_q;

                st.acc_stop_q += diff_counter(raw_stop_q, st.last_raw_stop_q);
                st.last_raw_stop_q = raw_stop_q;

                // Timer for production time (deciseconds → seconds)
                uint16_t delta_t = diff_timer(raw_prod_t, st.last_raw_prod_t);
                st.acc_prod_t_s += delta_t * 0.1;
                st.last_raw_prod_t = raw_prod_t;

                st.acc_stop_t_s += diff_counter(raw_stop_t, st.last_raw_stop_t);
                st.last_raw_stop_t = raw_stop_t;
            }

            prod_q_shift      = st.acc_prod_q;
            stop_q_shift      = st.acc_stop_q;
            prod_t_shift_s    = st.acc_prod_t_s;
            stop_t_shift_s    = st.acc_stop_t_s;
        }

        json qual;
        qual["alarms"] = alarms;
        qual["timestamp_device"] = iso8601_utc_now();

        json prod;
        prod["maquina_id"]        = 5;
        prod["turno"]             = shiftNum;
        prod["cantidad_produccion"] = prod_q_shift;
        prod["tiempo_produccion"]   = static_cast<uint32_t>(prod_t_shift_s);
        prod["cantidad_paradas"]    = stop_q_shift;
        prod["tiempo_paradas"]      = stop_t_shift_s;
        prod["timestamp_device"]    = iso8601_utc_now();

        auto t1 = isa95_prefix + std::to_string(line) + "/esmalte/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/esmalte/production";

        return {make_pub(t1, qual), make_pub(t2, prod)};
    }
};

std::mutex EsmalteProcessor::mtx_;
std::unordered_map<int, EsmalteProcessor::State>
    EsmalteProcessor::states_;

void EsmalteProcessor::reset_states()
{
    std::lock_guard<std::mutex> lk(mtx_);
    states_.clear();
}

// ============================================================================
// EntradaHornoProcessor - Fixed with correct counter handling
// ============================================================================
/**
 * PLC Register Mapping (from entrada_horno.pdf Sección14):
 * 
 * This device has ADDITIONAL registers compared to standard devices:
 * - D29007 = Número de Grades (BCD-converted grid/rack count) - PRIMARY PRODUCTION
 * - D29008 = Métrica FORMADOR count
 * - D29009 = Métrica FORMADOR time accumulator (deciseconds)
 * - D29013 = Falha Forno count (oven fault count)
 * - D29014 = Falha Forno time (oven fault time in seconds)
 */
class EntradaHornoProcessor : public IMessageProcessor
{
    struct State {
        bool initialized = false;
        int  shift = -1;

        // D29007 - Número de Grades (BCD-converted)
        uint16_t last_raw_grades = 0;
        uint32_t acc_grades = 0;

        // D29003 - Paradas MCF count
        uint16_t last_raw_stops_q = 0;
        uint32_t acc_stops_q = 0;

        // D29004 - Paradas MCF time (seconds)
        uint16_t last_raw_stops_t = 0;
        uint32_t acc_stops_t_s = 0;

        // D29013 - Falha Forno count
        uint16_t last_raw_faults_q = 0;
        uint32_t acc_faults_q = 0;

        // D29014 - Falha Forno time (seconds)
        uint16_t last_raw_faults_t = 0;
        uint32_t acc_faults_t_s = 0;

        // D29005/D29006 - MCF Metrics (optional validation)
        uint16_t last_raw_mcf_metric = 0;
        double   acc_mcf_metric_s = 0.0;

        // D29008/D29009 - FORMADOR Metrics (optional validation)
        uint16_t last_raw_for_metric = 0;
        double   acc_for_metric_s = 0.0;
    };

    static std::mutex mtx_;
    static std::unordered_map<int, State> states_;

    // D29007 uses BCD-converted format (max 9999)
    static inline uint16_t cleanBCD(int x) {
        uint16_t val = static_cast<uint16_t>(x);
        return (val > 9999) ? 9999 : val;
    }

public:
    static void reset_states();
    
    std::vector<Publication> process(const json &msg,
                                     const std::string &isa95_prefix) override
    {
        auto sh = current_shift_localtime();
        int shiftNum = (sh == Shift::S1 ? 1 : sh == Shift::S2 ? 2 : 3);

        int line     = msg.value("lineID", 0);
        int devType  = msg.value("deviceType", 0);

        if (devType != 6) {
            std::cerr << "[EntradaHorno] WARNING: Wrong deviceType " 
                      << devType << " (expected 6)" << std::endl;
        }

        int status   = msg.value("status", 0);
        int timer    = msg.value("timer1Hz", 0);
        int grades   = msg.value("cantidadGrades", 0);
        int stops_q  = msg.value("paradas", 0);
        int stops_t  = msg.value("tiempoParadas_s", 0);
        int faults_q = msg.value("fallaHorno", 0);
        int faults_t = msg.value("tiempoFalla_s", 0);
        int mcf_metric = msg.value("metricaMCF", 0);
        int for_metric = msg.value("metricaFOR", 0);

        uint32_t out_grades = 0;
        uint32_t out_stops_q = 0;
        uint32_t out_faults_q = 0;
        uint32_t out_stops_t_s = 0;
        uint32_t out_faults_t_s = 0;
        double   out_mcf_metric_s = 0.0;
        double   out_for_metric_s = 0.0;

        uint16_t raw_grades  = cleanBCD(grades);
        uint16_t raw_stops_q  = static_cast<uint16_t>(stops_q);
        uint16_t raw_stops_t  = static_cast<uint16_t>(stops_t);
        uint16_t raw_faults_q = static_cast<uint16_t>(faults_q);
        uint16_t raw_faults_t = static_cast<uint16_t>(faults_t);
        uint16_t raw_mcf_metric = static_cast<uint16_t>(mcf_metric);
        uint16_t raw_for_metric = static_cast<uint16_t>(for_metric);

        if (raw_grades > 9900) {
            std::cerr << "[EntradaHorno] WARNING: Grade counter approaching BCD limit: " 
                      << raw_grades << " (max 9999)" << std::endl;
        }

        {
            std::lock_guard<std::mutex> lock(mtx_);
            State &st = states_[line];

            if (!st.initialized || st.shift != shiftNum) {
                st = State();
                st.initialized = true;
                st.shift = shiftNum;
                st.last_raw_grades     = raw_grades;
                st.last_raw_stops_q    = raw_stops_q;
                st.last_raw_stops_t    = raw_stops_t;
                st.last_raw_faults_q   = raw_faults_q;
                st.last_raw_faults_t   = raw_faults_t;
                st.last_raw_mcf_metric = raw_mcf_metric;
                st.last_raw_for_metric = raw_for_metric;

                std::cout << "[EntradaHorno] Line " << line 
                          << " - Shift " << shiftNum 
                          << " initialized (grades=" << raw_grades << ")" << std::endl;
            }
            else {
                // Use PLC-compatible counter validation
                uint16_t delta_grades = diff_counter(raw_grades, st.last_raw_grades);
                st.acc_grades += delta_grades;
                st.last_raw_grades = raw_grades;

                st.acc_stops_q += diff_counter(raw_stops_q, st.last_raw_stops_q);
                st.last_raw_stops_q = raw_stops_q;

                st.acc_stops_t_s += diff_counter(raw_stops_t, st.last_raw_stops_t);
                st.last_raw_stops_t = raw_stops_t;

                st.acc_faults_q += diff_counter(raw_faults_q, st.last_raw_faults_q);
                st.last_raw_faults_q = raw_faults_q;

                st.acc_faults_t_s += diff_counter(raw_faults_t, st.last_raw_faults_t);
                st.last_raw_faults_t = raw_faults_t;

                // MCF Metric timer (deciseconds → seconds)
                uint16_t delta_mcf = diff_timer(raw_mcf_metric, st.last_raw_mcf_metric);
                st.acc_mcf_metric_s += delta_mcf * 0.1;
                st.last_raw_mcf_metric = raw_mcf_metric;

                // FORMADOR Metric timer (deciseconds → seconds)
                uint16_t delta_for = diff_timer(raw_for_metric, st.last_raw_for_metric);
                st.acc_for_metric_s += delta_for * 0.1;
                st.last_raw_for_metric = raw_for_metric;

                if (delta_grades > 0) {
                    std::cout << "[EntradaHorno] Line " << line
                              << " - Produced " << delta_grades
                              << " grades (total: " << st.acc_grades << ")" << std::endl;
                }
            }

            out_grades       = st.acc_grades;
            out_stops_q      = st.acc_stops_q;
            out_faults_q     = st.acc_faults_q;
            out_stops_t_s    = st.acc_stops_t_s;
            out_faults_t_s   = st.acc_faults_t_s;
            out_mcf_metric_s = st.acc_mcf_metric_s;
            out_for_metric_s = st.acc_for_metric_s;
        }

        // Calculate empty furnace time
        double vacio_horno_sec = static_cast<double>(timer) 
                                 - out_mcf_metric_s 
                                 - static_cast<double>(out_stops_t_s) 
                                 - static_cast<double>(out_faults_t_s);
        double vacio_horno_min = (vacio_horno_sec > 0) ? (vacio_horno_sec / 60.0) : 0.0;

        json j_status;
        j_status["status"]    = status;
        j_status["timer"]     = timer;
        j_status["raw_grades"] = raw_grades;
        j_status["ts"]        = iso8601_utc_now();

        json j_prod;
        j_prod["maquina_id"] = 6;
        j_prod["turno"]      = shiftNum;
        j_prod["cantidad_produccion"] = out_grades;
        j_prod["cantidad_paradas"]    = out_stops_q;
        j_prod["tiempo_paradas"]      = out_stops_t_s;
        j_prod["cantidad_fallas"]     = out_faults_q;
        j_prod["tiempo_fallas"]       = out_faults_t_s;
        j_prod["tiempo_metrica_mcf"]  = (uint32_t)out_mcf_metric_s;
        j_prod["tiempo_metrica_for"]  = (uint32_t)out_for_metric_s;
        j_prod["vacio_horno_min"]     = vacio_horno_min;
        j_prod["timestamp_device"]    = iso8601_utc_now();

        auto topic_status = isa95_prefix + std::to_string(line) + "/entrada_horno/status";
        auto topic_prod   = isa95_prefix + std::to_string(line) + "/entrada_horno/production";

        return { 
            make_pub(topic_status, j_status), 
            make_pub(topic_prod, j_prod) 
        };
    }
};

std::mutex EntradaHornoProcessor::mtx_;
std::unordered_map<int, EntradaHornoProcessor::State>
    EntradaHornoProcessor::states_;

void EntradaHornoProcessor::reset_states()
{
    std::lock_guard<std::mutex> lk(mtx_);
    states_.clear();
    std::cout << "[EntradaHornoProcessor] All states reset" << std::endl;
}

// ============================================================================
// SalidaHornoProcessor - Fixed with correct counter handling
// ============================================================================

class SalidaHornoProcessor : public IMessageProcessor
{
    struct State {
        bool initialized = false;
        int shift = -1;

        uint16_t last_bancalinos0 = 0;
        uint32_t acc_bancalinos0 = 0;

        uint16_t last_bancalinos1 = 0;
        uint32_t acc_bancalinos1 = 0;

        uint16_t last_bancalinosComb1 = 0;
        uint32_t acc_bancalinosComb1 = 0;

        uint16_t last_bancalinosComb2 = 0;
        uint32_t acc_bancalinosComb2 = 0;

        uint16_t last_bancalinosTotal = 0;
        uint32_t acc_bancalinosTotal = 0;

        uint16_t last_cambioBarrera = 0;
        uint32_t acc_cambioBarrera = 0;

        uint16_t last_cambioBarreraTotal = 0;
        uint32_t acc_cambioBarreraTotal = 0;

        uint16_t last_cambioSentido = 0;
        uint32_t acc_cambioSentido = 0;

        uint16_t last_cambioSentidoTotal = 0;
        uint32_t acc_cambioSentidoTotal = 0;

        uint16_t last_cantidad = 0;
        uint32_t acc_cantidad = 0;

        uint16_t last_cantidad_total = 0;
        uint32_t acc_cantidad_total = 0;

        uint16_t last_paradas_1 = 0;
        uint32_t acc_paradas_1 = 0;

        uint16_t last_paradas_2 = 0;
        uint32_t acc_paradas_2 = 0;

        uint16_t last_timer1Hz = 0;
        uint32_t acc_timer1Hz = 0;

        uint32_t acc_tiempo_operacion_s = 0;
    };

    static std::mutex mtx_;
    static std::unordered_map<int, State> states_;

public:
    static void reset_states() {
        std::lock_guard<std::mutex> lock(mtx_);
        states_.clear();
    }

    std::vector<Publication> process(const json &msg,
                                     const std::string &isa95_prefix) override
    {
        auto sh = current_shift_localtime();
        int shiftNum = (sh == Shift::S1 ? 1 : (sh == Shift::S2 ? 2 : 3));

        int line = jsonu::get_opt<int>(msg, "lineID").value_or(0);
        int alarms = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int checksum = jsonu::get_opt<int>(msg, "checksum").value_or(0);
        int deviceType = jsonu::get_opt<int>(msg, "deviceType").value_or(0);

        int bancalinos0_raw = jsonu::get_opt<int>(msg, "bancalinos0").value_or(0);
        int bancalinos1_raw = jsonu::get_opt<int>(msg, "bancalinos1").value_or(0);
        int bancalinosComb1_raw = jsonu::get_opt<int>(msg, "bancalinosComb1").value_or(0);
        int bancalinosComb2_raw = jsonu::get_opt<int>(msg, "bancalinosComb2").value_or(0);
        int bancalinosTotal_raw = jsonu::get_opt<int>(msg, "bancalinosTotal").value_or(0);

        int cambioBarrera_raw = jsonu::get_opt<int>(msg, "cambioBarrera").value_or(0);
        int cambioBarreraTotal_raw = jsonu::get_opt<int>(msg, "cambioBarreraTotal").value_or(0);
        int cambioSentido_raw = jsonu::get_opt<int>(msg, "cambioSentido").value_or(0);
        int cambioSentidoTotal_raw = jsonu::get_opt<int>(msg, "cambioSentidoTotal").value_or(0);

        int cantidad_raw = jsonu::get_opt<int>(msg, "cantidad").value_or(0);
        int cantidad_total_raw = jsonu::get_opt<int>(msg, "cantidad_total").value_or(0);

        int paradas_1_raw = jsonu::get_opt<int>(msg, "paradas_1").value_or(0);
        int paradas_2_raw = jsonu::get_opt<int>(msg, "paradas_2").value_or(0);

        int timer1Hz_raw = jsonu::get_opt<int>(msg, "timer1Hz").value_or(0);

        uint16_t bancalinos0 = static_cast<uint16_t>(bancalinos0_raw);
        uint16_t bancalinos1 = static_cast<uint16_t>(bancalinos1_raw);
        uint16_t bancalinosComb1 = static_cast<uint16_t>(bancalinosComb1_raw);
        uint16_t bancalinosComb2 = static_cast<uint16_t>(bancalinosComb2_raw);
        uint16_t bancalinosTotal = static_cast<uint16_t>(bancalinosTotal_raw);
        uint16_t cambioBarrera = static_cast<uint16_t>(cambioBarrera_raw);
        uint16_t cambioBarreraTotal = static_cast<uint16_t>(cambioBarreraTotal_raw);
        uint16_t cambioSentido = static_cast<uint16_t>(cambioSentido_raw);
        uint16_t cambioSentidoTotal = static_cast<uint16_t>(cambioSentidoTotal_raw);
        uint16_t cantidad = static_cast<uint16_t>(cantidad_raw);
        uint16_t cantidad_total = static_cast<uint16_t>(cantidad_total_raw);
        uint16_t paradas_1 = static_cast<uint16_t>(paradas_1_raw);
        uint16_t paradas_2 = static_cast<uint16_t>(paradas_2_raw);
        uint16_t timer1Hz = static_cast<uint16_t>(timer1Hz_raw);

        uint32_t acc_bancalinos0_out = 0;
        uint32_t acc_bancalinos1_out = 0;
        uint32_t acc_bancalinosComb1_out = 0;
        uint32_t acc_bancalinosComb2_out = 0;
        uint32_t acc_bancalinosTotal_out = 0;
        uint32_t acc_cambioBarrera_out = 0;
        uint32_t acc_cambioBarreraTotal_out = 0;
        uint32_t acc_cambioSentido_out = 0;
        uint32_t acc_cambioSentidoTotal_out = 0;
        uint32_t acc_cantidad_out = 0;
        uint32_t acc_cantidad_total_out = 0;
        uint32_t acc_paradas_1_out = 0;
        uint32_t acc_paradas_2_out = 0;
        uint32_t acc_tiempo_operacion_s_out = 0;

        {
            std::lock_guard<std::mutex> lock(mtx_);
            State &st = states_[line];

            if (!st.initialized || st.shift != shiftNum) {
                st = State();
                st.initialized = true;
                st.shift = shiftNum;

                st.last_bancalinos0 = bancalinos0;
                st.last_bancalinos1 = bancalinos1;
                st.last_bancalinosComb1 = bancalinosComb1;
                st.last_bancalinosComb2 = bancalinosComb2;
                st.last_bancalinosTotal = bancalinosTotal;
                st.last_cambioBarrera = cambioBarrera;
                st.last_cambioBarreraTotal = cambioBarreraTotal;
                st.last_cambioSentido = cambioSentido;
                st.last_cambioSentidoTotal = cambioSentidoTotal;
                st.last_cantidad = cantidad;
                st.last_cantidad_total = cantidad_total;
                st.last_paradas_1 = paradas_1;
                st.last_paradas_2 = paradas_2;
                st.last_timer1Hz = timer1Hz;
            }
            else {
                // Use PLC-compatible counter validation for all counters
                st.acc_bancalinos0 += diff_counter(bancalinos0, st.last_bancalinos0);
                st.last_bancalinos0 = bancalinos0;

                st.acc_bancalinos1 += diff_counter(bancalinos1, st.last_bancalinos1);
                st.last_bancalinos1 = bancalinos1;

                st.acc_bancalinosComb1 += diff_counter(bancalinosComb1, st.last_bancalinosComb1);
                st.last_bancalinosComb1 = bancalinosComb1;

                st.acc_bancalinosComb2 += diff_counter(bancalinosComb2, st.last_bancalinosComb2);
                st.last_bancalinosComb2 = bancalinosComb2;

                st.acc_bancalinosTotal += diff_counter(bancalinosTotal, st.last_bancalinosTotal);
                st.last_bancalinosTotal = bancalinosTotal;

                st.acc_cambioBarrera += diff_counter(cambioBarrera, st.last_cambioBarrera);
                st.last_cambioBarrera = cambioBarrera;

                st.acc_cambioBarreraTotal += diff_counter(cambioBarreraTotal, st.last_cambioBarreraTotal);
                st.last_cambioBarreraTotal = cambioBarreraTotal;

                st.acc_cambioSentido += diff_counter(cambioSentido, st.last_cambioSentido);
                st.last_cambioSentido = cambioSentido;

                st.acc_cambioSentidoTotal += diff_counter(cambioSentidoTotal, st.last_cambioSentidoTotal);
                st.last_cambioSentidoTotal = cambioSentidoTotal;

                st.acc_cantidad += diff_counter(cantidad, st.last_cantidad);
                st.last_cantidad = cantidad;

                st.acc_cantidad_total += diff_counter(cantidad_total, st.last_cantidad_total);
                st.last_cantidad_total = cantidad_total;

                st.acc_paradas_1 += diff_counter(paradas_1, st.last_paradas_1);
                st.last_paradas_1 = paradas_1;

                st.acc_paradas_2 += diff_counter(paradas_2, st.last_paradas_2);
                st.last_paradas_2 = paradas_2;

                // timer1Hz is a timer (counts seconds)
                uint16_t delta_timer = diff_timer(timer1Hz, st.last_timer1Hz);
                st.acc_timer1Hz += delta_timer;
                st.acc_tiempo_operacion_s += delta_timer;
                st.last_timer1Hz = timer1Hz;
            }

            acc_bancalinos0_out = st.acc_bancalinos0;
            acc_bancalinos1_out = st.acc_bancalinos1;
            acc_bancalinosComb1_out = st.acc_bancalinosComb1;
            acc_bancalinosComb2_out = st.acc_bancalinosComb2;
            acc_bancalinosTotal_out = st.acc_bancalinosTotal;
            acc_cambioBarrera_out = st.acc_cambioBarrera;
            acc_cambioBarreraTotal_out = st.acc_cambioBarreraTotal;
            acc_cambioSentido_out = st.acc_cambioSentido;
            acc_cambioSentidoTotal_out = st.acc_cambioSentidoTotal;
            acc_cantidad_out = st.acc_cantidad;
            acc_cantidad_total_out = st.acc_cantidad_total;
            acc_paradas_1_out = st.acc_paradas_1;
            acc_paradas_2_out = st.acc_paradas_2;
            acc_tiempo_operacion_s_out = st.acc_tiempo_operacion_s;
        }

        json prod;
        prod["maquina_id"] = 7;
        prod["turno"] = shiftNum;
        prod["deviceType"] = deviceType;
        prod["lineID"] = line;
        prod["checksum"] = checksum;

        prod["bancalinos0_instantaneo"] = bancalinos0;
        prod["bancalinos0_turno"] = acc_bancalinos0_out;

        prod["bancalinos1_instantaneo"] = bancalinos1;
        prod["bancalinos1_turno"] = acc_bancalinos1_out;

        prod["bancalinosComb1_instantaneo"] = bancalinosComb1;
        prod["bancalinosComb1_turno"] = acc_bancalinosComb1_out;

        prod["bancalinosComb2_instantaneo"] = bancalinosComb2;
        prod["bancalinosComb2_turno"] = acc_bancalinosComb2_out;

        prod["bancalinosTotal_raw"] = bancalinosTotal_raw;
        prod["bancalinosTotal_turno"] = acc_bancalinosTotal_out;

        prod["cambioBarrera_instantaneo"] = cambioBarrera;
        prod["cambioBarrera_turno"] = acc_cambioBarrera_out;

        prod["cambioBarreraTotal_raw"] = cambioBarreraTotal_raw;
        prod["cambioBarreraTotal_turno"] = acc_cambioBarreraTotal_out;

        prod["cambioSentido_instantaneo"] = cambioSentido;
        prod["cambioSentido_turno"] = acc_cambioSentido_out;

        prod["cambioSentidoTotal_raw"] = cambioSentidoTotal_raw;
        prod["cambioSentidoTotal_turno"] = acc_cambioSentidoTotal_out;

        prod["cantidad_instantanea"] = cantidad;
        prod["cantidad_raw"] = cantidad_raw;
        prod["cantidad_produccion_turno"] = acc_cantidad_out;

        prod["cantidad_total_raw"] = cantidad_total_raw;
        prod["cantidad_total_turno"] = acc_cantidad_total_out;

        prod["paradas_1_instantaneo"] = paradas_1;
        prod["paradas_1_turno"] = acc_paradas_1_out;

        prod["paradas_2_instantaneo"] = paradas_2;
        prod["paradas_2_turno"] = acc_paradas_2_out;

        prod["timer1Hz_instantaneo"] = timer1Hz;
        prod["tiempo_operacion_turno_s"] = acc_tiempo_operacion_s_out;

        prod["timestamp_device"] = iso8601_utc_now();

        json qual;
        qual["alarms"] = alarms;
        qual["timestamp_device"] = iso8601_utc_now();

        auto t1 = isa95_prefix + std::to_string(line) + "/salida_horno/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/salida_horno/production";

        return { make_pub(t1, qual), make_pub(t2, prod) };
    }
};

std::mutex SalidaHornoProcessor::mtx_;
std::unordered_map<int, SalidaHornoProcessor::State>
    SalidaHornoProcessor::states_;


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