#include "MessageProcessor.hpp"
#include "JsonUtils.hpp"
#include "Shift.hpp"
#include "TimeUtils.hpp"
#include <memory>
#include <sstream>
#include <mutex>
#include <atomic>
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

        // Example: publish to a “production” topic
        auto t1 = isa95_prefix + "/production/line/quantity";
        json p1;
        p1["quantity"] = jsonu::get_opt<int>(msg, "cantidad").value_or(0);
        p1["ts"] = std::time(nullptr);

        // Example: publish to a “quality/alarms” topic
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
// PrensaHidraulica1Processor - Enhanced with Monotonic Accumulators
// ============================================================================

class PrensaHidraulica1Processor : public IMessageProcessor
{
    struct PH1State {
        bool initialized = false;
        int  shift       = -1;

        // cantidadProductos (15-bit counter, MSB = bank flag)
        uint16_t last_contador15 = 0;
        uint32_t acc_pisadas = 0;

        // tiempoProduccion_ds (16-bit, no MSB flag)
        uint16_t last_raw_prod_time = 0;
        double   acc_prod_time_s = 0.0;

        // paradas (15-bit counter)
        uint16_t last_paradas15 = 0;
        uint32_t acc_paradas = 0;

        // tiempoParadas_s (15-bit counter)
        uint16_t last_tiempo_paradas15 = 0;
        uint32_t acc_tiempo_paradas_s = 0;
    };

    static std::mutex mtx_;
    static std::unordered_map<int, PH1State> states_;

    // 15-bit counter: MSB is a bank flag
    static constexpr uint16_t COUNTER_MASK = 0x7FFF;
    static constexpr uint16_t COUNTER_MOD  = 0x8000;

    static inline uint16_t clean15(int x) {
        return static_cast<uint16_t>(x) & COUNTER_MASK;
    }

    static inline bool is_corrupted(int x) {
        return (x & 0x8000) != 0;
    }

    static uint16_t diff15(uint16_t curr, uint16_t prev) {
        if (curr >= prev) {
            return curr - prev;
        } else {
            return static_cast<uint16_t>(COUNTER_MOD + curr - prev);
        }
    }

    static uint16_t diff16(uint16_t curr, uint16_t prev) {
        if (curr >= prev) {
            return curr - prev;
        } else {
            return static_cast<uint16_t>(65536 - prev + curr);
        }
    }

public:
    /**
     * Reset all accumulated states across all lines
     * Useful for testing or manual reset operations
     */
    static void reset_states() {
        std::lock_guard<std::mutex> lock(mtx_);
        states_.clear();
    }

    std::vector<Publication> process(const json &msg,
                                     const std::string &isa95_prefix) override
    {
        auto sh = current_shift_localtime();
        int shiftNum = (sh == Shift::S1 ? 1 : (sh == Shift::S2 ? 2 : 3));

        // Read inputs
        int line          = jsonu::get_opt<int>(msg, "lineID").value_or(0);
        int alarms        = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int raw_count_i   = jsonu::get_opt<int>(msg, "cantidadProductos").value_or(0);
        int raw_time_i    = jsonu::get_opt<int>(msg, "tiempoProduccion_ds").value_or(0);
        int paradas_raw   = jsonu::get_opt<int>(msg, "paradas").value_or(0);
        int tiempo_paradas_raw = jsonu::get_opt<int>(msg, "tiempoParadas_s").value_or(0);

        // Detect corruption
        bool corr_contador = is_corrupted(raw_count_i);
        bool corr_paradas = is_corrupted(paradas_raw);
        bool corr_tiempo_paradas = is_corrupted(tiempo_paradas_raw);

        // Clean values (remove MSB)
        uint16_t contador_clean = clean15(raw_count_i);
        uint16_t time_clean = static_cast<uint16_t>(raw_time_i);  // 16-bit, no corruption
        uint16_t paradas_clean = clean15(paradas_raw);
        uint16_t tiempo_paradas_clean = clean15(tiempo_paradas_raw);

        // Output accumulators
        uint32_t acc_pisadas_out = 0;
        double   acc_prod_time_s_out = 0.0;
        uint32_t acc_paradas_out = 0;
        uint32_t acc_tiempo_paradas_s_out = 0;
        double   pisadas_min = 0.0;

        {
            std::lock_guard<std::mutex> lock(mtx_);
            PH1State &st = states_[line];

            if (!st.initialized || st.shift != shiftNum) {
                // New shift - reset all accumulators
                st = PH1State();
                st.initialized = true;
                st.shift = shiftNum;

                st.last_contador15 = contador_clean;
                st.last_raw_prod_time = time_clean;
                st.last_paradas15 = paradas_clean;
                st.last_tiempo_paradas15 = tiempo_paradas_clean;
            }
            else {
                // Accumulate pisadas (15-bit counter)
                st.acc_pisadas += diff15(contador_clean, st.last_contador15);
                st.last_contador15 = contador_clean;

                // Accumulate production time (16-bit, deciseconds -> seconds)
                uint16_t delta_time = diff16(time_clean, st.last_raw_prod_time);
                st.acc_prod_time_s += delta_time * 0.1;
                st.last_raw_prod_time = time_clean;

                // Accumulate paradas (15-bit counter)
                st.acc_paradas += diff15(paradas_clean, st.last_paradas15);
                st.last_paradas15 = paradas_clean;

                // Accumulate tiempo paradas (15-bit counter, already in seconds)
                st.acc_tiempo_paradas_s += diff15(tiempo_paradas_clean, st.last_tiempo_paradas15);
                st.last_tiempo_paradas15 = tiempo_paradas_clean;
            }

            // Copy out accumulated values
            acc_pisadas_out = st.acc_pisadas;
            acc_prod_time_s_out = st.acc_prod_time_s;
            acc_paradas_out = st.acc_paradas;
            acc_tiempo_paradas_s_out = st.acc_tiempo_paradas_s;

            // Calculate rate (pisadas per minute)
            if (acc_prod_time_s_out > 1.0) {
                pisadas_min = acc_pisadas_out / (acc_prod_time_s_out / 60.0);
            }
        }

        // Build output JSON

        //Calcular facto de pisadas
        int factor_pisadas;
        switch (line) {
            case 1:
                factor_pisadas = L1_PIEZAS_PISADA;
                break;
            case 2:
                factor_pisadas = L2_PIEZAS_PISADA;
                break;
            case 3:
                factor_pisadas = L3_PIEZAS_PISADA;
                break;
            case 4:
                factor_pisadas = L4_PIEZAS_PISADA;
                break;
            case 5:
                factor_pisadas = L5_PIEZAS_PISADA;
                break;
            default:
                factor_pisadas = 3; // Valor por defecto si la línea no es reconocida
                break;
        }

        json qual;
        qual["alarms"] = alarms;
        qual["timestamp_device"] = iso8601_utc_now();

        json prod;
        prod["maquina_id"] = 1;
        prod["turno"] = shiftNum;

        // Pisadas (primary counter)
        prod["cantidadProductos_raw"] = raw_count_i;
        prod["cantidadProductos_instantaneo"] = contador_clean;
        prod["bit15_corruption_cantidadProductos"] = corr_contador;
        
        prod["cantidadPisadas_turno"] = acc_pisadas_out;
        prod["cantidadPisadas_min"] = static_cast<uint32_t>(pisadas_min);
        prod["cantidadProductos_turno"] = acc_pisadas_out * factor_pisadas;

        // Production time
        prod["tiempoProduccion_ds_instantaneo"] = time_clean;
        prod["tiempoProduccion_turno_s"] = static_cast<uint32_t>(acc_prod_time_s_out);

        // Paradas (stops)
        prod["paradas_raw"] = paradas_raw;
        prod["paradas_instantaneo"] = paradas_clean;
        prod["paradas_turno"] = acc_paradas_out;
        prod["bit15_corruption_paradas"] = corr_paradas;

        // Tiempo paradas (stop time)
        prod["tiempoParadas_raw"] = tiempo_paradas_raw;
        prod["tiempoParadas_instantaneo"] = tiempo_paradas_clean;
        prod["tiempoParadas_turno_s"] = acc_tiempo_paradas_s_out;
        prod["bit15_corruption_tiempoParadas"] = corr_tiempo_paradas;

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
// PrensaHidraulica2Processor - Enhanced with Monotonic Accumulators
// ============================================================================

class PrensaHidraulica2Processor : public IMessageProcessor
{
    struct PH2State {
        bool initialized = false;
        int  shift       = -1;

        // cantidadProductos (15-bit counter, MSB = bank flag)
        uint16_t last_contador15 = 0;
        uint32_t acc_pisadas = 0;

        // tiempoProduccion_ds (16-bit, no MSB flag)
        uint16_t last_raw_prod_time = 0;
        double   acc_prod_time_s = 0.0;

        // paradas (15-bit counter)
        uint16_t last_paradas15 = 0;
        uint32_t acc_paradas = 0;

        // tiempoParadas_s (15-bit counter)
        uint16_t last_tiempo_paradas15 = 0;
        uint32_t acc_tiempo_paradas_s = 0;
    };

    static std::mutex mtx_;
    static std::unordered_map<int, PH2State> states_;

    // 15-bit counter: MSB is a bank flag
    static constexpr uint16_t COUNTER_MASK = 0x7FFF;
    static constexpr uint16_t COUNTER_MOD  = 0x8000;

    static inline uint16_t clean15(int x) {
        return static_cast<uint16_t>(x) & COUNTER_MASK;
    }

    static inline bool is_corrupted(int x) {
        return (x & 0x8000) != 0;
    }

    static uint16_t diff15(uint16_t curr, uint16_t prev) {
        if (curr >= prev) {
            return curr - prev;
        } else {
            return static_cast<uint16_t>(COUNTER_MOD + curr - prev);
        }
    }

    static uint16_t diff16(uint16_t curr, uint16_t prev) {
        if (curr >= prev) {
            return curr - prev;
        } else {
            return static_cast<uint16_t>(65536 - prev + curr);
        }
    }

public:
    /**
     * Reset all accumulated states across all lines
     * Useful for testing or manual reset operations
     */
    static void reset_states() {
        std::lock_guard<std::mutex> lock(mtx_);
        states_.clear();
    }

    std::vector<Publication> process(const json &msg,
                                     const std::string &isa95_prefix) override
    {
        auto sh = current_shift_localtime();
        int shiftNum = (sh == Shift::S1 ? 1 : (sh == Shift::S2 ? 2 : 3));

        // Read inputs
        int line          = jsonu::get_opt<int>(msg, "lineID").value_or(0);
        int alarms        = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int raw_count_i   = jsonu::get_opt<int>(msg, "cantidadProductos").value_or(0);
        int raw_time_i    = jsonu::get_opt<int>(msg, "tiempoProduccion_ds").value_or(0);
        int paradas_raw   = jsonu::get_opt<int>(msg, "paradas").value_or(0);
        int tiempo_paradas_raw = jsonu::get_opt<int>(msg, "tiempoParadas_s").value_or(0);

        // Detect corruption
        bool corr_contador = is_corrupted(raw_count_i);
        bool corr_paradas = is_corrupted(paradas_raw);
        bool corr_tiempo_paradas = is_corrupted(tiempo_paradas_raw);

        // Clean values (remove MSB)
        uint16_t contador_clean = clean15(raw_count_i);
        uint16_t time_clean = static_cast<uint16_t>(raw_time_i);  // 16-bit, no corruption
        uint16_t paradas_clean = clean15(paradas_raw);
        uint16_t tiempo_paradas_clean = clean15(tiempo_paradas_raw);

        // Output accumulators
        uint32_t acc_pisadas_out = 0;
        double   acc_prod_time_s_out = 0.0;
        uint32_t acc_paradas_out = 0;
        uint32_t acc_tiempo_paradas_s_out = 0;
        double   pisadas_min = 0.0;

        {
            std::lock_guard<std::mutex> lock(mtx_);
            PH2State &st = states_[line];

            if (!st.initialized || st.shift != shiftNum) {
                // New shift - reset all accumulators
                st = PH2State();
                st.initialized = true;
                st.shift = shiftNum;

                st.last_contador15 = contador_clean;
                st.last_raw_prod_time = time_clean;
                st.last_paradas15 = paradas_clean;
                st.last_tiempo_paradas15 = tiempo_paradas_clean;
            }
            else {
                // Accumulate pisadas (15-bit counter)
                st.acc_pisadas += diff15(contador_clean, st.last_contador15);
                st.last_contador15 = contador_clean;

                // Accumulate production time (16-bit, deciseconds -> seconds)
                uint16_t delta_time = diff16(time_clean, st.last_raw_prod_time);
                st.acc_prod_time_s += delta_time * 0.1;
                st.last_raw_prod_time = time_clean;

                // Accumulate paradas (15-bit counter)
                st.acc_paradas += diff15(paradas_clean, st.last_paradas15);
                st.last_paradas15 = paradas_clean;

                // Accumulate tiempo paradas (15-bit counter, already in seconds)
                st.acc_tiempo_paradas_s += diff15(tiempo_paradas_clean, st.last_tiempo_paradas15);
                st.last_tiempo_paradas15 = tiempo_paradas_clean;
            }

            // Copy out accumulated values
            acc_pisadas_out = st.acc_pisadas;
            acc_prod_time_s_out = st.acc_prod_time_s;
            acc_paradas_out = st.acc_paradas;
            acc_tiempo_paradas_s_out = st.acc_tiempo_paradas_s;

            // Calculate rate (pisadas per minute)
            if (acc_prod_time_s_out > 1.0) {
                pisadas_min = acc_pisadas_out / (acc_prod_time_s_out / 60.0);
            }
        }

        // Build output JSON
        json qual;
        qual["alarms"] = alarms;
        qual["timestamp_device"] = iso8601_utc_now();

        json prod;
        prod["maquina_id"] = 2;  // Machine ID 2 for Prensa Hidraulica 2
        prod["turno"] = shiftNum;

        // Pisadas (primary counter)
        prod["cantidadProductos_raw"] = raw_count_i;
        prod["cantidadProductos_instantaneo"] = contador_clean;
        prod["bit15_corruption_cantidadProductos"] = corr_contador;
        
        prod["cantidadPisadas_turno"] = acc_pisadas_out;
        prod["cantidadPisadas_min"] = static_cast<uint32_t>(pisadas_min);
        prod["cantidadProductos_turno"] = acc_pisadas_out * PIEZAS_PISADA;  // 6 pieces per pisada

        // Production time
        prod["tiempoProduccion_ds_instantaneo"] = time_clean;
        prod["tiempoProduccion_turno_s"] = static_cast<uint32_t>(acc_prod_time_s_out);

        // Paradas (stops)
        prod["paradas_raw"] = paradas_raw;
        prod["paradas_instantaneo"] = paradas_clean;
        prod["paradas_turno"] = acc_paradas_out;
        prod["bit15_corruption_paradas"] = corr_paradas;

        // Tiempo paradas (stop time)
        prod["tiempoParadas_raw"] = tiempo_paradas_raw;
        prod["tiempoParadas_instantaneo"] = tiempo_paradas_clean;
        prod["tiempoParadas_turno_s"] = acc_tiempo_paradas_s_out;
        prod["bit15_corruption_tiempoParadas"] = corr_tiempo_paradas;

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

class EntradaSecadorProcessor : public IMessageProcessor
{
    struct State {
        bool initialized = false;
        int  shift = -1;

        uint16_t last_arranques = 0;
        uint32_t acc_arranques  = 0;

        uint16_t last_t_operacion = 0;
        uint32_t acc_t_operacion_s = 0;  // tiempoOperacion viene en segundos
    };

    static std::mutex mtx_;
    static std::unordered_map<int, State> states_;

    // ---- Same pattern used for Esmalte/SalidaHorno ----
    static inline uint16_t clean15(int x) {
        return static_cast<uint16_t>(x) & 0x7FFF;
    }
    static uint16_t diff16(uint16_t curr, uint16_t prev) {
        return (curr >= prev)
            ? (curr - prev)
            : static_cast<uint16_t>(curr + (65536 - prev));
    }

    // Reject 0 and absurd deltas
    static uint16_t safe_delta_u16(uint16_t prev, uint16_t curr, uint16_t max_reasonable)
    {
        const uint16_t d = diff16(curr, prev);
        if (d == 0) return 0;
        if (d > max_reasonable) return 0;   // ignore rollover/garbage
        return d;
    }

public:
static void reset_states();
    std::vector<Publication> process(const json &msg,
                                     const std::string &isa95_prefix) override
    {
        // ---- Determine shift ----
        auto sh = current_shift_localtime();
        int shiftNum = (sh == Shift::S1 ? 1 : (sh == Shift::S2 ? 2 : 3));

        int lineID        = msg.value("lineID", 0);
        int alarms        = msg.value("alarms", 0);

        int arr_in        = msg.value("arranques", 0);
        int t_oper_s_in   = msg.value("tiempoOperacion_s", 0);

        uint32_t out_arranques = 0;
        uint32_t out_t_oper    = 0;

        // mask MSB like other processors
        uint16_t raw_arr    = clean15(arr_in);
        uint16_t raw_t_oper = clean15(t_oper_s_in);

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
                // use reasonable deltas: assume no more than 100 arranques per 30 s
                st.acc_arranques += safe_delta_u16(st.last_arranques,
                                                   raw_arr,
                                                   100);
                st.last_arranques = raw_arr;

                // tiempo de operación en segundos: delta razonable 0..30
                st.acc_t_operacion_s += safe_delta_u16(st.last_t_operacion,
                                                       raw_t_oper,
                                                       30);
                st.last_t_operacion = raw_t_oper;
            }

            out_arranques = st.acc_arranques;
            out_t_oper    = st.acc_t_operacion_s;
        }

        // ---- Build outputs ----
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

class SalidaSecadorProcessor : public IMessageProcessor
{
    struct State {
        bool initialized = false;
        int  shift       = -1;

        // cantidadProductos (15-bit counter, MSB is flag)
        uint16_t last_prod_q15   = 0;
        uint32_t acc_prod_q      = 0;

        // paradas (15-bit counter, MSB is flag)
        uint16_t last_stop_q15   = 0;
        uint32_t acc_stop_q      = 0;

        // tiempoProduccion_ds (16-bit, deciseconds)
        uint16_t last_raw_prod_t = 0;
        double   acc_prod_t_s    = 0.0;

        // tiempoParadas_s (15-bit, MSB is flag)
        uint16_t last_stop_t15   = 0;
        uint32_t acc_stop_t_s    = 0;
    };

    static std::mutex mtx_;
    static std::unordered_map<int, State> states_;

    static constexpr uint16_t MASK_15 = 0x7FFF;  // bits 0..14
    static constexpr uint16_t MOD_15  = 0x8000;  // 2^15

public:
static void reset_states();
    std::vector<Publication> process(const json &msg,
                                     const std::string &isa95_prefix) override
    {
        // ---- Current shift ----
        auto sh       = current_shift_localtime();
        int  shiftNum = (sh == Shift::S1 ? 1 : (sh == Shift::S2 ? 2 : 3));

        // ---- Read fields ----
        int alarms = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int prod_q = jsonu::get_opt<int>(msg, "cantidadProductos").value_or(0);
        int prod_t = jsonu::get_opt<int>(msg, "tiempoProduccion_ds").value_or(0);
        int line   = jsonu::get_opt<int>(msg, "lineID").value_or(0);
        int stop_q = jsonu::get_opt<int>(msg, "paradas").value_or(0);
        int stop_t = jsonu::get_opt<int>(msg, "tiempoParadas_s").value_or(0);

        // Outputs
        uint32_t prod_q_shift   = 0;
        double   prod_t_shift_s = 0.0;
        uint32_t stop_q_shift   = 0;
        uint32_t stop_t_shift_s = 0;

        {
            std::lock_guard<std::mutex> lock(mtx_);
            State &st = states_[line];

            // ---- Apply MSB removal for 15-bit counters ----
            uint16_t prod_q15 = static_cast<uint16_t>(prod_q) & MASK_15;
            uint16_t stop_q15 = static_cast<uint16_t>(stop_q) & MASK_15;
            uint16_t prod_t16 = static_cast<uint16_t>(prod_t);           // 16-bit normal
            uint16_t stop_t15 = static_cast<uint16_t>(stop_t) & MASK_15;

            // ---- First sample or shift change ----
            if (!st.initialized || st.shift != shiftNum) {
                st.initialized = true;
                st.shift       = shiftNum;

                st.last_prod_q15   = prod_q15;
                st.acc_prod_q      = 0;

                st.last_stop_q15   = stop_q15;
                st.acc_stop_q      = 0;

                st.last_raw_prod_t = prod_t16;
                st.acc_prod_t_s    = 0.0;

                st.last_stop_t15   = stop_t15;
                st.acc_stop_t_s    = 0;
            }
            else {
                // ---- cantidadProductos (15-bit modulo) ----
                {
                    uint16_t prev = st.last_prod_q15;
                    uint16_t curr = prod_q15;
                    uint16_t diff =
                        (curr >= prev)
                          ? static_cast<uint16_t>(curr - prev)
                          : static_cast<uint16_t>(MOD_15 + curr - prev);

                    st.acc_prod_q   += diff;
                    st.last_prod_q15 = curr;
                }

                // ---- paradas (15-bit modulo) ----
                {
                    uint16_t prev = st.last_stop_q15;
                    uint16_t curr = stop_q15;
                    uint16_t diff =
                        (curr >= prev)
                          ? static_cast<uint16_t>(curr - prev)
                          : static_cast<uint16_t>(MOD_15 + curr - prev);

                    st.acc_stop_q   += diff;
                    st.last_stop_q15 = curr;
                }

                // ---- tiempoProduccion_ds (16-bit normal modulo, ds→s) ----
                {
                    uint16_t prev = st.last_raw_prod_t;
                    uint16_t curr = prod_t16;
                    uint16_t diff =
                        (curr >= prev)
                          ? static_cast<uint16_t>(curr - prev)
                          : static_cast<uint16_t>(curr + (uint16_t)(65536 - prev));

                    st.acc_prod_t_s += diff * 0.1; // ds → s
                    st.last_raw_prod_t = curr;
                }

                // ---- tiempoParadas_s (15-bit modulo) ----
                {
                    uint16_t prev = st.last_stop_t15;
                    uint16_t curr = stop_t15;
                    uint16_t diff =
                        (curr >= prev)
                          ? static_cast<uint16_t>(curr - prev)
                          : static_cast<uint16_t>(MOD_15 + curr - prev);

                    st.acc_stop_t_s += diff;
                    st.last_stop_t15 = curr;
                }
            }

            // ---- Final accumulated values ----
            prod_q_shift   = st.acc_prod_q;
            prod_t_shift_s = st.acc_prod_t_s;
            stop_q_shift   = st.acc_stop_q;
            stop_t_shift_s = st.acc_stop_t_s;
        }

        // ---- Build MQTT payloads ----
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

// ---- STATIC DEFINITIONS ----
std::mutex SalidaSecadorProcessor::mtx_;
std::unordered_map<int, SalidaSecadorProcessor::State>
    SalidaSecadorProcessor::states_;

void SalidaSecadorProcessor::reset_states()
{
    std::lock_guard<std::mutex> lk(mtx_);
    states_.clear();
}


class EsmalteProcessor : public IMessageProcessor
{
    struct State {
        bool initialized = false;
        int shift = -1;

        // cantidadProductos (15-bit)
        uint16_t last_raw_prod_q = 0;
        uint32_t acc_prod_q = 0;

        // paradas (15-bit)
        uint16_t last_raw_stop_q = 0;
        uint32_t acc_stop_q = 0;

        // tiempoProduccion_ds (16 bits reales, sin MSB flag)
        uint16_t last_raw_prod_t = 0;
        double   acc_prod_t_s = 0.0;

        // tiempoParadas_s (15-bit)
        uint16_t last_raw_stop_t = 0;
        uint32_t acc_stop_t_s = 0;
    };

    static std::mutex mtx_;
    static std::unordered_map<int, State> states_;

    // --- Helpers ---
    static constexpr uint16_t MASK_15 = 0x7FFF;
    static constexpr uint16_t MOD_15  = 0x8000;

    static uint16_t mask15(int x) {
        return static_cast<uint16_t>(x) & MASK_15;
    }

    static uint16_t diff16(uint16_t curr, uint16_t prev) {
        return (curr >= prev)
            ? static_cast<uint16_t>(curr - prev)
            : static_cast<uint16_t>(curr + (65536 - prev));
    }

public:
static void reset_states();
    std::vector<Publication> process(const json &msg,
                                     const std::string &isa95_prefix) override
    {
        // ---- Determine shift ----
        auto sh = current_shift_localtime();
        int shiftNum = (sh == Shift::S1 ? 1 : sh == Shift::S2 ? 2 : 3);

        // ---- Extract fields ----
        int alarms   = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int prod_q   = jsonu::get_opt<int>(msg, "cantidadProductos").value_or(0);
        int prod_t   = jsonu::get_opt<int>(msg, "tiempoProduccion_ds").value_or(0);   // 16-bit real
        int line     = jsonu::get_opt<int>(msg, "lineID").value_or(0);
        int stop_q   = jsonu::get_opt<int>(msg, "paradas").value_or(0);               // 15-bit
        int stop_t   = jsonu::get_opt<int>(msg, "tiempoParadas_s").value_or(0);       // 15-bit

        uint32_t prod_q_shift = 0;
        uint32_t stop_q_shift = 0;
        double   prod_t_shift_s = 0.0;
        uint32_t stop_t_shift_s = 0;

        {
            std::lock_guard<std::mutex> lock(mtx_);
            State &st = states_[line];

            // Valores crudos sin máscara
            uint16_t raw_prod_q = static_cast<uint16_t>(prod_q);
            uint16_t raw_stop_q = static_cast<uint16_t>(stop_q);
            uint16_t raw_prod_t = static_cast<uint16_t>(prod_t);
            uint16_t raw_stop_t = static_cast<uint16_t>(stop_t);

            // Reset por primer mensaje o cambio de turno
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
                // ---- PRODUCCIÓN ----
                st.acc_prod_q += safe_delta_u16(st.last_raw_prod_q, raw_prod_q);
                st.last_raw_prod_q = raw_prod_q;

                // ---- PARADAS ----
                st.acc_stop_q += safe_delta_u16(st.last_raw_stop_q, raw_stop_q);
                st.last_raw_stop_q = raw_stop_q;

                // ---- tiempoProduccion_ds -> 0.1 s ----
                st.acc_prod_t_s += safe_delta_u16(st.last_raw_prod_t, raw_prod_t) * 0.1;
                st.last_raw_prod_t = raw_prod_t;

                // ---- tiempoParadas_s ----
                st.acc_stop_t_s += safe_delta_u16(st.last_raw_stop_t, raw_stop_t);
                st.last_raw_stop_t = raw_stop_t;
            }

            prod_q_shift      = st.acc_prod_q;
            stop_q_shift      = st.acc_stop_q;
            prod_t_shift_s    = st.acc_prod_t_s;
            stop_t_shift_s    = st.acc_stop_t_s;
        }


        // ---- Create outputs ----
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

// ---- STATIC DEFINITIONS ----
std::mutex EsmalteProcessor::mtx_;
std::unordered_map<int, EsmalteProcessor::State>
    EsmalteProcessor::states_;

void EsmalteProcessor::reset_states()
{
    std::lock_guard<std::mutex> lk(mtx_);
    states_.clear();
}


class EntradaHornoProcessor : public IMessageProcessor
{
    struct State {
        bool initialized = false;
        int  shift = -1;

        uint16_t last_raw_prod_q = 0;
        uint32_t acc_prod_q = 0;

        uint16_t last_raw_stop_q = 0;
        uint32_t acc_stop_q = 0;

        uint16_t last_raw_falla_q = 0;
        uint32_t acc_falla_q = 0;

        uint16_t last_raw_prod_t = 0;
        double   acc_prod_t_s = 0.0;

        uint16_t last_raw_stop_t = 0;
        uint32_t acc_stop_t_s = 0;

        uint16_t last_raw_falla_t = 0;
        uint32_t acc_falla_t_s = 0;
    };

    static std::mutex mtx_;
    static std::unordered_map<int, State> states_;

    static inline uint16_t clean15(int x) {
        return static_cast<uint16_t>(x) & 0x7FFF;
    }

    static uint16_t diff16(uint16_t curr, uint16_t prev) {
        return (curr >= prev)
             ? (curr - prev)
             : static_cast<uint16_t>(curr + (65536 - prev));
    }

    static uint16_t safe_delta_u16(uint16_t prev, uint16_t curr, uint16_t max_reasonable)
    {
        const uint16_t d = diff16(curr, prev);
        if (d == 0) return 0;
        if (d > max_reasonable) return 0;
        return d;
    }

public:
static void reset_states();
    std::vector<Publication> process(const json &msg,
                                     const std::string &isa95_prefix) override
    {
        auto sh = current_shift_localtime();
        int shiftNum = (sh == Shift::S1 ? 1 : sh == Shift::S2 ? 2 : 3);

        int alarms   = msg.value("alarms", 0);
        int line     = msg.value("lineID", 0);

        int prod_q   = msg.value("cantidad", 0);
        int prod_t   = msg.value("tiempoProd_ds", 0);

        int stop_q   = msg.value("paradas", 0);
        int stop_t   = msg.value("tiempoParadas_s", 0);

        int falla_q  = msg.value("fallaHorno", 0);
        int falla_t  = msg.value("tiempoFalla_s", 0);

        uint32_t out_prod_q = 0;
        uint32_t out_stop_q = 0;
        uint32_t out_falla_q = 0;

        double   out_prod_t_s = 0;
        uint32_t out_stop_t_s = 0;
        uint32_t out_falla_t_s = 0;

        uint16_t raw_prod_q  = clean15(prod_q);
        uint16_t raw_stop_q  = clean15(stop_q);
        uint16_t raw_falla_q = clean15(falla_q);

        uint16_t raw_prod_t  = clean15(prod_t);
        uint16_t raw_stop_t  = clean15(stop_t);
        uint16_t raw_falla_t = clean15(falla_t);

        {
            std::lock_guard<std::mutex> lock(mtx_);
            State &st = states_[line];

            if (!st.initialized || st.shift != shiftNum) {
                st = State();
                st.initialized = true;
                st.shift = shiftNum;

                st.last_raw_prod_q  = raw_prod_q;
                st.last_raw_stop_q  = raw_stop_q;
                st.last_raw_falla_q = raw_falla_q;

                st.last_raw_prod_t  = raw_prod_t;
                st.last_raw_stop_t  = raw_stop_t;
                st.last_raw_falla_t = raw_falla_t;
            }
            else {
                st.acc_prod_q += safe_delta_u16(st.last_raw_prod_q,
                                                raw_prod_q,
                                                200);   // productos/30s
                st.last_raw_prod_q = raw_prod_q;

                st.acc_stop_q += safe_delta_u16(st.last_raw_stop_q,
                                                raw_stop_q,
                                                50);
                st.last_raw_stop_q = raw_stop_q;

                st.acc_falla_q += safe_delta_u16(st.last_raw_falla_q,
                                                 raw_falla_q,
                                                 20);
                st.last_raw_falla_q = raw_falla_q;

                st.acc_prod_t_s += safe_delta_u16(st.last_raw_prod_t,
                                                  raw_prod_t,
                                                  250) * 0.1;
                st.last_raw_prod_t = raw_prod_t;

                st.acc_stop_t_s += safe_delta_u16(st.last_raw_stop_t,
                                                  raw_stop_t,
                                                  30);
                st.last_raw_stop_t = raw_stop_t;

                st.acc_falla_t_s += safe_delta_u16(st.last_raw_falla_t,
                                                   raw_falla_t,
                                                   30);
                st.last_raw_falla_t = raw_falla_t;
            }

            out_prod_q  = st.acc_prod_q;
            out_stop_q  = st.acc_stop_q;
            out_falla_q = st.acc_falla_q;

            out_prod_t_s  = st.acc_prod_t_s;
            out_stop_t_s  = st.acc_stop_t_s;
            out_falla_t_s = st.acc_falla_t_s;
        }

        json j_alarm;
        j_alarm["alarms"] = alarms;
        j_alarm["ts"]     = iso8601_utc_now();

        json prod;
        prod["maquina_id"] = 6;
        prod["turno"]      = shiftNum;

        prod["cantidad_produccion"] = out_prod_q;
        prod["cantidad_paradas"]    = out_stop_q;
        prod["cantidad_fallas"]     = out_falla_q;

        prod["tiempo_produccion"] = (uint32_t)out_prod_t_s;
        prod["tiempo_paradas"]    = out_stop_t_s;
        prod["tiempo_fallas"]     = out_falla_t_s;

        prod["timestamp_device"] = iso8601_utc_now();

        auto t1 = isa95_prefix + std::to_string(line) + "/entrada_horno/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/entrada_horno/production";
        return { make_pub(t1, j_alarm), make_pub(t2, prod) };
    }
};

std::mutex EntradaHornoProcessor::mtx_;
std::unordered_map<int, EntradaHornoProcessor::State>
    EntradaHornoProcessor::states_;

void EntradaHornoProcessor::reset_states()
{
    std::lock_guard<std::mutex> lk(mtx_);
    states_.clear();
}

// ============================================================================
// SalidaHornoProcessor - Complete Implementation with Monotonic Accumulators
// ============================================================================

class SalidaHornoProcessor : public IMessageProcessor
{
    struct State {
        bool initialized = false;
        int shift = -1;

        // All 15-bit counters (MSB is bank flag)
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

        // timer1Hz is 16-bit counter (no MSB flag)
        uint16_t last_timer1Hz = 0;
        uint32_t acc_timer1Hz = 0;

        // tiempo_operacion in seconds
        uint32_t acc_tiempo_operacion_s = 0;
    };

    static std::mutex mtx_;
    static std::unordered_map<int, State> states_;

    // MSB mask for 15-bit counters
    static constexpr uint16_t MASK_15 = 0x7FFF;
    static constexpr uint16_t MOD_15 = 0x8000;

    static inline uint16_t clean15(int x) {
        return static_cast<uint16_t>(x) & MASK_15;
    }

    static inline bool is_corrupted(int x) {
        return (x & 0x8000) != 0;
    }

    static uint16_t diff15(uint16_t curr, uint16_t prev) {
        // Calculate delta for 15-bit counter with rollover
        if (curr >= prev) {
            return curr - prev;
        } else {
            return static_cast<uint16_t>(MOD_15 + curr - prev);
        }
    }

    static uint16_t diff16(uint16_t curr, uint16_t prev) {
        // Calculate delta for 16-bit counter with rollover
        if (curr >= prev) {
            return curr - prev;
        } else {
            return static_cast<uint16_t>(65536 + curr - prev);
        }
    }

public:
    /**
     * Reset all accumulated states across all lines
     * Useful for testing or manual reset operations
     */
    static void reset_states() {
        std::lock_guard<std::mutex> lock(mtx_);
        states_.clear();
    }

    std::vector<Publication> process(const json &msg,
                                     const std::string &isa95_prefix) override
    {
        auto sh = current_shift_localtime();
        int shiftNum = (sh == Shift::S1 ? 1 : (sh == Shift::S2 ? 2 : 3));

        // Read all raw values from PLC
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

        // Detect corruption for all 15-bit counters
        bool corr_bancalinos0 = is_corrupted(bancalinos0_raw);
        bool corr_bancalinos1 = is_corrupted(bancalinos1_raw);
        bool corr_bancalinosComb1 = is_corrupted(bancalinosComb1_raw);
        bool corr_bancalinosComb2 = is_corrupted(bancalinosComb2_raw);
        bool corr_bancalinosTotal = is_corrupted(bancalinosTotal_raw);
        bool corr_cambioBarrera = is_corrupted(cambioBarrera_raw);
        bool corr_cambioBarreraTotal = is_corrupted(cambioBarreraTotal_raw);
        bool corr_cambioSentido = is_corrupted(cambioSentido_raw);
        bool corr_cambioSentidoTotal = is_corrupted(cambioSentidoTotal_raw);
        bool corr_cantidad = is_corrupted(cantidad_raw);
        bool corr_cantidad_total = is_corrupted(cantidad_total_raw);
        bool corr_paradas_1 = is_corrupted(paradas_1_raw);
        bool corr_paradas_2 = is_corrupted(paradas_2_raw);

        // Clean all 15-bit counters (remove MSB)
        uint16_t bancalinos0_clean = clean15(bancalinos0_raw);
        uint16_t bancalinos1_clean = clean15(bancalinos1_raw);
        uint16_t bancalinosComb1_clean = clean15(bancalinosComb1_raw);
        uint16_t bancalinosComb2_clean = clean15(bancalinosComb2_raw);
        uint16_t bancalinosTotal_clean = clean15(bancalinosTotal_raw);
        uint16_t cambioBarrera_clean = clean15(cambioBarrera_raw);
        uint16_t cambioBarreraTotal_clean = clean15(cambioBarreraTotal_raw);
        uint16_t cambioSentido_clean = clean15(cambioSentido_raw);
        uint16_t cambioSentidoTotal_clean = clean15(cambioSentidoTotal_raw);
        uint16_t cantidad_clean = clean15(cantidad_raw);
        uint16_t cantidad_total_clean = clean15(cantidad_total_raw);
        uint16_t paradas_1_clean = clean15(paradas_1_raw);
        uint16_t paradas_2_clean = clean15(paradas_2_raw);

        // timer1Hz is 16-bit (no corruption)
        uint16_t timer1Hz_clean = static_cast<uint16_t>(timer1Hz_raw);

        // Output accumulators
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
        uint32_t acc_timer1Hz_out = 0;
        uint32_t acc_tiempo_operacion_s_out = 0;

        {
            std::lock_guard<std::mutex> lock(mtx_);
            State &st = states_[line];

            if (!st.initialized || st.shift != shiftNum) {
                // New shift - reset all accumulators
                st = State();
                st.initialized = true;
                st.shift = shiftNum;

                // Initialize last values
                st.last_bancalinos0 = bancalinos0_clean;
                st.last_bancalinos1 = bancalinos1_clean;
                st.last_bancalinosComb1 = bancalinosComb1_clean;
                st.last_bancalinosComb2 = bancalinosComb2_clean;
                st.last_bancalinosTotal = bancalinosTotal_clean;
                st.last_cambioBarrera = cambioBarrera_clean;
                st.last_cambioBarreraTotal = cambioBarreraTotal_clean;
                st.last_cambioSentido = cambioSentido_clean;
                st.last_cambioSentidoTotal = cambioSentidoTotal_clean;
                st.last_cantidad = cantidad_clean;
                st.last_cantidad_total = cantidad_total_clean;
                st.last_paradas_1 = paradas_1_clean;
                st.last_paradas_2 = paradas_2_clean;
                st.last_timer1Hz = timer1Hz_clean;
            }
            else {
                // Accumulate deltas for all 15-bit counters
                st.acc_bancalinos0 += diff15(bancalinos0_clean, st.last_bancalinos0);
                st.last_bancalinos0 = bancalinos0_clean;

                st.acc_bancalinos1 += diff15(bancalinos1_clean, st.last_bancalinos1);
                st.last_bancalinos1 = bancalinos1_clean;

                st.acc_bancalinosComb1 += diff15(bancalinosComb1_clean, st.last_bancalinosComb1);
                st.last_bancalinosComb1 = bancalinosComb1_clean;

                st.acc_bancalinosComb2 += diff15(bancalinosComb2_clean, st.last_bancalinosComb2);
                st.last_bancalinosComb2 = bancalinosComb2_clean;

                st.acc_bancalinosTotal += diff15(bancalinosTotal_clean, st.last_bancalinosTotal);
                st.last_bancalinosTotal = bancalinosTotal_clean;

                st.acc_cambioBarrera += diff15(cambioBarrera_clean, st.last_cambioBarrera);
                st.last_cambioBarrera = cambioBarrera_clean;

                st.acc_cambioBarreraTotal += diff15(cambioBarreraTotal_clean, st.last_cambioBarreraTotal);
                st.last_cambioBarreraTotal = cambioBarreraTotal_clean;

                st.acc_cambioSentido += diff15(cambioSentido_clean, st.last_cambioSentido);
                st.last_cambioSentido = cambioSentido_clean;

                st.acc_cambioSentidoTotal += diff15(cambioSentidoTotal_clean, st.last_cambioSentidoTotal);
                st.last_cambioSentidoTotal = cambioSentidoTotal_clean;

                st.acc_cantidad += diff15(cantidad_clean, st.last_cantidad);
                st.last_cantidad = cantidad_clean;

                st.acc_cantidad_total += diff15(cantidad_total_clean, st.last_cantidad_total);
                st.last_cantidad_total = cantidad_total_clean;

                st.acc_paradas_1 += diff15(paradas_1_clean, st.last_paradas_1);
                st.last_paradas_1 = paradas_1_clean;

                st.acc_paradas_2 += diff15(paradas_2_clean, st.last_paradas_2);
                st.last_paradas_2 = paradas_2_clean;

                // timer1Hz is 16-bit counter
                uint16_t delta_timer = diff16(timer1Hz_clean, st.last_timer1Hz);
                st.acc_timer1Hz += delta_timer;
                st.acc_tiempo_operacion_s += delta_timer; // timer1Hz counts seconds
                st.last_timer1Hz = timer1Hz_clean;
            }

            // Copy out accumulated values
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
            acc_timer1Hz_out = st.acc_timer1Hz;
            acc_tiempo_operacion_s_out = st.acc_tiempo_operacion_s;
        }

        // Build output JSON with all fields
        json prod;
        prod["maquina_id"] = 7;
        prod["turno"] = shiftNum;
        prod["deviceType"] = deviceType;
        prod["lineID"] = line;
        prod["checksum"] = checksum;

        // Bancalinos fields
        prod["bancalinos0_instantaneo"] = bancalinos0_clean;
        prod["bancalinos0_turno"] = acc_bancalinos0_out;

        prod["bancalinos1_instantaneo"] = bancalinos1_clean;
        prod["bancalinos1_turno"] = acc_bancalinos1_out;

        prod["bancalinosComb1_instantaneo"] = bancalinosComb1_clean;
        prod["bancalinosComb1_turno"] = acc_bancalinosComb1_out;

        prod["bancalinosComb2_instantaneo"] = bancalinosComb2_clean;
        prod["bancalinosComb2_turno"] = acc_bancalinosComb2_out;

        prod["bancalinosTotal_raw"] = bancalinosTotal_raw;
        prod["bancalinosTotal_turno"] = acc_bancalinosTotal_out;
        prod["bit15_corruption_bancalinosTotal"] = corr_bancalinosTotal;

        // CambioBarrera fields
        prod["cambioBarrera_instantaneo"] = cambioBarrera_clean;
        prod["cambioBarrera_turno"] = acc_cambioBarrera_out;

        prod["cambioBarreraTotal_raw"] = cambioBarreraTotal_raw;
        prod["cambioBarreraTotal_turno"] = acc_cambioBarreraTotal_out;
        prod["bit15_corruption_cambioBarreraTotal"] = corr_cambioBarreraTotal;

        // CambioSentido fields
        prod["cambioSentido_instantaneo"] = cambioSentido_clean;
        prod["cambioSentido_turno"] = acc_cambioSentido_out;

        prod["cambioSentidoTotal_raw"] = cambioSentidoTotal_raw;
        prod["cambioSentidoTotal_turno"] = acc_cambioSentidoTotal_out;
        prod["bit15_corruption_cambioSentidoTotal"] = corr_cambioSentidoTotal;

        // Cantidad fields
        prod["cantidad_instantanea"] = cantidad_clean;
        prod["cantidad_raw"] = cantidad_raw;
        prod["cantidad_produccion_turno"] = acc_cantidad_out;
        prod["bit15_corruption_cantidad"] = corr_cantidad;

        prod["cantidad_total_raw"] = cantidad_total_raw;
        prod["cantidad_total_turno"] = acc_cantidad_total_out;
        prod["bit15_corruption_cantidad_total"] = corr_cantidad_total;

        // Paradas fields
        prod["paradas_1_instantaneo"] = paradas_1_clean;
        prod["paradas_1_turno"] = acc_paradas_1_out;

        prod["paradas_2_instantaneo"] = paradas_2_clean;
        prod["paradas_2_turno"] = acc_paradas_2_out;

        // Timer fields
        prod["timer1Hz_instantaneo"] = timer1Hz_clean;
        prod["tiempo_operacion_turno_s"] = acc_tiempo_operacion_s_out;

        prod["timestamp_device"] = iso8601_utc_now();

        // Alarms
        json qual;
        qual["alarms"] = alarms;
        qual["timestamp_device"] = iso8601_utc_now();

        auto t1 = isa95_prefix + std::to_string(line) + "/salida_horno/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/salida_horno/production";

        return { make_pub(t1, qual), make_pub(t2, prod) };
    }
};

// Static definitions
std::mutex SalidaHornoProcessor::mtx_;
std::unordered_map<int, SalidaHornoProcessor::State>
    SalidaHornoProcessor::states_;


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
    CalidadProcessor::reset_states();   // si lo tienes
}
