#include "MessageProcessor.hpp"
#include "JsonUtils.hpp"
#include "Shift.hpp"
#include "TimeUtils.hpp"
#include <memory>
#include <sstream>
#include <mutex>
using json = nlohmann::json;


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
 * CalidadShiftAccumulatorProcessor:
 * - Maintains per-shift accumulators for qualities 1, 2, 6 and discarded ("quebrados").
 * - Resets accumulators when the shift changes (S1->S2->S3->S1...).
 * - Thread-safe via a static mutex (safe even if processor instances are recreated).
 */
class CalidadProcessor final : public IMessageProcessor {
    // Static state shared across instances (simple + robust with current factory)
    static std::mutex mtx_;
    static int        current_shift_;     // 1,2,3
    static uint64_t   acc_q1_;
    static uint64_t   acc_q2_;
    static uint64_t   acc_q6_;
    static uint64_t   acc_discarded_;

    static void ensure_shift_is_current_and_reset_if_needed(int new_shift) {
        if (current_shift_ != new_shift) {
            // Shift boundary: reset all accumulators.
            current_shift_ = new_shift;
            acc_q1_ = acc_q2_ = acc_q6_ = acc_discarded_ = 0;
        }
    }

public:
    std::vector<Publication> process(const json& msg, const std::string& isa95_prefix) override {
        const int shift_now = static_cast<int>(current_shift_localtime());
        const int line_id   = msg.value("lineID", 0);

        // Read inputs (tolerate both "quebrados" and "quebrado")
        const int cajaCalidad = msg.value("cajaCalidad", 0);  // valid: 1,2,6
        const int quebrados   = msg.contains("quebrados")
                                  ? msg.value("quebrados", 0)
                                  : msg.value("quebrado", 0);

        {
            std::lock_guard<std::mutex> lock(mtx_);
            ensure_shift_is_current_and_reset_if_needed(shift_now);

            // Increment quality bucket by exactly 1 when cajaCalidad matches
            if      (cajaCalidad == 1) ++acc_q1_;
            else if (cajaCalidad == 2) ++acc_q2_;
            else if (cajaCalidad == 6) ++acc_q6_;
            // else: ignore unknown quality codes (0 or others)

            // Add discarded count since last Calidad report
            if (quebrados > 0) acc_discarded_ += static_cast<uint64_t>(quebrados);
        }

        // Snapshot for publishing (avoid holding the mutex while dumping JSON)
        uint64_t q1, q2, q6, disc;
        int cur_shift;
        {
            std::lock_guard<std::mutex> lock(mtx_);
            q1 = acc_q1_; q2 = acc_q2_; q6 = acc_q6_; disc = acc_discarded_;
            cur_shift = current_shift_;
        }

        json out;
        out["maquina_id"] = 8;
        out["timestamp_device"]  =  iso8601_utc_now();
        out["shift"]       = cur_shift;          // 1,2,3
        out["lineID"]      = line_id;
        out["extra_c1"]      = q1;
        out["extra_c2"]      = q2;
        out["comercial"]      = q6;
        out["quebrados"]      = disc;

        // Publish to your ISA-95-ish topics (same payload in both for now)

        const auto t1 = isa95_prefix + std::to_string(line_id) + "/calidad/production";

        return { make_pub(t1, out) };
    }
};

// --- Static member definitions
std::mutex  CalidadProcessor::mtx_;
int         CalidadProcessor::current_shift_  = static_cast<int>(current_shift_localtime());
uint64_t    CalidadProcessor::acc_q1_         = 0;
uint64_t    CalidadProcessor::acc_q2_         = 0;
uint64_t    CalidadProcessor::acc_q6_         = 0;
uint64_t    CalidadProcessor::acc_discarded_  = 0;

constexpr uint16_t PH1_COUNTER_MASK = 0x7FFF;  // keep lower 15 bits
constexpr uint16_t PH1_COUNTER_MOD  = 0x8000;  // 2^15

class PrensaHidraulica1Processor : public IMessageProcessor
{
    struct PH1State {
        bool initialized = false;
        int shift = -1;

        // Production time (WORD overflow-safe, normal 16-bit)
        uint16_t last_raw_prod_time = 0;
        double   acc_prod_time_s = 0.0;

        // Pisadas counter: true counter is 15 bits, MSB is a flag
        uint16_t last_counter15 = 0;  // last *masked* 15-bit value
        uint32_t acc_count = 0;       // accumulated pisadas in current shift
    };

    static std::mutex mtx_;
    static std::unordered_map<int, PH1State> states_;

public:
    std::vector<Publication> process(const json &msg, const std::string &isa95_prefix) override
    {
        auto sh = current_shift_localtime();
        int shiftNum = (sh == Shift::S1 ? 1 : sh == Shift::S2 ? 2 : 3);

        int line          = jsonu::get_opt<int>(msg, "lineID").value_or(0);
        int alarms        = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int raw_count     = jsonu::get_opt<int>(msg, "cantidadProductos").value_or(0);
        int raw_prod_time = jsonu::get_opt<int>(msg, "tiempoProduccion_ds").value_or(0);
        int stop_q        = jsonu::get_opt<int>(msg, "paradas").value_or(0);
        int stop_t        = jsonu::get_opt<int>(msg, "tiempoParadas_s").value_or(0);

        double   production_time_s = 0.0;
        uint32_t prod_count_shift  = 0;
        double   pisadas_min       = 0.0;

        {
            std::lock_guard<std::mutex> lock(mtx_);
            PH1State &st = states_[line];

            // Time: normal 16-bit word, no special flag
            uint16_t time_raw = static_cast<uint16_t>(raw_prod_time);

            // Counter: mask off MSB; treat it as a 15-bit modulo counter
            uint16_t count_word   = static_cast<uint16_t>(raw_count);
            uint16_t count_15bit  = static_cast<uint16_t>(count_word & PH1_COUNTER_MASK);

            if (!st.initialized || st.shift != shiftNum) {
                st.initialized = true;
                st.shift = shiftNum;

                st.last_raw_prod_time = time_raw;
                st.acc_prod_time_s    = 0.0;

                st.last_counter15 = count_15bit;
                st.acc_count      = 0;
            }
            else {
                // --- Production time accumulator (normal 16-bit modulo 65536) ---
                uint16_t prev_t = st.last_raw_prod_time;
                uint16_t diff_t = (time_raw >= prev_t)
                    ? static_cast<uint16_t>(time_raw - prev_t)
                    : static_cast<uint16_t>(time_raw + (uint16_t(65536) - prev_t));

                st.acc_prod_time_s += diff_t * 0.1;  // deciseconds → seconds
                st.last_raw_prod_time = time_raw;

                // --- Pisadas accumulator: 15-bit modulo counter (0..32767) ---
                uint16_t prev_c = st.last_counter15;
                uint16_t curr_c = count_15bit;

                uint16_t diff_c;
                if (curr_c >= prev_c) {
                    diff_c = static_cast<uint16_t>(curr_c - prev_c);
                } else {
                    // wrap in 15 bits
                    diff_c = static_cast<uint16_t>(PH1_COUNTER_MOD + curr_c - prev_c);
                }

                st.acc_count      += diff_c;
                st.last_counter15  = curr_c;
            }

            production_time_s = st.acc_prod_time_s;
            prod_count_shift  = st.acc_count;

            if (production_time_s > 0.1) {
                double minutes_prod = production_time_s / 60.0;
                pisadas_min = prod_count_shift / minutes_prod;
            } else {
                pisadas_min = 0.0;
            }
        }

        json qual;
        qual["alarms"]           = alarms;
        qual["timestamp_device"] = iso8601_utc_now();

        json prod;
        prod["maquina_id"]          = 1;
        prod["turno"]               = shiftNum;
        prod["cantidadProductos"]   = prod_count_shift * PIEZAS_PISADA;
        prod["cantidadPisadas"]     = prod_count_shift;
        prod["tiempoProduccion_s"]  = static_cast<uint32_t>(production_time_s);
        prod["cantidadPisadas_min"] = static_cast<uint32_t>(pisadas_min);
        prod["paradas"]             = stop_q;
        prod["tiempoParadas_s"]     = stop_t;
        prod["timestamp_device"]    = iso8601_utc_now();

        auto t1 = isa95_prefix + std::to_string(line) + "/prensa_hidraulica1/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/prensa_hidraulica1/production";

        return { make_pub(t1, qual), make_pub(t2, prod) };
    }
};

// ===== STATIC DEFINITIONS FOR PrensaHidraulica1Processor =====
std::mutex PrensaHidraulica1Processor::mtx_;
std::unordered_map<int, PrensaHidraulica1Processor::PH1State>
    PrensaHidraulica1Processor::states_;

class PrensaHidraulica2Processor : public IMessageProcessor
{
    // --- Per-PLC state ---
    struct PH2State {
        bool initialized = false;
        int  shift       = -1;

        // Production time (normal 16-bit word)
        uint16_t last_raw_prod_time = 0;
        double   acc_prod_time_s    = 0.0;     // seconds

        // Pisadas counter (MSB = flag → use only low 15 bits)
        uint16_t last_counter15     = 0;       // masked 15-bit counter
        uint32_t acc_count          = 0;       // accumulated pisadas
    };

    static std::mutex mtx_;
    static std::unordered_map<int, PH2State> states_;

    // Mask and modulo for 15-bit counter
    static constexpr uint16_t COUNTER_MASK = 0x7FFF;
    static constexpr uint16_t COUNTER_MOD  = 0x8000;

public:
    std::vector<Publication> process(const json &msg, const std::string &isa95_prefix) override
    {
        // ---- Current shift ----
        auto sh = current_shift_localtime();
        int shiftNum = (sh == Shift::S1 ? 1 : (sh == Shift::S2 ? 2 : 3));

        // ---- Extract fields ----
        int line          = jsonu::get_opt<int>(msg, "lineID").value_or(0);
        int alarms        = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int raw_count_i   = jsonu::get_opt<int>(msg, "cantidadProductos").value_or(0);
        int raw_time_i    = jsonu::get_opt<int>(msg, "tiempoProduccion_ds").value_or(0);
        int stop_q        = jsonu::get_opt<int>(msg, "paradas").value_or(0);
        int stop_t        = jsonu::get_opt<int>(msg, "tiempoParadas_s").value_or(0);

        double   production_time_s = 0.0;
        uint32_t prod_count_shift  = 0;
        double   pisadas_min       = 0.0;

        {
            std::lock_guard<std::mutex> lock(mtx_);

            PH2State &st = states_[line];

            uint16_t time_raw = static_cast<uint16_t>(raw_time_i);

            // --- Mask off flag bit (MSB) from counter ---
            uint16_t count_word   = static_cast<uint16_t>(raw_count_i);
            uint16_t count_15bit  = static_cast<uint16_t>(count_word & COUNTER_MASK);

            // ---- First message of shift ----
            if (!st.initialized || st.shift != shiftNum) {
                st.initialized = true;
                st.shift       = shiftNum;

                st.last_raw_prod_time = time_raw;
                st.acc_prod_time_s    = 0.0;

                st.last_counter15     = count_15bit;
                st.acc_count          = 0;
            }
            else {
                // --- Production time accumulator (16-bit modulo) ---
                uint16_t prev_t = st.last_raw_prod_time;
                uint16_t diff_t =
                    (time_raw >= prev_t)
                    ? static_cast<uint16_t>(time_raw - prev_t)
                    : static_cast<uint16_t>(time_raw + (65536 - prev_t));

                st.acc_prod_time_s += diff_t * 0.1;   // ds → s
                st.last_raw_prod_time = time_raw;

                // --- Pisadas accumulator using 15-bit modulo ---
                uint16_t prev_c = st.last_counter15;
                uint16_t curr_c = count_15bit;

                uint16_t diff_c;
                if (curr_c >= prev_c)
                    diff_c = static_cast<uint16_t>(curr_c - prev_c);
                else
                    diff_c = static_cast<uint16_t>(COUNTER_MOD + curr_c - prev_c);

                st.acc_count      += diff_c;
                st.last_counter15  = curr_c;
            }

            production_time_s = st.acc_prod_time_s;
            prod_count_shift  = st.acc_count;

            // ---- Pisadas per minute (based on production time only) ----
            if (production_time_s > 1.0) {
                double minutes_prod = production_time_s / 60.0;
                pisadas_min = prod_count_shift / minutes_prod;
            }
            else {
                pisadas_min = 0.0;
            }
        }

        // ---- Build publications ----
        json qual;
        qual["alarms"]           = alarms;
        qual["timestamp_device"] = iso8601_utc_now();

        json prod;
        prod["maquina_id"]          = 1;
        prod["turno"]               = shiftNum;

        prod["cantidadPisadas"]     = prod_count_shift;
        prod["cantidadPisadas_min"] = static_cast<uint32_t>(pisadas_min);
        prod["cantidadProductos"]   = prod_count_shift * PIEZAS_PISADA;
        prod["tiempoProduccion_s"]  = static_cast<uint32_t>(production_time_s);

        prod["paradas"]             = stop_q;
        prod["tiempoParadas_s"]     = stop_t;
        prod["timestamp_device"]    = iso8601_utc_now();

        auto t1 = isa95_prefix + std::to_string(line) + "/prensa_hidraulica2/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/prensa_hidraulica2/production";

        return { make_pub(t1, qual), make_pub(t2, prod) };
    }
};


// ---- STATIC DEFINITIONS ----
std::mutex PrensaHidraulica2Processor::mtx_;
std::unordered_map<int, PrensaHidraulica2Processor::PH2State>
    PrensaHidraulica2Processor::states_;

class EntradaSecadorProcessor : public IMessageProcessor
{
    struct ESState {
        bool initialized = false;
        int  shift       = -1;

        // Operation time (15-bit seconds, MSB is a flag)
        uint16_t last_time15   = 0;
        double   acc_time_s    = 0.0;

        // Arranques (15-bit counter, MSB is a flag)
        uint16_t last_count15  = 0;
        uint32_t acc_arranques = 0;
    };

    static std::mutex mtx_;
    static std::unordered_map<int, ESState> states_;

    static constexpr uint16_t MASK_15 = 0x7FFF;  // bits 0-14
    static constexpr uint16_t MOD_15  = 0x8000;  // 2^15

public:
    std::vector<Publication> process(const json &msg, const std::string &isa95_prefix) override
    {
        auto sh = current_shift_localtime();
        int  shiftNum = (sh == Shift::S1 ? 1 : (sh == Shift::S2 ? 2 : 3));

        int line       = jsonu::get_opt<int>(msg, "lineID").value_or(0);
        int alarms     = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int raw_arr    = jsonu::get_opt<int>(msg, "arranques").value_or(0);
        int raw_time_s = jsonu::get_opt<int>(msg, "tiempoOperacion_s").value_or(0);

        double   acc_time_out   = 0.0;
        uint32_t arranques_out  = 0;

        {
            std::lock_guard<std::mutex> lock(mtx_);
            ESState &st = states_[line];

            // Mask MSB off both counters (15-bit true values)
            uint16_t time15  = static_cast<uint16_t>(raw_time_s) & MASK_15;
            uint16_t count15 = static_cast<uint16_t>(raw_arr)    & MASK_15;

            if (!st.initialized || st.shift != shiftNum) {
                // New shift or first sample
                st.initialized   = true;
                st.shift         = shiftNum;

                st.last_time15   = time15;
                st.acc_time_s    = 0.0;

                st.last_count15  = count15;
                st.acc_arranques = 0;
            }
            else {
                // --- Time delta, modulo 15 bits ---
                uint16_t prev_t = st.last_time15;
                uint16_t dt;
                if (time15 >= prev_t)
                    dt = static_cast<uint16_t>(time15 - prev_t);
                else
                    dt = static_cast<uint16_t>(MOD_15 + time15 - prev_t);

                st.acc_time_s += dt;       // already in seconds
                st.last_time15 = time15;

                // --- Arranques delta, modulo 15 bits ---
                uint16_t prev_c = st.last_count15;
                uint16_t dc;
                if (count15 >= prev_c)
                    dc = static_cast<uint16_t>(count15 - prev_c);
                else
                    dc = static_cast<uint16_t>(MOD_15 + count15 - prev_c);

                st.acc_arranques += dc;
                st.last_count15   = count15;
            }

            acc_time_out  = st.acc_time_s;
            arranques_out = st.acc_arranques;
        }

        // --- Build publications ---
        json qual;
        qual["alarms"] = alarms;
        qual["ts"]     = iso8601_utc_now();

        json prod;
        prod["maquina_id"]        = 3;
        prod["turno"]             = shiftNum;
        prod["cantidad_arranques"] = arranques_out;
        prod["tiempo_operacion"]   = static_cast<uint32_t>(acc_time_out);
        prod["timestamp_device"]   = iso8601_utc_now();

        auto t1 = isa95_prefix + std::to_string(line) + "/entrada_secador/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/entrada_secador/production";

        return { make_pub(t1, qual), make_pub(t2, prod) };
    }
};

// In MessageProcessor.cpp (once, not in header)
std::mutex EntradaSecadorProcessor::mtx_;
std::unordered_map<int, EntradaSecadorProcessor::ESState>
    EntradaSecadorProcessor::states_;


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

        // tiempoProduccion_ds (16-bit, deciseconds, clean)
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
    std::vector<Publication> process(const json &msg,
                                     const std::string &isa95_prefix) override
    {
        // ---- Current shift ----
        auto sh      = current_shift_localtime();
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

            // Mask MSB for counters that use 15-bit + flag
            uint16_t prod_q15  = static_cast<uint16_t>(prod_q) & MASK_15;
            uint16_t stop_q15  = static_cast<uint16_t>(stop_q) & MASK_15;
            uint16_t prod_t16  = static_cast<uint16_t>(prod_t);            // full 16 bits
            uint16_t stop_t15  = static_cast<uint16_t>(stop_t) & MASK_15;

            // ---- Initialize or shift change ----
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

                // ---- tiempoProduccion_ds (16-bit modulo, ds → s) ----
                {
                    uint16_t prev = st.last_raw_prod_t;
                    uint16_t curr = prod_t16;
                    uint16_t diff =
                        (curr >= prev)
                            ? static_cast<uint16_t>(curr - prev)
                            : static_cast<uint16_t>(curr + (uint16_t)(65536 - prev));

                    st.acc_prod_t_s += diff * 0.1; // deciseconds → seconds
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
        prod["maquina_id"]        = 4;
        prod["turno"]             = shiftNum;

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


class EsmalteProcessor : public IMessageProcessor
{
    struct State {
        bool initialized = false;
        int shift = -1;

        uint16_t last_raw_prod_q = 0;   // 15-bit masked value
        uint32_t acc_prod_q = 0;

        uint16_t last_raw_stop_q = 0;
        uint32_t acc_stop_q = 0;

        uint16_t last_raw_prod_t = 0;   // tiempoProduccion_ds
        double   acc_prod_t_s = 0.0;

        uint16_t last_raw_stop_t = 0;   // tiempoParadas_s
        uint32_t acc_stop_t_s = 0;
    };

    static std::mutex mtx_;
    static std::unordered_map<int, State> states_;

    // ---- Corrected overflow-safe diff with MSB masking ----
    static uint16_t clean15(int x) {
        return static_cast<uint16_t>(x) & 0x7FFF;   // remove MSB bank flag
    }

    static uint16_t diff16(uint16_t curr, uint16_t prev) {
        return (curr >= prev)
            ? (curr - prev)
            : static_cast<uint16_t>(curr + (65536 - prev));
    }

public:
    std::vector<Publication> process(const json &msg,
                                     const std::string &isa95_prefix) override
    {
        // ---- Determine shift ----
        auto sh = current_shift_localtime();
        int shiftNum = (sh == Shift::S1 ? 1 : sh == Shift::S2 ? 2 : 3);

        // ---- Extract PLC 16-bit fields ----
        int alarms   = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int prod_q   = jsonu::get_opt<int>(msg, "cantidadProductos").value_or(0);
        int prod_t   = jsonu::get_opt<int>(msg, "tiempoProduccion_ds").value_or(0);
        int line     = jsonu::get_opt<int>(msg, "lineID").value_or(0);
        int stop_q   = jsonu::get_opt<int>(msg, "paradas").value_or(0);
        int stop_t   = jsonu::get_opt<int>(msg, "tiempoParadas_s").value_or(0);

        // ---- Final accumulated outputs ----
        uint32_t prod_q_shift = 0;
        uint32_t stop_q_shift = 0;
        double   prod_t_shift_s = 0.0;
        uint32_t stop_t_shift_s = 0;

        {
            std::lock_guard<std::mutex> lock(mtx_);
            State &st = states_[line];

            // ---- Mask MSB and keep only 15 real bits ----
            uint16_t raw_prod_q = clean15(prod_q);
            uint16_t raw_stop_q = clean15(stop_q);
            uint16_t raw_prod_t = clean15(prod_t);
            uint16_t raw_stop_t = clean15(stop_t);

            // ---- First message / new shift ----
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
                // ---- cantidadProductos ----
                st.acc_prod_q += diff16(raw_prod_q, st.last_raw_prod_q);
                st.last_raw_prod_q = raw_prod_q;

                // ---- paradas ----
                st.acc_stop_q += diff16(raw_stop_q, st.last_raw_stop_q);
                st.last_raw_stop_q = raw_stop_q;

                // ---- tiempoProduccion_ds (×0.1 seconds) ----
                st.acc_prod_t_s += diff16(raw_prod_t, st.last_raw_prod_t) * 0.1;
                st.last_raw_prod_t = raw_prod_t;

                // ---- tiempoParadas_s ----
                st.acc_stop_t_s += diff16(raw_stop_t, st.last_raw_stop_t);
                st.last_raw_stop_t = raw_stop_t;
            }

            // ---- Copy results ----
            prod_q_shift = st.acc_prod_q;
            stop_q_shift = st.acc_stop_q;
            prod_t_shift_s = st.acc_prod_t_s;
            stop_t_shift_s = st.acc_stop_t_s;
        }

        // ---- Build JSON payloads ----
        json qual;
        qual["alarms"] = alarms;
        qual["timestamp_device"] = iso8601_utc_now();

        json prod;
        prod["maquina_id"] = 5;
        prod["turno"] = shiftNum;
        prod["cantidad_produccion"] = prod_q_shift;
        prod["tiempo_produccion"] = (uint32_t)prod_t_shift_s;
        prod["cantidad_paradas"] = stop_q_shift;
        prod["tiempo_paradas"] = stop_t_shift_s;
        prod["timestamp_device"] = iso8601_utc_now();

        auto t1 = isa95_prefix + std::to_string(line) + "/esmalte/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/esmalte/production";

        return {make_pub(t1, qual), make_pub(t2, prod)};
    }
};

// ---- STATIC DEFINITIONS ----
std::mutex EsmalteProcessor::mtx_;
std::unordered_map<int, EsmalteProcessor::State>
    EsmalteProcessor::states_;


class EntradaHornoProcessor : public IMessageProcessor
{
    struct State {
        bool initialized = false;
        int shift = -1;

        // cantidad (WORD counter, 15-bit value with MSB as bank flag)
        uint16_t last_raw_prod_q = 0;
        uint32_t acc_prod_q = 0;

        // paradas (WORD counter)
        uint16_t last_raw_stop_q = 0;
        uint32_t acc_stop_q = 0;

        // fallaHorno (WORD counter)
        uint16_t last_raw_falla_q = 0;
        uint32_t acc_falla_q = 0;

        // tiempoProduccion_ds (WORD, deciseconds)
        uint16_t last_raw_prod_t = 0;
        double   acc_prod_t_s = 0.0;

        // tiempoParadas_s (WORD seconds)
        uint16_t last_raw_stop_t = 0;
        uint32_t acc_stop_t_s = 0;

        // tiempoFalla_s (WORD seconds)
        uint16_t last_raw_falla_t = 0;
        uint32_t acc_falla_t_s = 0;
    };

    static std::mutex mtx_;
    static std::unordered_map<int, State> states_;

    // Remove MSB "bank" bit, keep only 15 real bits
    static uint16_t clean15(int x) {
        return static_cast<uint16_t>(x) & 0x7FFF;
    }

    static uint16_t diff16(uint16_t curr, uint16_t prev) {
        return (curr >= prev)
            ? static_cast<uint16_t>(curr - prev)
            : static_cast<uint16_t>(curr + (65536 - prev));
    }

public:
    std::vector<Publication> process(const json &msg,
                                     const std::string &isa95_prefix) override
    {
        // ---- Determine shift ----
        auto sh = current_shift_localtime();
        int shiftNum = (sh == Shift::S1 ? 1 : sh == Shift::S2 ? 2 : 3);

        // ---- Read PLC fields (raw 16-bit words) ----
        int alarms  = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int prod_q  = jsonu::get_opt<int>(msg, "cantidad").value_or(0);
        int prod_t  = jsonu::get_opt<int>(msg, "tiempoProduccion_ds").value_or(0);
        int line    = jsonu::get_opt<int>(msg, "lineID").value_or(0);

        int stop_q  = jsonu::get_opt<int>(msg, "paradas").value_or(0);
        int stop_t  = jsonu::get_opt<int>(msg, "tiempoParadas_s").value_or(0);

        int falla_q = jsonu::get_opt<int>(msg, "fallaHorno").value_or(0);
        int falla_t = jsonu::get_opt<int>(msg, "tiempoFalla_s").value_or(0);

        // ---- Output accumulators ----
        uint32_t prod_q_shift   = 0;
        uint32_t stop_q_shift   = 0;
        uint32_t falla_q_shift  = 0;

        double   prod_t_shift_s = 0.0;
        uint32_t stop_t_shift_s = 0;
        uint32_t falla_t_shift_s = 0;

        {
            std::lock_guard<std::mutex> lock(mtx_);
            State &st = states_[line];

            // Mask MSB (bank bit) → use only 0..32767 as real value
            uint16_t raw_prod_q  = clean15(prod_q);
            uint16_t raw_stop_q  = clean15(stop_q);
            uint16_t raw_falla_q = clean15(falla_q);

            uint16_t raw_prod_t  = clean15(prod_t);
            uint16_t raw_stop_t  = clean15(stop_t);
            uint16_t raw_falla_t = clean15(falla_t);

            // ---- First message or shift change ----
            if (!st.initialized || st.shift != shiftNum) {
                st = State(); // reset whole structure
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
                // ---- cantidad ----
                st.acc_prod_q += diff16(raw_prod_q, st.last_raw_prod_q);
                st.last_raw_prod_q = raw_prod_q;

                // ---- paradas ----
                st.acc_stop_q += diff16(raw_stop_q, st.last_raw_stop_q);
                st.last_raw_stop_q = raw_stop_q;

                // ---- fallaHorno ----
                st.acc_falla_q += diff16(raw_falla_q, st.last_raw_falla_q);
                st.last_raw_falla_q = raw_falla_q;

                // ---- tiempoProduccion_ds → seconds ----
                st.acc_prod_t_s += diff16(raw_prod_t, st.last_raw_prod_t) * 0.1;
                st.last_raw_prod_t = raw_prod_t;

                // ---- tiempoParadas_s ----
                st.acc_stop_t_s += diff16(raw_stop_t, st.last_raw_stop_t);
                st.last_raw_stop_t = raw_stop_t;

                // ---- tiempoFalla_s ----
                st.acc_falla_t_s += diff16(raw_falla_t, st.last_raw_falla_t);
                st.last_raw_falla_t = raw_falla_t;
            }

            // Copy accumulated results
            prod_q_shift   = st.acc_prod_q;
            stop_q_shift   = st.acc_stop_q;
            falla_q_shift  = st.acc_falla_q;

            prod_t_shift_s  = st.acc_prod_t_s;
            stop_t_shift_s  = st.acc_stop_t_s;
            falla_t_shift_s = st.acc_falla_t_s;
        }

        // ---- Build output JSON ----
        json qual;
        qual["alarms"] = alarms;
        qual["ts"] = iso8601_utc_now();

        json prod;
        prod["maquina_id"] = 6;
        prod["turno"] = shiftNum;

        // Shift accumulated
        prod["cantidad_produccion"] = prod_q_shift;
        prod["cantidad_paradas"]    = stop_q_shift;
        prod["cantidad_fallas"]     = falla_q_shift;

        prod["tiempo_produccion"] = static_cast<uint32_t>(prod_t_shift_s);
        prod["tiempo_paradas"]    = stop_t_shift_s;
        prod["tiempo_fallas"]     = falla_t_shift_s;

        prod["timestamp_device"] = iso8601_utc_now();

        auto t1 = isa95_prefix + std::to_string(line) + "/entrada_horno/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/entrada_horno/production";

        return {make_pub(t1, qual), make_pub(t2, prod)};
    }
};

// ---- STATIC DEFINITIONS ----
std::mutex EntradaHornoProcessor::mtx_;
std::unordered_map<int, EntradaHornoProcessor::State>
    EntradaHornoProcessor::states_;


class SalidaHornoProcessor : public IMessageProcessor
{
    struct State {
        bool initialized = false;
        int shift = -1;

        // cantidad (WORD counter)
        uint16_t last_raw_prod_q = 0;
        uint32_t acc_prod_q = 0;

        // cantidad_total (WORD counter)
        uint16_t last_raw_prod_qtotal = 0;
        uint32_t acc_prod_qtotal = 0;

        // tiempoProduccion_ds (WORD, 0.1s units)
        uint16_t last_raw_prod_t = 0;
        double   acc_prod_t_s = 0.0;
    };

    static std::mutex mtx_;
    static std::unordered_map<int, State> states_;

    // MSB mask (fix for bank toggle issue)
    static inline uint16_t clean15(int x) {
        return static_cast<uint16_t>(x) & 0x7FFF;
    }

    static uint16_t diff16(uint16_t curr, uint16_t prev) {
        return (curr >= prev)
            ? (curr - prev)
            : static_cast<uint16_t>(curr + (65536 - prev));
    }

public:
    std::vector<Publication> process(const json &msg,
                                     const std::string &isa95_prefix) override
    {
        // ---- Determine shift ----
        auto sh = current_shift_localtime();
        int shiftNum = (sh == Shift::S1 ? 1 : sh == Shift::S2 ? 2 : 3);

        // ---- Read PLC values ----
        int alarms       = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int prod_q_in    = jsonu::get_opt<int>(msg, "cantidad").value_or(0);
        int prod_qt_in   = jsonu::get_opt<int>(msg, "cantidad_total").value_or(0);
        int prod_t_in    = jsonu::get_opt<int>(msg, "tiempoProduccion_ds").value_or(0);
        int line         = jsonu::get_opt<int>(msg, "lineID").value_or(0);

        // ---- Clean MSB bank bit (mask 0x8000) ----
        uint16_t raw_prod_q      = clean15(prod_q_in);
        uint16_t raw_prod_qtotal = clean15(prod_qt_in);
        uint16_t raw_prod_t      = clean15(prod_t_in);

        // ---- Outputs ----
        uint32_t prod_q_shift      = 0;
        uint32_t prod_qtotal_shift = 0;
        double   prod_t_shift_s    = 0.0;

        {
            std::lock_guard<std::mutex> lock(mtx_);
            State &st = states_[line];

            // ---- First message or shift change ----
            if (!st.initialized || st.shift != shiftNum) {
                st = State();
                st.initialized = true;
                st.shift = shiftNum;

                st.last_raw_prod_q      = raw_prod_q;
                st.last_raw_prod_qtotal = raw_prod_qtotal;
                st.last_raw_prod_t      = raw_prod_t;
            }
            else {
                // ---- cantidad ----
                st.acc_prod_q += diff16(raw_prod_q, st.last_raw_prod_q);
                st.last_raw_prod_q = raw_prod_q;

                // ---- cantidad_total ----
                st.acc_prod_qtotal += diff16(raw_prod_qtotal, st.last_raw_prod_qtotal);
                st.last_raw_prod_qtotal = raw_prod_qtotal;

                // ---- tiempoProduccion_ds → seconds ----
                st.acc_prod_t_s += diff16(raw_prod_t, st.last_raw_prod_t) * 0.1;
                st.last_raw_prod_t = raw_prod_t;
            }

            // Final accumulated results
            prod_q_shift      = st.acc_prod_q;
            prod_qtotal_shift = st.acc_prod_qtotal;
            prod_t_shift_s    = st.acc_prod_t_s;
        }

        // ---- Build JSON outputs ----
        json qual;
        qual["alarms"] = alarms;
        qual["ts"] = iso8601_utc_now();

        json prod;
        prod["maquina_id"] = 7;
        prod["turno"] = shiftNum;

        // Shift accumulated values
        prod["cantidad_produccion"] = prod_q_shift;
        prod["cantidad_total"] = prod_qtotal_shift;
        prod["tiempoProduccion_s"] = static_cast<uint32_t>(prod_t_shift_s);

        prod["timestamp_device"] = iso8601_utc_now();

        // ---- Topics ----
        auto t1 = isa95_prefix + std::to_string(line) + "/salida_horno/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/salida_horno/production";

        return {make_pub(t1, qual), make_pub(t2, prod)};
    }
};

// ---- STATIC DEFINITIONS ----
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
