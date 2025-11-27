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
 * CalidadShiftAccumulatorProcessor:
 * - Maintains per-shift accumulators for qualities 1, 2, 6 and discarded ("quebrados").
 * - Resets accumulators when the shift changes (S1->S2->S3->S1...).
 * - Thread-safe via a static mutex (safe even if processor instances are recreated).
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

        const int cajaCalidad = msg.value("cajaCalidad", 0); // 1,2,6
        const int quebrados   = msg.contains("quebrados")
                                  ? msg.value("quebrados", 0)
                                  : msg.value("quebrado", 0);

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

            // Count exactly one for each cajaCalidad message
            if      (cajaCalidad == 1) ++st.acc_q1;
            else if (cajaCalidad == 2) ++st.acc_q2;
            else if (cajaCalidad == 6) ++st.acc_q6;

            // quebrados is a delta
            if (quebrados > 0)
                st.acc_discarded += static_cast<uint64_t>(quebrados);

            // snapshot
            q1 = st.acc_q1;
            q2 = st.acc_q2;
            q6 = st.acc_q6;
            disc = st.acc_discarded;
        }

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

// static definitions
std::mutex CalidadProcessor::mtx_;
std::unordered_map<int, CalidadProcessor::LineState> CalidadProcessor::states_;

void CalidadProcessor::reset_states() {
    std::lock_guard<std::mutex> lock(mtx_);
    states_.clear();
}

class PrensaHidraulica1Processor : public IMessageProcessor
{
    struct PH1State {
        bool initialized = false;
        int  shift       = -1;

        uint16_t last_raw_prod_time = 0;
        double   acc_prod_time_s    = 0.0;

        uint16_t last_counter15     = 0;   // 15-bit masked contador
        uint32_t acc_count          = 0;
    };

    static std::mutex mtx_;
    static std::unordered_map<int, PH1State> states_;

    static constexpr uint16_t COUNTER_MASK = 0x7FFF; // 15 bits
    static constexpr uint16_t COUNTER_MOD  = 0x8000; // overflow at 32768

public:
 static void reset_states();
    std::vector<Publication> process(const json &msg,
                                     const std::string &isa95_prefix) override
    {
        auto sh = current_shift_localtime();
        int shiftNum = (sh == Shift::S1 ? 1 : (sh == Shift::S2 ? 2 : 3));

        // ---- Inputs (idénticos al original PH1) ----
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

            PH1State &st = states_[line];

            uint16_t time_raw = static_cast<uint16_t>(raw_time_i);

            // ---- contadorProductos viene con MSB flag -> nos quedamos con 15 bits ----
            uint16_t count_raw  = static_cast<uint16_t>(raw_count_i);
            uint16_t count15    = static_cast<uint16_t>(count_raw & COUNTER_MASK);

            if (!st.initialized || st.shift != shiftNum) {
                st.initialized        = true;
                st.shift              = shiftNum;

                st.last_raw_prod_time = time_raw;
                st.acc_prod_time_s    = 0.0;

                st.last_counter15     = count15;
                st.acc_count          = 0;
            }
            else {
                // ---- tiempoProduccion_ds (overflow 16 bits) ----
                uint16_t prev_t = st.last_raw_prod_time;
                uint16_t diff_t =
                    (time_raw >= prev_t)
                    ? time_raw - prev_t
                    : static_cast<uint16_t>(65536 - prev_t + time_raw);

                st.acc_prod_time_s += diff_t * 0.1;
                st.last_raw_prod_time = time_raw;

                // ---- contador pisadas (overflow 15 bits) ----
                uint16_t prev_c = st.last_counter15;
                uint16_t curr_c = count15;

                uint16_t diff_c =
                    (curr_c >= prev_c)
                    ? curr_c - prev_c
                    : static_cast<uint16_t>(COUNTER_MOD + curr_c - prev_c);

                st.acc_count += diff_c;
                st.last_counter15 = curr_c;
            }

            production_time_s = st.acc_prod_time_s;
            prod_count_shift  = st.acc_count;

            if (production_time_s > 1.0) {
                pisadas_min = prod_count_shift / (production_time_s / 60.0);
            }
        }

        // ---- Publicación ----
        json qual;
        qual["alarms"]           = alarms;
        qual["timestamp_device"] = iso8601_utc_now();

        json prod;
        prod["maquina_id"]          = 1;
        prod["turno"]               = shiftNum;

        prod["cantidadPisadas"]     = prod_count_shift;
        prod["cantidadPisadas_min"] = static_cast<uint32_t>(pisadas_min);

        // MISMA fórmula original
        prod["cantidadProductos"]   = prod_count_shift * PIEZAS_PISADA;

        prod["tiempoProduccion_s"]  = static_cast<uint32_t>(production_time_s);
        prod["paradas"]             = stop_q;
        prod["tiempoParadas_s"]     = stop_t;

        prod["timestamp_device"]    = iso8601_utc_now();

        auto t1 = isa95_prefix + std::to_string(line) + "/prensa_hidraulica1/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/prensa_hidraulica1/production";

        return {make_pub(t1, qual), make_pub(t2, prod)};
    }
};

std::mutex PrensaHidraulica1Processor::mtx_;
std::unordered_map<int, PrensaHidraulica1Processor::PH1State>
    PrensaHidraulica1Processor::states_;

void PrensaHidraulica1Processor::reset_states()
{
    std::lock_guard<std::mutex> lk(mtx_);
    states_.clear();
}

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

    // 15-bit counter: MSB is a flag, thrown away
    static constexpr uint16_t COUNTER_MASK = 0x7FFF;
    static constexpr uint16_t COUNTER_MOD  = 0x8000;

public:
static void reset_states();
    std::vector<Publication> process(const json &msg, const std::string &isa95_prefix) override
    {
        // ---- Current shift ----
        auto sh = current_shift_localtime();
        int shiftNum = (sh == Shift::S1 ? 1 : (sh == Shift::S2 ? 2 : 3));

        // ---- Extract fields (EXACT SAME INPUT AS BEFORE) ----
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

            // --- Production time raw word (normal 16-bit offset) ---
            uint16_t time_raw = static_cast<uint16_t>(raw_time_i);

            // --- Pisadas: remove MSB flag (the PLC intentionally sets MSB flags here) ---
            uint16_t count_word  = static_cast<uint16_t>(raw_count_i);
            uint16_t count15     = static_cast<uint16_t>(count_word & COUNTER_MASK);

            // ---- First message of shift ----
            if (!st.initialized || st.shift != shiftNum) {
                st.initialized        = true;
                st.shift              = shiftNum;
                st.last_raw_prod_time = time_raw;
                st.acc_prod_time_s    = 0.0;

                st.last_counter15     = count15;
                st.acc_count          = 0;
            }
            else {
                // ---- Production time accumulator (normal 16-bit mod) ----
                uint16_t prev_t = st.last_raw_prod_time;
                uint16_t diff_t =
                    (time_raw >= prev_t)
                    ? static_cast<uint16_t>(time_raw - prev_t)
                    : static_cast<uint16_t>(time_raw + (65536 - prev_t));

                st.acc_prod_time_s += diff_t * 0.1;     // ds → seconds
                st.last_raw_prod_time = time_raw;

                // ---- Pisadas accumulator using 15-bit modulo ----
                uint16_t prev_c = st.last_counter15;
                uint16_t curr_c = count15;

                uint16_t diff_c;
                if (curr_c >= prev_c)
                    diff_c = static_cast<uint16_t>(curr_c - prev_c);
                else
                    diff_c = static_cast<uint16_t>(COUNTER_MOD + curr_c - prev_c);

                st.acc_count += diff_c;
                st.last_counter15 = curr_c;
            }

            production_time_s = st.acc_prod_time_s;
            prod_count_shift  = st.acc_count;

            // ---- Pisadas per minute (with the same original logic) ----
            if (production_time_s > 1.0) {
                double minutes_prod = production_time_s / 60.0;
                pisadas_min = prod_count_shift / minutes_prod;
            }
        }

        // ---- Build publications (EXACT SAME STRUCTURE AS ORIGINAL) ----
        json qual;
        qual["alarms"]           = alarms;
        qual["timestamp_device"] = iso8601_utc_now();

        json prod;
        prod["maquina_id"]          = 1;
        prod["turno"]               = shiftNum;

        prod["cantidadPisadas"]     = prod_count_shift;
        prod["cantidadPisadas_min"] = static_cast<uint32_t>(pisadas_min);

        // ORIGINAL: cantidadProductos = pisadas * PIEZAS_PISADA
        prod["cantidadProductos"]   = prod_count_shift * PIEZAS_PISADA;

        prod["tiempoProduccion_s"]  = static_cast<uint32_t>(production_time_s);

        prod["paradas"]             = stop_q;
        prod["tiempoParadas_s"]     = stop_t;

        prod["timestamp_device"]   = iso8601_utc_now();

        auto t1 = isa95_prefix + std::to_string(line) + "/prensa_hidraulica2/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/prensa_hidraulica2/production";

        return { make_pub(t1, qual), make_pub(t2, prod) };
    }
};

// Static definitions
std::mutex PrensaHidraulica2Processor::mtx_;
std::unordered_map<int, PrensaHidraulica2Processor::PH2State>
    PrensaHidraulica2Processor::states_;
void PrensaHidraulica2Processor::reset_states()
{
    std::lock_guard<std::mutex> lk(mtx_);
    states_.clear();
}
class EntradaSecadorProcessor : public IMessageProcessor
{
    struct ESState {
        bool initialized = false;
        int  shift       = -1;

        // Operation time (15-bit seconds, MSB = flag)
        uint16_t last_time15   = 0;
        double   acc_time_s    = 0.0;

        // Arranques (15-bit counter, MSB = flag)
        uint16_t last_count15  = 0;
        uint32_t acc_arranques = 0;
    };

    static std::mutex mtx_;
    static std::unordered_map<int, ESState> states_;

    // 15-bit logic (MSB=flag)
    static constexpr uint16_t MASK_15 = 0x7FFF;
    static constexpr uint16_t MOD_15  = 0x8000;

public:
static void reset_states();
    std::vector<Publication> process(const json &msg, const std::string &isa95_prefix) override
    {
        // ---- Turno actual ----
        auto sh = current_shift_localtime();
        int  shiftNum = (sh == Shift::S1 ? 1 : (sh == Shift::S2 ? 2 : 3));

        // ---- Inputs del mensaje ----
        int line       = jsonu::get_opt<int>(msg, "lineID").value_or(0);
        int alarms     = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int raw_arr    = jsonu::get_opt<int>(msg, "arranques").value_or(0);
        int raw_time_s = jsonu::get_opt<int>(msg, "tiempoOperacion_s").value_or(0);

        double   acc_time_out  = 0.0;
        uint32_t arranques_out = 0;

        {
            std::lock_guard<std::mutex> lock(mtx_);
            ESState &st = states_[line];

            // ---- Aplicar mask MSB y obtener contador real 15-bit ----
            uint16_t time15  = static_cast<uint16_t>(raw_time_s) & MASK_15;
            uint16_t count15 = static_cast<uint16_t>(raw_arr)    & MASK_15;

            // ---- Primer mensaje del turno ----
            if (!st.initialized || st.shift != shiftNum) {
                st.initialized   = true;
                st.shift         = shiftNum;

                st.last_time15   = time15;
                st.acc_time_s    = 0.0;

                st.last_count15  = count15;
                st.acc_arranques = 0;
            }
            else {
                // ====== Tiempo acumulado (overflow 15-bit) ======
                uint16_t prev_t = st.last_time15;
                uint16_t dt     = (time15 >= prev_t)
                                  ? static_cast<uint16_t>(time15 - prev_t)
                                  : static_cast<uint16_t>(MOD_15 + time15 - prev_t);

                st.acc_time_s += dt;
                st.last_time15 = time15;

                // ====== Arranques acumulados (overflow 15-bit) ======
                uint16_t prev_c = st.last_count15;
                uint16_t dc     = (count15 >= prev_c)
                                  ? static_cast<uint16_t>(count15 - prev_c)
                                  : static_cast<uint16_t>(MOD_15 + count15 - prev_c);

                st.acc_arranques += dc;
                st.last_count15   = count15;
            }

            acc_time_out  = st.acc_time_s;
            arranques_out = st.acc_arranques;
        }

        // ---- Publicaciones ----
        json qual;
        qual["alarms"] = alarms;
        qual["ts"]     = iso8601_utc_now();

        json prod;
        prod["maquina_id"]         = 3;
        prod["turno"]              = shiftNum;
        prod["cantidad_arranques"] = arranques_out;
        prod["tiempo_operacion"]   = static_cast<uint32_t>(acc_time_out);
        prod["timestamp_device"]   = iso8601_utc_now();

        auto t1 = isa95_prefix + std::to_string(line) + "/entrada_secador/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/entrada_secador/production";

        return { make_pub(t1, qual), make_pub(t2, prod) };
    }
};

// Implementación estática
std::mutex EntradaSecadorProcessor::mtx_;
std::unordered_map<int, EntradaSecadorProcessor::ESState>
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

            // ---- Apply masks correctly ----
            uint16_t raw_prod_q15 = mask15(prod_q);
            uint16_t raw_stop_q15 = mask15(stop_q);
            uint16_t raw_prod_t16 = static_cast<uint16_t>(prod_t);        // NO MSB flag → no mask
            uint16_t raw_stop_t15 = mask15(stop_t);

            // ---- First message or shift change ----
            if (!st.initialized || st.shift != shiftNum) {
                st = State();
                st.initialized = true;
                st.shift = shiftNum;

                st.last_raw_prod_q = raw_prod_q15;
                st.last_raw_stop_q = raw_stop_q15;
                st.last_raw_prod_t = raw_prod_t16;
                st.last_raw_stop_t = raw_stop_t15;
            }
            else {
                // ---- cantidadProductos (15-bit modulo) ----
                {
                    uint16_t prev = st.last_raw_prod_q;
                    uint16_t curr = raw_prod_q15;
                    uint16_t diff =
                        (curr >= prev)
                          ? curr - prev
                          : static_cast<uint16_t>(MOD_15 + curr - prev);

                    st.acc_prod_q += diff;
                    st.last_raw_prod_q = curr;
                }

                // ---- paradas (15-bit modulo) ----
                {
                    uint16_t prev = st.last_raw_stop_q;
                    uint16_t curr = raw_stop_q15;
                    uint16_t diff =
                        (curr >= prev)
                          ? curr - prev
                          : static_cast<uint16_t>(MOD_15 + curr - prev);

                    st.acc_stop_q += diff;
                    st.last_raw_stop_q = curr;
                }

                // ---- tiempoProduccion_ds (16-bit normal) ----
                {
                    uint16_t prev = st.last_raw_prod_t;
                    uint16_t curr = raw_prod_t16;
                    uint16_t diff = diff16(curr, prev);
                    st.acc_prod_t_s += diff * 0.1;
                    st.last_raw_prod_t = curr;
                }

                // ---- tiempoParadas_s (15-bit modulo) ----
                {
                    uint16_t prev = st.last_raw_stop_t;
                    uint16_t curr = raw_stop_t15;
                    uint16_t diff =
                        (curr >= prev)
                          ? curr - prev
                          : static_cast<uint16_t>(MOD_15 + curr - prev);

                    st.acc_stop_t_s += diff;
                    st.last_raw_stop_t = curr;
                }
            }

            prod_q_shift   = st.acc_prod_q;
            stop_q_shift   = st.acc_stop_q;
            prod_t_shift_s = st.acc_prod_t_s;
            stop_t_shift_s = st.acc_stop_t_s;
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
        int shift = -1;

        // 15-bit counters (MSB is bank flag)
        uint16_t last_raw_prod_q = 0;
        uint32_t acc_prod_q = 0;

        uint16_t last_raw_stop_q = 0;
        uint32_t acc_stop_q = 0;

        uint16_t last_raw_falla_q = 0;
        uint32_t acc_falla_q = 0;

        // tiempoProduccion_ds → 16 bits reales (NO MSB flag)
        uint16_t last_raw_prod_t = 0;
        double   acc_prod_t_s = 0.0;

        // tiempos con 15 bits
        uint16_t last_raw_stop_t = 0;
        uint32_t acc_stop_t_s = 0;

        uint16_t last_raw_falla_t = 0;
        uint32_t acc_falla_t_s = 0;
    };

    static std::mutex mtx_;
    static std::unordered_map<int, State> states_;

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
        auto sh = current_shift_localtime();
        int shiftNum = (sh == Shift::S1 ? 1 : (sh == Shift::S2 ? 2 : 3));

        int alarms  = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int prod_q  = jsonu::get_opt<int>(msg, "cantidad").value_or(0);
        int prod_t  = jsonu::get_opt<int>(msg, "tiempoProduccion_ds").value_or(0);
        int line    = jsonu::get_opt<int>(msg, "lineID").value_or(0);

        int stop_q  = jsonu::get_opt<int>(msg, "paradas").value_or(0);
        int stop_t  = jsonu::get_opt<int>(msg, "tiempoParadas_s").value_or(0);

        int falla_q = jsonu::get_opt<int>(msg, "fallaHorno").value_or(0);
        int falla_t = jsonu::get_opt<int>(msg, "tiempoFalla_s").value_or(0);

        uint32_t prod_q_out = 0;
        uint32_t stop_q_out = 0;
        uint32_t falla_q_out = 0;

        double   prod_t_out_s = 0.0;
        uint32_t stop_t_out_s = 0;
        uint32_t falla_t_out_s = 0;

        {
            std::lock_guard<std::mutex> lock(mtx_);
            State &st = states_[line];

            // --- 15-bit counters ---
            uint16_t raw_prod_q15  = mask15(prod_q);
            uint16_t raw_stop_q15  = mask15(stop_q);
            uint16_t raw_falla_q15 = mask15(falla_q);

            // --- tiempoProduccion_ds -> 16 bits reales (no mask) ---
            uint16_t raw_prod_t16  = static_cast<uint16_t>(prod_t);

            // --- tiempos con MSB flag ---
            uint16_t raw_stop_t15  = mask15(stop_t);
            uint16_t raw_falla_t15 = mask15(falla_t);

            // --- First message or shift restart ---
            if (!st.initialized || st.shift != shiftNum) {
                st = State();
                st.initialized = true;
                st.shift       = shiftNum;

                st.last_raw_prod_q  = raw_prod_q15;
                st.last_raw_stop_q  = raw_stop_q15;
                st.last_raw_falla_q = raw_falla_q15;

                st.last_raw_prod_t  = raw_prod_t16;
                st.last_raw_stop_t  = raw_stop_t15;
                st.last_raw_falla_t = raw_falla_t15;
            }
            else {
                // cantidad
                st.acc_prod_q += diff16(raw_prod_q15, st.last_raw_prod_q);
                st.last_raw_prod_q = raw_prod_q15;

                // paradas
                st.acc_stop_q += diff16(raw_stop_q15, st.last_raw_stop_q);
                st.last_raw_stop_q = raw_stop_q15;

                // fallaHorno
                st.acc_falla_q += diff16(raw_falla_q15, st.last_raw_falla_q);
                st.last_raw_falla_q = raw_falla_q15;

                // tiempoProduccion_ds (16 bits → seconds)
                st.acc_prod_t_s += diff16(raw_prod_t16, st.last_raw_prod_t) * 0.1;
                st.last_raw_prod_t = raw_prod_t16;

                // tiempoParadas_s
                st.acc_stop_t_s += diff16(raw_stop_t15, st.last_raw_stop_t);
                st.last_raw_stop_t = raw_stop_t15;

                // tiempoFalla_s
                st.acc_falla_t_s += diff16(raw_falla_t15, st.last_raw_falla_t);
                st.last_raw_falla_t = raw_falla_t15;
            }

            prod_q_out   = st.acc_prod_q;
            stop_q_out   = st.acc_stop_q;
            falla_q_out  = st.acc_falla_q;

            prod_t_out_s  = st.acc_prod_t_s;
            stop_t_out_s  = st.acc_stop_t_s;
            falla_t_out_s = st.acc_falla_t_s;
        }

        // --- Build JSON ---
        json qual;
        qual["alarms"] = alarms;
        qual["ts"] = iso8601_utc_now();

        json prod;
        prod["maquina_id"] = 6;
        prod["turno"] = shiftNum;

        prod["cantidad_produccion"] = prod_q_out;
        prod["cantidad_paradas"]    = stop_q_out;
        prod["cantidad_fallas"]     = falla_q_out;

        prod["tiempo_produccion"] = static_cast<uint32_t>(prod_t_out_s);
        prod["tiempo_paradas"]    = stop_t_out_s;
        prod["tiempo_fallas"]     = falla_t_out_s;

        prod["timestamp_device"] = iso8601_utc_now();

        auto t1 = isa95_prefix + std::to_string(line) + "/entrada_horno/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/entrada_horno/production";

        return { make_pub(t1, qual), make_pub(t2, prod) };
    }
};

// ---- STATIC DEFINITIONS ----
std::mutex EntradaHornoProcessor::mtx_;
std::unordered_map<int, EntradaHornoProcessor::State>
    EntradaHornoProcessor::states_;

void EntradaHornoProcessor::reset_states()
{
    std::lock_guard<std::mutex> lk(mtx_);
    states_.clear();
}

class SalidaHornoProcessor : public IMessageProcessor
{
    struct State {
        bool initialized = false;
        int shift = -1;

        // cantidad (15-bit counter)
        uint16_t last_raw_prod_q = 0;
        uint32_t acc_prod_q = 0;

        // cantidad_total (15-bit counter)
        uint16_t last_raw_prod_qtotal = 0;
        uint32_t acc_prod_qtotal = 0;

        // tiempoProduccion_ds (16-bit real word, deciseconds)
        uint16_t last_raw_prod_t = 0;
        double   acc_prod_t_s = 0.0;
    };

    static std::mutex mtx_;
    static std::unordered_map<int, State> states_;

    // MSB mask for 15-bit counters
    static inline uint16_t clean15(int x) {
        return static_cast<uint16_t>(x) & 0x7FFF;
    }

    static uint16_t diff16(uint16_t curr, uint16_t prev) {
        return (curr >= prev)
            ? (curr - prev)
            : static_cast<uint16_t>(curr + (65536 - prev));
    }

public:
static void reset_states();
    std::vector<Publication> process(const json &msg,
                                     const std::string &isa95_prefix) override
    {
        auto sh = current_shift_localtime();
        int shiftNum = (sh == Shift::S1 ? 1 : (sh == Shift::S2 ? 2 : 3));

        int alarms       = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int prod_q_in    = jsonu::get_opt<int>(msg, "cantidad").value_or(0);
        int prod_qt_in   = jsonu::get_opt<int>(msg, "cantidad_total").value_or(0);
        int prod_t_in    = jsonu::get_opt<int>(msg, "tiempoProduccion_ds").value_or(0);
        int line         = jsonu::get_opt<int>(msg, "lineID").value_or(0);

        // --- Clean 15-bit counters ---
        uint16_t raw_prod_q      = clean15(prod_q_in);
        uint16_t raw_prod_qtotal = clean15(prod_qt_in);

        // --- tiempoProduccion_ds is 16-bit real, DO NOT mask ---
        uint16_t raw_prod_t      = static_cast<uint16_t>(prod_t_in);

        uint32_t prod_q_shift      = 0;
        uint32_t prod_qtotal_shift = 0;
        double   prod_t_shift_s    = 0.0;

        {
            std::lock_guard<std::mutex> lock(mtx_);

            State &st = states_[line];

            if (!st.initialized || st.shift != shiftNum) {
                st = State();
                st.initialized = true;
                st.shift = shiftNum;

                st.last_raw_prod_q      = raw_prod_q;
                st.last_raw_prod_qtotal = raw_prod_qtotal;
                st.last_raw_prod_t      = raw_prod_t;
            }
            else {
                // cantidad
                st.acc_prod_q += diff16(raw_prod_q, st.last_raw_prod_q);
                st.last_raw_prod_q = raw_prod_q;

                // cantidad_total
                st.acc_prod_qtotal += diff16(raw_prod_qtotal, st.last_raw_prod_qtotal);
                st.last_raw_prod_qtotal = raw_prod_qtotal;

                // tiempoProduccion_ds → seconds
                st.acc_prod_t_s += diff16(raw_prod_t, st.last_raw_prod_t) * 0.1;
                st.last_raw_prod_t = raw_prod_t;
            }

            prod_q_shift      = st.acc_prod_q;
            prod_qtotal_shift = st.acc_prod_qtotal;
            prod_t_shift_s    = st.acc_prod_t_s;
        }

        json qual;
        qual["alarms"] = alarms;
        qual["ts"] = iso8601_utc_now();

        json prod;
        prod["maquina_id"] = 7;
        prod["turno"]      = shiftNum;

        prod["cantidad_produccion"] = prod_q_shift;
        prod["cantidad_total"]      = prod_qtotal_shift;
        prod["tiempoProduccion_s"]  = static_cast<uint32_t>(prod_t_shift_s);

        prod["timestamp_device"] = iso8601_utc_now();

        auto t1 = isa95_prefix + std::to_string(line) + "/salida_horno/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/salida_horno/production";

        return { make_pub(t1, qual), make_pub(t2, prod) };
    }
};

// ---- STATIC DEFINITIONS ----
std::mutex SalidaHornoProcessor::mtx_;
std::unordered_map<int, SalidaHornoProcessor::State>
    SalidaHornoProcessor::states_;

void SalidaHornoProcessor::reset_states()
{
    std::lock_guard<std::mutex> lk(mtx_);
    states_.clear();
}



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
