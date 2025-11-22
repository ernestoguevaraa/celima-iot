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

class PrensaHidraulica1Processor : public IMessageProcessor
{
    // --- Per-PLC state ---
    struct PH1State {
        bool initialized = false;
        int shift = -1;

        // Production time (WORD overflow-safe)
        uint16_t last_raw_prod_time = 0;
        double   acc_prod_time_s = 0.0;    // seconds

        // Pisadas (WORD overflow-safe)
        uint16_t last_raw_count = 0;
        uint32_t acc_count = 0;
    };

    static std::mutex mtx_;
    static std::unordered_map<int, PH1State> states_;

public:
    std::vector<Publication> process(const json &msg, const std::string &isa95_prefix) override
    {
        // -------- Current shift --------
        auto sh = current_shift_localtime();
        int shiftNum = (sh == Shift::S1 ? 1 : sh == Shift::S2 ? 2 : 3);

        // -------- Extract fields --------
        int line            = jsonu::get_opt<int>(msg, "lineID").value_or(0);
        int alarms          = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int raw_count       = jsonu::get_opt<int>(msg, "cantidadProductos").value_or(0);
        int raw_prod_time   = jsonu::get_opt<int>(msg, "tiempoProduccion_ds").value_or(0);  // tenths of sec
        int stop_q          = jsonu::get_opt<int>(msg, "paradas").value_or(0);
        int stop_t          = jsonu::get_opt<int>(msg, "tiempoParadas_s").value_or(0);

        double production_time_s = 0.0;
        uint32_t prod_count_shift = 0;
        double pisadas_min = 0.0;

        {
            std::lock_guard<std::mutex> lock(mtx_);

            // Per-line state reference
            PH1State &st = states_[line]; 

            uint16_t time_raw  = static_cast<uint16_t>(raw_prod_time);
            uint16_t count_raw = static_cast<uint16_t>(raw_count);

            // -------- Shift change / first message --------
            if (!st.initialized || st.shift != shiftNum) {
                st.initialized = true;
                st.shift = shiftNum;

                st.last_raw_prod_time = time_raw;
                st.acc_prod_time_s = 0.0;

                st.last_raw_count = count_raw;
                st.acc_count = 0;
            }
            else {
                // -------- Production time accumulator --------
                uint16_t prev_t = st.last_raw_prod_time;
                uint16_t diff_t = (time_raw >= prev_t)
                    ? (time_raw - prev_t)
                    : static_cast<uint16_t>(time_raw + (65536 - prev_t));

                st.acc_prod_time_s += diff_t * 0.1;  // deciseconds → seconds
                st.last_raw_prod_time = time_raw;

                // -------- Pisadas accumulator (WORD overflow-safe) --------
                uint16_t prev_c = st.last_raw_count;
                uint16_t diff_c = (count_raw >= prev_c)
                    ? (count_raw - prev_c)
                    : static_cast<uint16_t>(count_raw + (65536 - prev_c));

                st.acc_count += diff_c;
                st.last_raw_count = count_raw;
            }

            production_time_s = st.acc_prod_time_s;
            prod_count_shift  = st.acc_count;

            // -------- NEW: cantidadPisadas_min --------
            if (production_time_s > 0.1) {
                double minutes_prod = production_time_s / 60.0;
                pisadas_min = prod_count_shift / minutes_prod;
            }
            else {
                pisadas_min = 0.0;
            }
        }

        // -------- Build publications --------
        json qual;
        qual["alarms"] = alarms;
        qual["ts"] = iso8601_utc_now();

        json prod;
        prod["maquina_id"] = 1;
        prod["turno"] = shiftNum;

        // Raw PLC values
        prod["cantidadProductos_raw"] = raw_count;
        prod["tiempoProduccion_ds_raw"] = raw_prod_time;

        // Shift accumulators
        prod["cantidadProductos_shift"] = prod_count_shift;
        prod["tiempoProduccion_s"] = production_time_s;

        // NEW rate metric
        prod["cantidadPisadas_min"] = pisadas_min;

        prod["paradas"] = stop_q;
        prod["tiempoParadas_s"] = stop_t;
        prod["timestamp_device"] = iso8601_utc_now();

        // Topics
        auto t1 = isa95_prefix + std::to_string(line) + "/prensa_hidraulica1/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/prensa_hidraulica1/production";

        return {make_pub(t1, qual), make_pub(t2, prod)};
    }
};

// ---- STATIC DEFINITIONS ----
std::mutex PrensaHidraulica1Processor::mtx_;
std::unordered_map<int, PrensaHidraulica1Processor::PH1State>
    PrensaHidraulica1Processor::states_;

class PrensaHidraulica2Processor : public IMessageProcessor
{
    // --- Per-PLC state ---
    struct PH2State {
        bool initialized = false;
        int shift = -1;

        // Production time (WORD overflow-safe)
        uint16_t last_raw_prod_time = 0;
        double   acc_prod_time_s = 0.0;    // seconds

        // Pisadas (WORD overflow-safe)
        uint16_t last_raw_count = 0;
        uint32_t acc_count = 0;
    };

    static std::mutex mtx_;
    static std::unordered_map<int, PH2State> states_;

public:
    std::vector<Publication> process(const json &msg, const std::string &isa95_prefix) override
    {
        // -------- Current shift --------
        auto sh = current_shift_localtime();
        int shiftNum = (sh == Shift::S1 ? 1 : sh == Shift::S2 ? 2 : 3);

        // -------- Extract fields --------
        int line            = jsonu::get_opt<int>(msg, "lineID").value_or(0);
        int alarms          = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int raw_count       = jsonu::get_opt<int>(msg, "cantidadProductos").value_or(0);
        int raw_prod_time   = jsonu::get_opt<int>(msg, "tiempoProduccion_ds").value_or(0);  // tenths of sec
        int stop_q          = jsonu::get_opt<int>(msg, "paradas").value_or(0);
        int stop_t          = jsonu::get_opt<int>(msg, "tiempoParadas_s").value_or(0);

        double production_time_s = 0.0;
        uint32_t prod_count_shift = 0;
        double pisadas_min = 0.0;

        {
            std::lock_guard<std::mutex> lock(mtx_);

            // Per-line state reference
            PH2State &st = states_[line]; 

            uint16_t time_raw  = static_cast<uint16_t>(raw_prod_time);
            uint16_t count_raw = static_cast<uint16_t>(raw_count);

            // -------- Shift change / first message --------
            if (!st.initialized || st.shift != shiftNum) {
                st.initialized = true;
                st.shift = shiftNum;

                st.last_raw_prod_time = time_raw;
                st.acc_prod_time_s = 0.0;

                st.last_raw_count = count_raw;
                st.acc_count = 0;
            }
            else {
                // -------- Production time accumulator --------
                uint16_t prev_t = st.last_raw_prod_time;
                uint16_t diff_t = (time_raw >= prev_t)
                    ? (time_raw - prev_t)
                    : static_cast<uint16_t>(time_raw + (65536 - prev_t));

                st.acc_prod_time_s += diff_t * 0.1;  // deciseconds → seconds
                st.last_raw_prod_time = time_raw;

                // -------- Pisadas accumulator (WORD overflow-safe) --------
                uint16_t prev_c = st.last_raw_count;
                uint16_t diff_c = (count_raw >= prev_c)
                    ? (count_raw - prev_c)
                    : static_cast<uint16_t>(count_raw + (65536 - prev_c));

                st.acc_count += diff_c;
                st.last_raw_count = count_raw;
            }

            production_time_s = st.acc_prod_time_s;
            prod_count_shift  = st.acc_count;

            // -------- NEW: cantidadPisadas_min --------
            if (production_time_s > 0.1) {
                double minutes_prod = production_time_s / 60.0;
                pisadas_min = prod_count_shift / minutes_prod;
            }
            else {
                pisadas_min = 0.0;
            }
        }

        // -------- Build publications --------
        json qual;
        qual["alarms"] = alarms;
        qual["ts"] = iso8601_utc_now();

        json prod;
        prod["maquina_id"] = 1;
        prod["turno"] = shiftNum;

        // Raw PLC values
        prod["cantidadProductos_raw"] = raw_count;
        prod["tiempoProduccion_ds_raw"] = raw_prod_time;

        // Shift accumulators
        prod["cantidadProductos_shift"] = prod_count_shift;
        prod["tiempoProduccion_s"] = production_time_s;

        // NEW rate metric
        prod["cantidadPisadas_min"] = pisadas_min;

        prod["paradas"] = stop_q;
        prod["tiempoParadas_s"] = stop_t;
        prod["timestamp_device"] = iso8601_utc_now();

        // Topics
        auto t1 = isa95_prefix + std::to_string(line) + "/prensa_hidraulica2/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/prensa_hidraulica2/production";

        return {make_pub(t1, qual), make_pub(t2, prod)};
    }
};

// ---- STATIC DEFINITIONS ----
std::mutex PrensaHidraulica2Processor::mtx_;
std::unordered_map<int, PrensaHidraulica2Processor::PH2State>
    PrensaHidraulica2Processor::states_;

class EntradaSecadorProcessor : public IMessageProcessor
{
    // Per-PLC state
    struct State {
        bool initialized = false;
        int shift = -1;

        // Event counter (arranques): WORD overflow-safe
        uint16_t last_raw_arr = 0;
        uint32_t acc_arranques = 0;

        // Operation time: WORD overflow-safe (seconds)
        uint16_t last_raw_oper = 0;
        uint32_t acc_oper_s = 0;
    };

    static std::mutex mtx_;
    static std::unordered_map<int, State> states_;

public:
    std::vector<Publication> process(const json &msg, const std::string &isa95_prefix) override
    {
        // ---- Current shift ----
        auto sh = current_shift_localtime();
        int shiftNum = (sh == Shift::S1 ? 1 : sh == Shift::S2 ? 2 : 3);

        // ---- Read PLC fields ----
        int line   = jsonu::get_opt<int>(msg, "lineID").value_or(0);
        int alarms = jsonu::get_opt<int>(msg, "alarms").value_or(0);

        int raw_arr = jsonu::get_opt<int>(msg, "arranques").value_or(0);              // WORD counter
        int raw_op  = jsonu::get_opt<int>(msg, "tiempoOperacion_s").value_or(0);      // WORD seconds

        uint32_t arranques_shift = 0;
        uint32_t tiempo_oper_s   = 0;

        {
            std::lock_guard<std::mutex> lock(mtx_);

            State &st = states_[line];

            uint16_t arr_raw = static_cast<uint16_t>(raw_arr);
            uint16_t op_raw  = static_cast<uint16_t>(raw_op);

            // ---- Shift reset or first message ----
            if (!st.initialized || st.shift != shiftNum) {
                st.initialized = true;
                st.shift = shiftNum;

                st.last_raw_arr = arr_raw;
                st.acc_arranques = 0;

                st.last_raw_oper = op_raw;
                st.acc_oper_s = 0;
            }
            else {
                // ---- Overflow-safe arranques increment ----
                uint16_t prev_a = st.last_raw_arr;
                uint16_t diff_a =
                    (arr_raw >= prev_a) ? (arr_raw - prev_a)
                                        : static_cast<uint16_t>(arr_raw + (65536 - prev_a));

                st.acc_arranques += diff_a;
                st.last_raw_arr = arr_raw;

                // ---- Overflow-safe operation time increment ----
                uint16_t prev_o = st.last_raw_oper;
                uint16_t diff_o =
                    (op_raw >= prev_o) ? (op_raw - prev_o)
                                       : static_cast<uint16_t>(op_raw + (65536 - prev_o));

                st.acc_oper_s += diff_o;
                st.last_raw_oper = op_raw;
            }

            arranques_shift = st.acc_arranques;
            tiempo_oper_s   = st.acc_oper_s;
        }

        // ---- Build output JSON ----
        json qual;
        qual["alarms"] = alarms;
        qual["ts"] = iso8601_utc_now();

        json prod;
        prod["maquina_id"] = 3;
        prod["turno"] = shiftNum;

        // raw PLC values
        prod["arranques_raw"] = raw_arr;
        prod["tiempoOperacion_s_raw"] = raw_op;

        // shift-accumulated values
        prod["arranques_shift"] = arranques_shift;
        prod["tiempoOperacion_shift_s"] = tiempo_oper_s;

        prod["timestamp_device"] = iso8601_utc_now();

        // ---- Topics ----
        auto t1 = isa95_prefix + std::to_string(line) + "/entrada_secador/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/entrada_secador/production";

        return {make_pub(t1, qual), make_pub(t2, prod)};
    }
};

// ---- STATIC DEFINITIONS ----
std::mutex EntradaSecadorProcessor::mtx_;
std::unordered_map<int, EntradaSecadorProcessor::State>
    EntradaSecadorProcessor::states_;


class SalidaSecadorProcessor : public IMessageProcessor
{
    struct State {
        bool initialized = false;
        int shift = -1;

        // cantidadProductos (WORD counter)
        uint16_t last_raw_prod_q = 0;
        uint32_t acc_prod_q = 0;

        // paradas (WORD counter)
        uint16_t last_raw_stop_q = 0;
        uint32_t acc_stop_q = 0;

        // tiempoProduccion_ds (WORD, deciseconds)
        uint16_t last_raw_prod_t = 0;
        double   acc_prod_t_s = 0.0;

        // tiempoParadas_s (WORD, seconds)
        uint16_t last_raw_stop_t = 0;
        uint32_t acc_stop_t_s = 0;
    };

    static std::mutex mtx_;
    static std::unordered_map<int, State> states_;

public:
    std::vector<Publication> process(const json &msg,
                                     const std::string &isa95_prefix) override
    {
        // ---- Current shift ----
        auto sh = current_shift_localtime();
        int shiftNum    = (sh == Shift::S1 ? 1 : sh == Shift::S2 ? 2 : 3);

        // ---- Read fields ----
        int alarms   = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int prod_q   = jsonu::get_opt<int>(msg, "cantidadProductos").value_or(0);
        int prod_t   = jsonu::get_opt<int>(msg, "tiempoProduccion_ds").value_or(0);
        int line     = jsonu::get_opt<int>(msg, "lineID").value_or(0);
        int stop_q   = jsonu::get_opt<int>(msg, "paradas").value_or(0);
        int stop_t   = jsonu::get_opt<int>(msg, "tiempoParadas_s").value_or(0);

        // Outputs
        uint32_t prod_q_shift   = 0;
        double   prod_t_shift_s = 0.0;
        uint32_t stop_q_shift   = 0;
        uint32_t stop_t_shift_s = 0;

        {
            std::lock_guard<std::mutex> lock(mtx_);

            State &st = states_[line];

            uint16_t raw_prod_q = static_cast<uint16_t>(prod_q);
            uint16_t raw_stop_q = static_cast<uint16_t>(stop_q);
            uint16_t raw_prod_t = static_cast<uint16_t>(prod_t);
            uint16_t raw_stop_t = static_cast<uint16_t>(stop_t);

            // ---- Initialize or shift change ----
            if (!st.initialized || st.shift != shiftNum) {
                st.initialized = true;
                st.shift = shiftNum;

                st.last_raw_prod_q = raw_prod_q;
                st.acc_prod_q = 0;

                st.last_raw_stop_q = raw_stop_q;
                st.acc_stop_q = 0;

                st.last_raw_prod_t = raw_prod_t;
                st.acc_prod_t_s = 0.0;

                st.last_raw_stop_t = raw_stop_t;
                st.acc_stop_t_s = 0;
            }
            else {
                // ---- cantidadProductos overflow-safe counter ----
                uint16_t prev_pq = st.last_raw_prod_q;
                uint16_t diff_pq = (raw_prod_q >= prev_pq)
                    ? (raw_prod_q - prev_pq)
                    : static_cast<uint16_t>(raw_prod_q + (65536 - prev_pq));

                st.acc_prod_q += diff_pq;
                st.last_raw_prod_q = raw_prod_q;

                // ---- paradas overflow-safe counter ----
                uint16_t prev_sq = st.last_raw_stop_q;
                uint16_t diff_sq = (raw_stop_q >= prev_sq)
                    ? (raw_stop_q - prev_sq)
                    : static_cast<uint16_t>(raw_stop_q + (65536 - prev_sq));

                st.acc_stop_q += diff_sq;
                st.last_raw_stop_q = raw_stop_q;

                // ---- tiempoProduccion_ds overflow-safe ----
                uint16_t prev_pt = st.last_raw_prod_t;
                uint16_t diff_pt = (raw_prod_t >= prev_pt)
                    ? (raw_prod_t - prev_pt)
                    : static_cast<uint16_t>(raw_prod_t + (65536 - prev_pt));

                st.acc_prod_t_s += diff_pt * 0.1;  // ds → seconds
                st.last_raw_prod_t = raw_prod_t;

                // ---- tiempoParadas_s overflow-safe ----
                uint16_t prev_st = st.last_raw_stop_t;
                uint16_t diff_st = (raw_stop_t >= prev_st)
                    ? (raw_stop_t - prev_st)
                    : static_cast<uint16_t>(raw_stop_t + (65536 - prev_st));

                st.acc_stop_t_s += diff_st;
                st.last_raw_stop_t = raw_stop_t;
            }

            // ---- Final accumulated values ----
            prod_q_shift   = st.acc_prod_q;
            prod_t_shift_s = st.acc_prod_t_s;
            stop_q_shift   = st.acc_stop_q;
            stop_t_shift_s = st.acc_stop_t_s;
        }

        // ---- Build MQ payloads ----
        json qual;
        qual["alarms"] = alarms;
        qual["ts"] = iso8601_utc_now();

        json prod;
        prod["maquina_id"] = 4;
        prod["turno"] = shiftNum;

        // raw PLC values
        prod["cantidadProductos_raw"] = prod_q;
        prod["tiempoProduccion_ds_raw"] = prod_t;
        prod["paradas_raw"] = stop_q;
        prod["tiempoParadas_s_raw"] = stop_t;

        // shift accumulated values
        prod["cantidadProductos_shift"] = prod_q_shift;
        prod["tiempoProduccion_shift_s"] = prod_t_shift_s;
        prod["paradas_shift"] = stop_q_shift;
        prod["tiempoParadas_shift_s"] = stop_t_shift_s;

        prod["timestamp_device"] = iso8601_utc_now();

        // Topics
        auto t1 = isa95_prefix + std::to_string(line) + "/salida_secador/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/salida_secador/production";

        return {make_pub(t1, qual), make_pub(t2, prod)};
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

        // cantidadProductos (WORD event counter)
        uint16_t last_raw_prod_q = 0;
        uint32_t acc_prod_q = 0;

        // paradas (WORD event counter)
        uint16_t last_raw_stop_q = 0;
        uint32_t acc_stop_q = 0;

        // tiempoProduccion_ds (WORD, tenths of seconds)
        uint16_t last_raw_prod_t = 0;
        double   acc_prod_t_s = 0.0;

        // tiempoParadas_s (WORD, seconds)
        uint16_t last_raw_stop_t = 0;
        uint32_t acc_stop_t_s = 0;
    };

    static std::mutex mtx_;
    static std::unordered_map<int, State> states_;

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

        // ---- Extract PLC fields ----
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

            uint16_t raw_prod_q = static_cast<uint16_t>(prod_q);
            uint16_t raw_stop_q = static_cast<uint16_t>(stop_q);

            uint16_t raw_prod_t = static_cast<uint16_t>(prod_t);
            uint16_t raw_stop_t = static_cast<uint16_t>(stop_t);

            // ---- First message for this shift OR shift changed ----
            if (!st.initialized || st.shift != shiftNum) {
                st = State();  // reset structure
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

                // ---- tiempoProduccion_ds → seconds ----
                st.acc_prod_t_s += diff16(raw_prod_t, st.last_raw_prod_t) * 0.1;
                st.last_raw_prod_t = raw_prod_t;

                // ---- tiempoParadas_s ----
                st.acc_stop_t_s += diff16(raw_stop_t, st.last_raw_stop_t);
                st.last_raw_stop_t = raw_stop_t;
            }

            // Copy accumulated values
            prod_q_shift = st.acc_prod_q;
            stop_q_shift = st.acc_stop_q;

            prod_t_shift_s = st.acc_prod_t_s;
            stop_t_shift_s = st.acc_stop_t_s;
        }

        // ---- Build JSON ----
        json qual;
        qual["alarms"] = alarms;
        qual["ts"] = iso8601_utc_now();

        json prod;
        prod["maquina_id"] = 5;
        prod["turno"] = shiftNum;

        // Raw values from PLC
        prod["cantidadProductos_raw"] = prod_q;
        prod["tiempoProduccion_ds_raw"] = prod_t;
        prod["paradas_raw"] = stop_q;
        prod["tiempoParadas_s_raw"] = stop_t;

        // Shift-accumulated values
        prod["cantidadProductos_shift"] = prod_q_shift;
        prod["tiempoProduccion_shift_s"] = prod_t_shift_s;
        prod["paradas_shift"] = stop_q_shift;
        prod["tiempoParadas_shift_s"] = stop_t_shift_s;

        prod["timestamp_device"] = iso8601_utc_now();

        // ---- Topics ----
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
public:
    std::vector<Publication> process(const json &msg, const std::string &isa95_prefix) override
    {
        // Interpret fields
        auto s = current_shift_localtime();
        int shiftNum = (s == Shift::S1 ? 1 : s == Shift::S2 ? 2
                                                            : 3);

        int alarms = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int prod_q = jsonu::get_opt<int>(msg, "cantidad").value_or(0);
        //int prod_t = jsonu::get_opt<int>(msg, "tiempoProduccion_ds").value_or(0);
        int line = jsonu::get_opt<int>(msg, "lineID").value_or(0);
        int stop_q = jsonu::get_opt<int>(msg, "paradas").value_or(0);
        int stop_t = jsonu::get_opt<int>(msg, "tiempoParadas_s").value_or(0);
        int falla_q = jsonu::get_opt<int>(msg, "fallaHorno").value_or(0);
        int falla_t = jsonu::get_opt<int>(msg, "tiempoFalla_s").value_or(0);


        json qual;
        qual["alarms"] = alarms;
        qual["ts"] = iso8601_utc_now();

        json prod;
        prod["maquina_id"] = 6;
        prod["cantidad_produccion"] = prod_q;
        prod["turno"] = shiftNum;
        prod["cantidad_paradas"] = stop_q;
        prod["tiempo_paradas"] = stop_t;
        prod["timestamp_device"] = iso8601_utc_now();
        prod["cantidad_fallas"] = falla_q;
        prod["tiempo_fallas"] = falla_t;

        auto t1 = isa95_prefix + std::to_string(line) + "/entrada_horno/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/entrada_horno/production";

        return {make_pub(t1, qual), make_pub(t2, prod)};
    }
};

class SalidaHornoProcessor : public IMessageProcessor
{
public:
    std::vector<Publication> process(const json &msg, const std::string &isa95_prefix) override
    {
        // Interpret fields
        auto s = current_shift_localtime();
        int shiftNum = (s == Shift::S1 ? 1 : s == Shift::S2 ? 2
                                                            : 3);

        int alarms = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int prod_q = jsonu::get_opt<int>(msg, "cantidad").value_or(0);
        int prod_qtotal = jsonu::get_opt<int>(msg, "cantidad_total").value_or(0);
        //int prod_t = jsonu::get_opt<int>(msg, "tiempoProduccion_ds").value_or(0);
        int line = jsonu::get_opt<int>(msg, "lineID").value_or(0);


        json qual;
        qual["alarms"] = alarms;
        qual["ts"] = iso8601_utc_now();

        json prod;
        prod["maquina_id"] = 7;
        prod["cantidad_produccion"] = prod_q;
        prod["turno"] = shiftNum;
        prod["cantidad_total"] = prod_qtotal;

        prod["timestamp_device"] = iso8601_utc_now();


        auto t1 = isa95_prefix + std::to_string(line) + "/salida_horno/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/salida_horno/production";

        return {make_pub(t1, qual), make_pub(t2, prod)};
    }
};

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
