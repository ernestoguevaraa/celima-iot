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
    // ---- STATIC ACCUMULATORS (persist across messages) ----
    static std::mutex mtx_;

    static bool     initialized_;
    static int      current_shift_;

    // tiempoProduccion_ds accumulator
    static uint16_t last_raw_ds_;
    static double   accumulated_prod_time_s_;

    // cantidadProductos accumulator (word overflow safe)
    static uint16_t last_raw_count_;
    static uint32_t accumulated_count_;   // grows per shift

public:
    std::vector<Publication> process(const json &msg, const std::string &isa95_prefix) override
    {
        // Current shift
        auto shift = current_shift_localtime();
        int shiftNum = (shift == Shift::S1 ? 1 : shift == Shift::S2 ? 2 : 3);

        // --- INPUT FIELDS ---
        int alarms          = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int raw_count       = jsonu::get_opt<int>(msg, "cantidadProductos").value_or(0);      // WORD
        int raw_prod_time   = jsonu::get_opt<int>(msg, "tiempoProduccion_ds").value_or(0);    // WORD (0.1s)
        int stop_q          = jsonu::get_opt<int>(msg, "paradas").value_or(0);
        int stop_t          = jsonu::get_opt<int>(msg, "tiempoParadas_s").value_or(0);
        int line            = jsonu::get_opt<int>(msg, "lineID").value_or(0);

        double production_time_s = 0.0;
        uint32_t prod_count_shift = 0;

        {
            std::lock_guard<std::mutex> lock(mtx_);

            uint16_t count_raw  = static_cast<uint16_t>(raw_count);
            uint16_t time_raw   = static_cast<uint16_t>(raw_prod_time);

            // ---- SHIFT CHANGE HANDLING ----
            if (!initialized_ || shiftNum != current_shift_) {
                initialized_ = true;
                current_shift_ = shiftNum;

                // reset per-shift accumulators
                last_raw_ds_      = time_raw;
                accumulated_prod_time_s_ = 0.0;

                last_raw_count_   = count_raw;
                accumulated_count_ = 0;
            }
            else {
                // ---- 1) PRODUCTION TIME (WORD w/ overflow) ----
                {
                    uint16_t prev = last_raw_ds_;
                    uint16_t diff = (time_raw >= prev)
                        ? (time_raw - prev)
                        : static_cast<uint16_t>(time_raw + (65536 - prev));

                    accumulated_prod_time_s_ += diff * 0.1; // ds → seconds
                    last_raw_ds_ = time_raw;
                }

                // ---- 2) PRODUCT COUNT (WORD w/ overflow) ----
                {
                    uint16_t prev = last_raw_count_;
                    uint16_t diff = (count_raw >= prev)
                        ? (count_raw - prev)
                        : static_cast<uint16_t>(count_raw + (65536 - prev));

                    accumulated_count_ += diff;
                    last_raw_count_ = count_raw;
                }
            }

            production_time_s = accumulated_prod_time_s_;
            prod_count_shift  = accumulated_count_;
        }

        // ------------ BUILD PAYLOADS -------------
        json qual;
        qual["alarms"] = alarms;
        qual["timestamp_device"] = iso8601_utc_now();

        json prod;
        prod["maquina_id"] = 1;
        prod["turno"] = shiftNum;

        // original raw PLC values (optional, but you had them)
        prod["tiempoParadas_s"]   = stop_t;
        prod["paradas"]           = stop_q;

        // ---- NEW SHIFT-ACCUMULATED VALUES ----
        prod["cantidadPisadas"] = prod_count_shift;
        prod["cantidadProductos"] = prod_count_shift * PIEZAS_PISADA;
        prod["tiempoProduccion_s"]      = production_time_s;

        prod["timestamp_device"] = iso8601_utc_now();

        // Topics
        auto t1 = isa95_prefix + std::to_string(line) + "/prensa_hidraulica1/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/prensa_hidraulica1/production";

        return {make_pub(t1, qual), make_pub(t2, prod)};
    }
};

// ---- STATIC DEFINITIONS ----
std::mutex PrensaHidraulica1Processor::mtx_;
bool      PrensaHidraulica1Processor::initialized_            = false;
int       PrensaHidraulica1Processor::current_shift_          = -1;
uint16_t  PrensaHidraulica1Processor::last_raw_ds_            = 0;
double    PrensaHidraulica1Processor::accumulated_prod_time_s_ = 0.0;
uint16_t  PrensaHidraulica1Processor::last_raw_count_         = 0;
uint32_t  PrensaHidraulica1Processor::accumulated_count_      = 0;


class PrensaHidraulica2Processor : public IMessageProcessor
{
    // ---- STATIC ACCUMULATORS (persist across messages) ----
    static std::mutex mtx_;

    static bool     initialized_;
    static int      current_shift_;

    // tiempoProduccion_ds accumulator
    static uint16_t last_raw_ds_;
    static double   accumulated_prod_time_s_;

    // cantidadProductos accumulator (word overflow safe)
    static uint16_t last_raw_count_;
    static uint32_t accumulated_count_;   // grows per shift

public:
    std::vector<Publication> process(const json &msg, const std::string &isa95_prefix) override
    {
        // Current shift
        auto shift = current_shift_localtime();
        int shiftNum = (shift == Shift::S1 ? 1 : shift == Shift::S2 ? 2 : 3);

        // --- INPUT FIELDS ---
        int alarms          = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int raw_count       = jsonu::get_opt<int>(msg, "cantidadProductos").value_or(0);      // WORD
        int raw_prod_time   = jsonu::get_opt<int>(msg, "tiempoProduccion_ds").value_or(0);    // WORD (0.1s)
        int stop_q          = jsonu::get_opt<int>(msg, "paradas").value_or(0);
        int stop_t          = jsonu::get_opt<int>(msg, "tiempoParadas_s").value_or(0);
        int line            = jsonu::get_opt<int>(msg, "lineID").value_or(0);

        double production_time_s = 0.0;
        uint32_t prod_count_shift = 0;

        {
            std::lock_guard<std::mutex> lock(mtx_);

            uint16_t count_raw  = static_cast<uint16_t>(raw_count);
            uint16_t time_raw   = static_cast<uint16_t>(raw_prod_time);

            // ---- SHIFT CHANGE HANDLING ----
            if (!initialized_ || shiftNum != current_shift_) {
                initialized_ = true;
                current_shift_ = shiftNum;

                // reset per-shift accumulators
                last_raw_ds_      = time_raw;
                accumulated_prod_time_s_ = 0.0;

                last_raw_count_   = count_raw;
                accumulated_count_ = 0;
            }
            else {
                // ---- 1) PRODUCTION TIME (WORD w/ overflow) ----
                {
                    uint16_t prev = last_raw_ds_;
                    uint16_t diff = (time_raw >= prev)
                        ? (time_raw - prev)
                        : static_cast<uint16_t>(time_raw + (65536 - prev));

                    accumulated_prod_time_s_ += diff * 0.1; // ds → seconds
                    last_raw_ds_ = time_raw;
                }

                // ---- 2) PRODUCT COUNT (WORD w/ overflow) ----
                {
                    uint16_t prev = last_raw_count_;
                    uint16_t diff = (count_raw >= prev)
                        ? (count_raw - prev)
                        : static_cast<uint16_t>(count_raw + (65536 - prev));

                    accumulated_count_ += diff;
                    last_raw_count_ = count_raw;
                }
            }

            production_time_s = accumulated_prod_time_s_;
            prod_count_shift  = accumulated_count_;
        }

        // ------------ BUILD PAYLOADS -------------
        json qual;
        qual["alarms"] = alarms;
        qual["timestamp_device"] = iso8601_utc_now();

        json prod;
        prod["maquina_id"] = 1;
        prod["turno"] = shiftNum;

        // original raw PLC values (optional, but you had them)
        prod["tiempoParadas_s"]   = stop_t;
        prod["paradas"]           = stop_q;

        // ---- NEW SHIFT-ACCUMULATED VALUES ----
        prod["cantidadPisadas"] = prod_count_shift;
        prod["cantidadProductos"] = prod_count_shift * PIEZAS_PISADA;
        prod["tiempoProduccion_s"]      = production_time_s;

        prod["timestamp_device"] = iso8601_utc_now();

        // Topics
        auto t1 = isa95_prefix + std::to_string(line) + "/prensa_hidraulica2/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/prensa_hidraulica2/production";

        return {make_pub(t1, qual), make_pub(t2, prod)};
    }
};

// ---- STATIC DEFINITIONS ----
std::mutex PrensaHidraulica2Processor::mtx_;
bool      PrensaHidraulica2Processor::initialized_            = false;
int       PrensaHidraulica2Processor::current_shift_          = -1;
uint16_t  PrensaHidraulica2Processor::last_raw_ds_            = 0;
double    PrensaHidraulica2Processor::accumulated_prod_time_s_ = 0.0;
uint16_t  PrensaHidraulica2Processor::last_raw_count_         = 0;
uint32_t  PrensaHidraulica2Processor::accumulated_count_      = 0;


class EntradaSecadorProcessor : public IMessageProcessor
{
public:
    std::vector<Publication> process(const json &msg, const std::string &isa95_prefix) override
    {
        // Interpret fields
        auto s = current_shift_localtime();
        int shiftNum = (s == Shift::S1 ? 1 : s == Shift::S2 ? 2
                                                            : 3);

        int alarms = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int prod_s = jsonu::get_opt<int>(msg, "arranques").value_or(0);
        int prod_t = jsonu::get_opt<int>(msg, "tiempoOperacion_s").value_or(0);
        int line = jsonu::get_opt<int>(msg, "lineID").value_or(0);


        json qual;
        qual["alarms"] = alarms;
        qual["ts"] = iso8601_utc_now();

        json prod;
        prod["maquina_id"] = 3;
        prod["cantidad_arranques"] = prod_s;
        prod["turno"] = shiftNum;
        prod["tiempo_operacion"] = prod_t;
        prod["timestamp_device"] = iso8601_utc_now();

        auto t1 = isa95_prefix + std::to_string(line) + "/entrada_secador/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/entrada_secador/production";

        return {make_pub(t1, qual), make_pub(t2, prod)};
    }
};

class SalidaSecadorProcessor : public IMessageProcessor
{
public:
    std::vector<Publication> process(const json &msg, const std::string &isa95_prefix) override
    {
        // Interpret fields
        auto s = current_shift_localtime();
        int shiftNum = (s == Shift::S1 ? 1 : s == Shift::S2 ? 2
                                                            : 3);

        int alarms = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int prod_q = jsonu::get_opt<int>(msg, "cantidadProductos").value_or(0);
        //int prod_t = jsonu::get_opt<int>(msg, "tiempoProduccion_ds").value_or(0);
        int line = jsonu::get_opt<int>(msg, "lineID").value_or(0);
        int stop_q = jsonu::get_opt<int>(msg, "paradas").value_or(0);
        int stop_t = jsonu::get_opt<int>(msg, "tiempoParadas_s").value_or(0);


        json qual;
        qual["alarms"] = alarms;
        qual["ts"] = iso8601_utc_now();

        json prod;
        prod["maquina_id"] = 4;
        prod["cantidad_produccion"] = prod_q;
        prod["turno"] = shiftNum;
        prod["cantidad_paradas"] = stop_q;
        prod["tiempo_paradas"] = stop_t;
        prod["timestamp_device"] = iso8601_utc_now();

        auto t1 = isa95_prefix + std::to_string(line) + "/salida_secador/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/salida_secador/production";

        return {make_pub(t1, qual), make_pub(t2, prod)};
    }
};

class EsmalteProcessor : public IMessageProcessor
{
public:
    std::vector<Publication> process(const json &msg, const std::string &isa95_prefix) override
    {
        // Interpret fields
        auto s = current_shift_localtime();
        int shiftNum = (s == Shift::S1 ? 1 : s == Shift::S2 ? 2
                                                            : 3);

        int alarms = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int prod_q = jsonu::get_opt<int>(msg, "cantidadProductos").value_or(0);
        //int prod_t = jsonu::get_opt<int>(msg, "tiempoProduccion_ds").value_or(0);
        int line = jsonu::get_opt<int>(msg, "lineID").value_or(0);
        int stop_q = jsonu::get_opt<int>(msg, "paradas").value_or(0);
        int stop_t = jsonu::get_opt<int>(msg, "tiempoParadas_s").value_or(0);


        json qual;
        qual["alarms"] = alarms;
        qual["ts"] = iso8601_utc_now();

        json prod;
        prod["maquina_id"] = 5;
        prod["cantidad_produccion"] = prod_q;
        prod["turno"] = shiftNum;
        prod["cantidad_paradas"] = stop_q;
        prod["tiempo_paradas"] = stop_t;
        prod["timestamp_device"] = iso8601_utc_now();

        auto t1 = isa95_prefix + std::to_string(line) + "/esmalte/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/esmalte/production";

        return {make_pub(t1, qual), make_pub(t2, prod)};
    }
};

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
