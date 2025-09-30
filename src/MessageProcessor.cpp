#include "MessageProcessor.hpp"
#include "JsonUtils.hpp"
#include "Shift.hpp"
#include <memory>
#include <sstream>
using json = nlohmann::json;

/**
 * Two ISA-95-style example topics. Adjust/extend later:
 *  1) <prefix>/production/line/quantity
 *  2) <prefix>/quality/alarms
 * Where <prefix> can be like: enterprise/site/area/line1
 */

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

/** Example specialized processor: Calidad (deviceType 8) */
class CalidadProcessor : public IMessageProcessor
{
public:
    std::vector<Publication> process(const json &msg, const std::string &isa95_prefix) override
    {
        // Interpret some likely quality-related signals
        int alarms = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int qty = jsonu::get_opt<int>(msg, "cantidad").value_or(0);

        // Example outputs:
        json qual;
        qual["alarms"] = alarms;
        if (auto defects = jsonu::get_opt<int>(msg, "defects"))
            qual["defects"] = *defects;
        qual["ts"] = std::time(nullptr);

        json prod;
        prod["good_count"] = qty;
        if (auto rejects = jsonu::get_opt<int>(msg, "rejects"))
            prod["rejects"] = *rejects;
        prod["ts"] = std::time(nullptr);

        auto t1 = isa95_prefix + "/quality/alarms";
        auto t2 = isa95_prefix + "/production/line/quantity";

        return {make_pub(t1, qual), make_pub(t2, prod)};
    }
};


class PrensaHidraulica1Processor : public IMessageProcessor
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
        int line = jsonu::get_opt<int>(msg, "lineaID").value_or(0);
        int stop_q = jsonu::get_opt<int>(msg, "paradas").value_or(0);
        int stop_t = jsonu::get_opt<int>(msg, "tiempoParadas_s").value_or(0);


        json qual;
        qual["alarms"] = alarms;
        qual["ts"] = std::time(nullptr);

        json prod;
        prod["cantidad_produccion"] = prod_q;
        prod["turno"] = shiftNum;
        prod["cantidad_paradas"] = stop_q;
        prod["tiempo_paradas"] = stop_t;
        prod["ts"] = std::time(nullptr);

        auto t1 = isa95_prefix + "/prensa_hidraulica1/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/prensa_hidraulica1/production";

        return {make_pub(t1, qual), make_pub(t2, prod)};
    }
};

class PrensaHidraulica2Processor : public IMessageProcessor
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
        int line = jsonu::get_opt<int>(msg, "lineaID").value_or(0);
        int stop_q = jsonu::get_opt<int>(msg, "paradas").value_or(0);
        int stop_t = jsonu::get_opt<int>(msg, "tiempoParadas_s").value_or(0);


        json qual;
        qual["alarms"] = alarms;
        qual["ts"] = std::time(nullptr);

        json prod;
        prod["cantidad_produccion"] = prod_q;
        prod["turno"] = shiftNum;
        prod["cantidad_paradas"] = stop_q;
        prod["tiempo_paradas"] = stop_t;
        prod["ts"] = std::time(nullptr);

        auto t1 = isa95_prefix + "/prensa_hidraulica2/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/prensa_hidraulica2/production";

        return {make_pub(t1, qual), make_pub(t2, prod)};
    }
};

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
        int line = jsonu::get_opt<int>(msg, "lineaID").value_or(0);


        json qual;
        qual["alarms"] = alarms;
        qual["ts"] = std::time(nullptr);

        json prod;
        prod["cantidad_arranques"] = prod_s;
        prod["turno"] = shiftNum;
        prod["tiempo_operacion"] = prod_t;
        prod["ts"] = std::time(nullptr);

        auto t1 = isa95_prefix + "/entrada_secador/alarms";
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
        int line = jsonu::get_opt<int>(msg, "lineaID").value_or(0);
        int stop_q = jsonu::get_opt<int>(msg, "paradas").value_or(0);
        int stop_t = jsonu::get_opt<int>(msg, "tiempoParadas_s").value_or(0);


        json qual;
        qual["alarms"] = alarms;
        qual["ts"] = std::time(nullptr);

        json prod;
        prod["cantidad_produccion"] = prod_q;
        prod["turno"] = shiftNum;
        prod["cantidad_paradas"] = stop_q;
        prod["tiempo_paradas"] = stop_t;
        prod["ts"] = std::time(nullptr);

        auto t1 = isa95_prefix + "/salida_secador/alarms";
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
        int line = jsonu::get_opt<int>(msg, "lineaID").value_or(0);
        int stop_q = jsonu::get_opt<int>(msg, "paradas").value_or(0);
        int stop_t = jsonu::get_opt<int>(msg, "tiempoParadas_s").value_or(0);


        json qual;
        qual["alarms"] = alarms;
        qual["ts"] = std::time(nullptr);

        json prod;
        prod["cantidad_produccion"] = prod_q;
        prod["turno"] = shiftNum;
        prod["cantidad_paradas"] = stop_q;
        prod["tiempo_paradas"] = stop_t;
        prod["ts"] = std::time(nullptr);

        auto t1 = isa95_prefix + "/esmalte/alarms";
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
        int line = jsonu::get_opt<int>(msg, "lineaID").value_or(0);
        int stop_q = jsonu::get_opt<int>(msg, "paradas").value_or(0);
        int stop_t = jsonu::get_opt<int>(msg, "tiempoParadas_s").value_or(0);
        int falla_q = jsonu::get_opt<int>(msg, "fallaHorno").value_or(0);
        int falla_t = jsonu::get_opt<int>(msg, "tiempoFalla_s").value_or(0);


        json qual;
        qual["alarms"] = alarms;
        qual["ts"] = std::time(nullptr);

        json prod;
        prod["cantidad_produccion"] = prod_q;
        prod["turno"] = shiftNum;
        prod["cantidad_paradas"] = stop_q;
        prod["tiempo_paradas"] = stop_t;
        prod["ts"] = std::time(nullptr);
        prod["cantidad_fallas"] = falla_q;
        prod["tiempo_fallas"] = falla_t;

        auto t1 = isa95_prefix + "/entrada_horno/alarms";
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
        int line = jsonu::get_opt<int>(msg, "lineaID").value_or(0);


        json qual;
        qual["alarms"] = alarms;
        qual["ts"] = std::time(nullptr);

        json prod;
        prod["cantidad_produccion"] = prod_q;
        prod["turno"] = shiftNum;
        prod["cantidad_total"] = prod_qtotal;

        prod["ts"] = std::time(nullptr);


        auto t1 = isa95_prefix + "/salida_horno/alarms";
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
    // case DeviceType::PH_1: return std::make_unique<PH1Processor>();
    default:
        return std::make_unique<DefaultProcessor>();
    }
}
