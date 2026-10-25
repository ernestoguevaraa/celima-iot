// Mensajes de arranque y diagnóstico del firmware v6.
//
// El codec del UG65 separa por fPort: el 1 son las tramas de producción de
// siempre (sin msgType), el 2 el reporte de arranque ("arranque") y el 3 el de
// diagnóstico ("diag"). Los tres llegan mezclados en celima/data, y el fPort no
// viaja en el JSON: lo único que los distingue es msgType.
//
// Nada de esto toca a los procesadores de producción: ni su estado, ni el
// cambio de turno global, ni la persistencia. Es una vía aparte que desemboca
// en ${ISA95_PREFIX}<línea>/<estación>/diagnostico.
#include "MessageProcessor.hpp"
#include "JsonUtils.hpp"
#include "Logging.hpp"
#include "TimeUtils.hpp"
#include <array>
#include <map>
#include <mutex>
#include <set>
#include <utility>

using json = nlohmann::json;

namespace {

// Primer aviso por texto distinto, el mismo patrón que is_decoder_error(): un
// dispositivo mal configurado manda la misma trama cada pocos minutos, y basta
// con verla una vez para saber que existe.
bool first_seen(const std::string& key)
{
    static std::mutex mtx;
    static std::set<std::string> seen;
    std::lock_guard<std::mutex> lock(mtx);
    return seen.insert(key).second;
}

void ignore_once(const json& msg, const std::string& reason, const std::string& detail)
{
    if (!first_seen(reason + "|" + detail))
        return;
    celima::log::state_event("frame_ignored", msg.value("lineID", -1), "diagnostico",
        "reason=" + reason + " " + detail +
        " devEUI=" + jsonu::get_opt<std::string>(msg, "devEUI").value_or("?"));
}

std::string text_of(const json& v)
{
    return v.is_string() ? v.get<std::string>() : v.dump();
}

// Valor de un campo del mensaje para un evento [STATE]: "?" si falta.
std::string field_text(const json& msg, const char* key)
{
    return msg.contains(key) ? text_of(msg[key]) : "?";
}

} // namespace

/**
 * DiagnosticoProcessor: publica los reportes de arranque y de diagnóstico.
 *
 * Los cuatro contadores de diag (glitches, rechazosConteo, rechazosIntegridad,
 * descartadas) son acumulados uint16 desde el arranque del EQUIPO y vuelven a 0
 * en cada reinicio. Se publican tal cual y, además, su delta contra el diag
 * anterior del mismo (línea, deviceType).
 *
 * El delta va en null cuando no hay base contra la que restar:
 *  - primer diag tras arrancar el servicio (la base vive solo en memoria, a
 *    propósito: no es producción, y un null tras un reinicio no pierde nada que
 *    los acumulados publicados no permitan reconstruir);
 *  - primer diag tras un arranque del equipo, que borra la base;
 *  - un contador que BAJA sin arranque de por medio.
 *
 * Esto último se trata como un reinicio del equipo cuyo reporte de arranque se
 * perdió, no como un rollover: son contadores de anomalías, y llegar a 65.535
 * entre dos reinicios supondría cientos de fallos por hora durante días. En la
 * práctica no dan la vuelta, así que una bajada solo puede ser un reinicio. Se
 * re-ancla con los valores actuales y el siguiente diag ya tiene delta.
 */
class DiagnosticoProcessor : public IMessageProcessor
{
public:
    std::vector<Publication> process(const json &msg, const std::string &isa95_prefix,
                                     int /*shift_mode*/ = 3) override
    {
        const int devTypeInt = msg.value("deviceType", 0);
        const auto dt = deviceTypeFromInt(devTypeInt);
        if (!dt) {
            ignore_once(msg, "deviceType_desconocido",
                        "deviceType=" + field_text(msg, "deviceType"));
            return {};
        }
        const auto line = jsonu::get_opt<int>(msg, "lineID");
        if (!line) {
            ignore_once(msg, "lineID_invalido", "lineID=" + field_text(msg, "lineID"));
            return {};
        }

        const char *station = stationSegment(*dt);
        const std::string type = msg.value("msgType", std::string{});

        json out = type == "arranque" ? arranque(msg, *line, devTypeInt, station)
                                      : diag(msg, *line, devTypeInt, station);

        // Tal como vienen en el mensaje; si faltan, no se inventan.
        if (msg.contains("devEUI"))     out["devEUI"]     = msg["devEUI"];
        if (msg.contains("deviceName")) out["deviceName"] = msg["deviceName"];
        out["timestamp_device"] = device_timestamp(msg);

        return { Publication{isa95_topic(isa95_prefix, *line, *dt, "diagnostico"), out.dump()} };
    }

    static void reset_states()
    {
        std::lock_guard<std::mutex> lock(mtx_);
        bases_.clear();
    }

private:
    static constexpr std::array<const char *, 4> kCounters = {
        "glitches", "rechazosConteo", "rechazosIntegridad", "descartadas"};

    // Línea base de diag por (línea, deviceType). Un contador ausente o fuera
    // de rango queda sin base y su delta sale en null.
    using Base = std::array<std::optional<uint16_t>, 4>;

    static std::mutex mtx_;
    static std::map<std::pair<int, int>, Base> bases_;

    static std::optional<uint16_t> counter(const json &msg, const char *key)
    {
        const auto v = jsonu::get_opt<int64_t>(msg, key);
        if (!v || *v < 0 || *v > 0xFFFF) return std::nullopt;
        return static_cast<uint16_t>(*v);
    }

    static std::string identity(const json &msg)
    {
        return " devEUI=" + field_text(msg, "devEUI") +
               " deviceName=" + field_text(msg, "deviceName");
    }

    json arranque(const json &msg, int line, int devType, const char *station)
    {
        {
            std::lock_guard<std::mutex> lock(mtx_);
            bases_.erase({line, devType});
        }
        celima::log::state_event("arranque", line, station,
            "causa=" + field_text(msg, "causaReinicio") +
            " causaCodigo=" + field_text(msg, "causaReinicioCodigo") +
            " joinsFallidos=" + field_text(msg, "joinsFallidos") + identity(msg));

        json out;
        out["tipo"]                = "arranque";
        out["causaReinicio"]       = msg.value("causaReinicio", json());
        out["causaReinicioCodigo"] = msg.value("causaReinicioCodigo", json());
        out["joinsFallidos"]       = msg.value("joinsFallidos", json());
        return out;
    }

    json diag(const json &msg, int line, int devType, const char *station)
    {
        Base curr;
        for (size_t i = 0; i < kCounters.size(); ++i)
            curr[i] = counter(msg, kCounters[i]);

        std::array<json, 4> deltas;   // null por defecto
        {
            std::lock_guard<std::mutex> lock(mtx_);
            const auto it = bases_.find({line, devType});
            if (it != bases_.end()) {
                const Base &prev = it->second;
                bool dropped = false;
                for (size_t i = 0; i < kCounters.size(); ++i)
                    if (curr[i] && prev[i] && *curr[i] < *prev[i]) dropped = true;
                // Una bajada es un reinicio sin reporte: todos los deltas en
                // null, no solo el del contador que bajó.
                if (!dropped)
                    for (size_t i = 0; i < kCounters.size(); ++i)
                        if (curr[i] && prev[i]) deltas[i] = *curr[i] - *prev[i];
            }
            bases_[{line, devType}] = curr;
        }

        json out;
        out["tipo"] = msg.value("tipoDiag", json());
        for (size_t i = 0; i < kCounters.size(); ++i) {
            out[kCounters[i]] = msg.value(kCounters[i], json());
            out[std::string(kCounters[i]) + "_delta"] = deltas[i];
        }

        // El equipo no tiene datos válidos que mandar y solo late: es la alarma
        // que hay que ver en el journal sin esperar a un panel.
        if (msg.value("tipoDiag", std::string{}) == "latido") {
            std::string detail;
            for (const char *k : kCounters)
                detail += std::string(detail.empty() ? "" : " ") + k + "=" + field_text(msg, k);
            celima::log::state_event("alarma_sin_datos_validos", line, station,
                                     detail + identity(msg));
        }
        return out;
    }
};

std::mutex DiagnosticoProcessor::mtx_;
std::map<std::pair<int, int>, DiagnosticoProcessor::Base> DiagnosticoProcessor::bases_;

std::optional<std::vector<Publication>> process_typed_message(const json &msg,
                                                              const std::string &isa95_prefix)
{
    if (!msg.contains("msgType"))
        return std::nullopt;

    const json &mt = msg["msgType"];
    if (mt == "arranque" || mt == "diag")
        return DiagnosticoProcessor().process(msg, isa95_prefix);

    ignore_once(msg, "msgType_desconocido", "msgType=" + text_of(mt));
    return std::vector<Publication>{};
}

void reset_diagnostico_state_for_tests()
{
    DiagnosticoProcessor::reset_states();
}
