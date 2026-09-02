#pragma once
// Utilidades compartidas por las pruebas. Nada de esto entra en el binario de
// producción: el Makefile compila tests/ en un target aparte.
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "DeviceTypes.hpp"
#include "JsonUtils.hpp"
#include "MessageProcessor.hpp"
#include "Shift.hpp"

namespace testsup {

// ---------------------------------------------------------------------------
// Determinismo de turno.
//
// current_shift_localtime() lee el reloj del sistema, así que el resultado de
// un replay depende de cuándo se ejecute. Fijamos TZ a un offset constante
// calculado para que la hora local caiga en la hora pedida, sea cual sea el
// momento real de la ejecución. Las fronteras de turno están en horas en punto,
// así que basta con clavar la hora.
//
// POSIX invierte el signo del offset: TZ="XXX-5" significa UTC+5.
inline void pin_local_hour(int target_hour)
{
    const std::time_t now = std::time(nullptr);
    std::tm utc{};
    gmtime_r(&now, &utc);

    const int offset = target_hour - utc.tm_hour;   // horas a sumar a UTC
    const std::string tz = "XXX" + std::to_string(-offset);
    setenv("TZ", tz.c_str(), 1);
    tzset();
}

inline int local_hour_now()
{
    const std::time_t now = std::time(nullptr);
    std::tm lt{};
    localtime_r(&now, &lt);
    return lt.tm_hour;
}

// ---------------------------------------------------------------------------
// Captura de std::cout, para verificar los eventos [STATE].
class CoutCapture {
public:
    CoutCapture() : old_(std::cout.rdbuf(buf_.rdbuf())) {}
    ~CoutCapture() { std::cout.rdbuf(old_); }

    std::string str() const { return buf_.str(); }

    // Cuenta líneas que contienen todas las subcadenas dadas.
    int count(const std::vector<std::string>& needles) const
    {
        int n = 0;
        std::istringstream in(buf_.str());
        std::string line;
        while (std::getline(in, line)) {
            bool all = true;
            for (const auto& s : needles)
                if (line.find(s) == std::string::npos) { all = false; break; }
            if (all) ++n;
        }
        return n;
    }

private:
    std::ostringstream buf_;
    std::streambuf* old_;
};

// ---------------------------------------------------------------------------
// Replay: reproduce el enrutado de MqttApp::handle_celima_data() sin broker.
// MqttApp está pegado a Paho y no se puede instanciar sin conexión, así que
// esta es una copia deliberada de esas ~15 líneas. Si cambia el enrutado,
// cambia aquí también.
inline std::string replay_file(const std::string& path, int shift_mode)
{
    std::ifstream in(path);
    if (!in) return {};

    std::ostringstream dump;
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) continue;

        std::string err;
        auto jopt = jsonu::parse(line, err);
        if (!jopt) continue;
        auto& j = *jopt;

        const int devTypeInt = j.value("deviceType", 0);
        auto dt = deviceTypeFromInt(devTypeInt);
        auto proc = dt ? createProcessor(*dt) : createDefaultProcessor();

        const int shiftNum = static_cast<int>(current_shift_localtime(shift_mode));
        if (detect_global_shift_change(shiftNum))
            reset_all_processor_states();

        for (const auto& p : proc->process(j, "celima/punta_hermosa/planta/linea/", shift_mode))
            dump << p.topic << '\t' << p.payload << '\n';
    }
    return dump.str();
}

inline std::string read_file(const std::string& path)
{
    std::ifstream in(path);
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

inline bool write_file(const std::string& path, const std::string& content)
{
    std::ofstream out(path, std::ios::trunc);
    if (!out) return false;
    out << content;
    return out.good();
}

} // namespace testsup
