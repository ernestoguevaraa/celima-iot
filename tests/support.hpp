#pragma once
// Utilidades compartidas por las pruebas. Nada de esto entra en el binario de
// producción: el Makefile compila tests/ en un target aparte.
#include <cstdio>
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
#include "RateConfig.hpp"
#include "StateStore.hpp"
#include "Shift.hpp"

namespace testsup {

// Configuración de tasas determinista para los tests: 1 unidad/s con margen
// 1,5. Con el intervalo de 180 s la cota escalada queda en 270, y el techo
// mínimo de max_valid (5000) sigue cubriendo el camino normal; en un hueco de
// 5 h la cota sube a 27.000, que es lo que permite recuperar.
inline void rates_for_tests(double rate_per_h = 3600.0, double margin = 1.5)
{
    celima::rates().reset_for_tests();
    celima::rates().set_for_tests(rate_per_h, margin);
}

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
// Captura de std::cout Y std::cerr, para verificar los eventos [STATE] y para
// que "el camino caliente no loguea" no dé falsos negativos: capturar solo
// stdout dejaba pasar el [ESM diag] que EsmalteProcessor escribía en stderr.
class StreamCapture {
public:
    StreamCapture()
        : old_out_(std::cout.rdbuf(buf_.rdbuf()))
        , old_err_(std::cerr.rdbuf(buf_.rdbuf())) {}
    ~StreamCapture() {
        std::cout.rdbuf(old_out_);
        std::cerr.rdbuf(old_err_);
    }

    std::string str() const { return buf_.str(); }

    // Cuenta líneas del texto capturado que contienen todas las subcadenas.
    static int count_in(const std::string& text, const std::vector<std::string>& needles)
    {
        int n = 0;
        std::istringstream in(text);
        std::string line;
        while (std::getline(in, line)) {
            bool all = true;
            for (const auto& s : needles)
                if (line.find(s) == std::string::npos) { all = false; break; }
            if (all) ++n;
        }
        return n;
    }

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
    std::streambuf* old_out_;
    std::streambuf* old_err_;
};

// Ejecuta f() con cout/cerr capturados y devuelve lo emitido.
//
// Úsalo siempre en lugar de tener un StreamCapture vivo mientras se evalúan
// CHECKs: doctest informa de los fallos por std::cout, así que una captura
// activa se traga el informe y el fallo aparece sin explicación.
template <typename F>
inline std::string capture_streams(F&& f)
{
    StreamCapture cap;
    f();
    return cap.str();
}

inline int count_lines(const std::string& text, const std::vector<std::string>& needles)
{
    return StreamCapture::count_in(text, needles);
}

// ---------------------------------------------------------------------------
// Constructor de tramas para los 8 deviceType.
//
// Todos los contadores avanzan `step` por tick (timer1Hz avanza 180, un tick =
// un intervalo de 180 s) y gatewayTime avanza con el tick, así que el hueco
// entre tramas es explícito: pasa un `tick` salteado para simular un hueco.
inline std::string iso_utc(int64_t epoch_s)
{
    std::time_t t = static_cast<std::time_t>(epoch_s);
    std::tm utc{};
    gmtime_r(&t, &utc);
    char buf[32];
    std::strftime(buf, sizeof buf, "%Y-%m-%dT%H:%M:%SZ", &utc);
    return buf;
}

// Campos de entrada por deviceType, tal como los lee cada procesador.
inline const std::vector<const char*>& frame_fields(int dt)
{
    static const std::vector<const char*> f1 = {
        "cantidadProductos", "tiempoProduccion_ds", "paradas", "tiempoParadas_s"};
    static const std::vector<const char*> f3 = {
        "paradas_cantidad", "paradas_tempo_s", "ingreso_elevador_cantidad",
        "ingreso_elevador_tiempo_ds", "bancalino_l1_cantidad", "bancalino_l1_tiempo_ds",
        "bancalino_l2_cantidad", "bancalino_l2_tiempo_ds"};
    static const std::vector<const char*> f4 = {
        "parada_mds_cantidad", "parada_mds_tiempo_s", "metrica_mds_cantidad",
        "metrica_mds_tiempo_ds"};
    static const std::vector<const char*> f5 = {
        "parada_esm_cantidad", "parada_esm_tiempo_s", "metrica_esm_cantidad",
        "metrica_esm_tiempo_ds"};
    static const std::vector<const char*> f6 = {
        "numero_grades", "parada_mcf_cantidad", "parada_mcf_tiempo_s",
        "metrica_mcf_cantidad", "metrica_mcf_tiempo_ds", "metrica_formador_cantidad",
        "metrica_formador_tiempo_ds", "falha_forno_cantidad", "falha_forno_tiempo_s"};
    static const std::vector<const char*> f7 = {
        "paradas_cantidad", "paradas_tempo", "metrica_mdf_ciclos", "metrica_mdf_tiempo",
        "bancalinos_q301", "bancalinos_q300", "bancalinos_comb1", "bancalinos_comb2",
        "bancalinos_total", "parada_escolha_cantidad", "parada_escolha_tempo",
        "sentido_escolha_cantidad", "sentido_escolha_tiempo", "barreira1_cantidad",
        "barreira1_tiempo"};
    static const std::vector<const char*> f8 = {
        "boxesQ1", "boxesQ2", "boxesQ6", "totalBroken"};
    static const std::vector<const char*> none = {};
    switch (dt) {
        case 1: case 2: return f1;
        case 3: return f3;
        case 4: return f4;
        case 5: return f5;
        case 6: return f6;
        case 7: return f7;
        case 8: return f8;
        default: return none;
    }
}

// epoch base fijo: 2026-09-02T08:00:00Z
inline constexpr int64_t kBaseEpoch = 1788336000;
inline constexpr int     kInterval  = 180;

// Intervalo de publicación por clase de dispositivo: entrada_horno (6) publica
// cada ~120 s; el resto, cada ~180 s.
inline constexpr int frame_interval(int dt) { return dt == 6 ? 120 : 180; }

inline nlohmann::json make_frame(int dt, int line, int tick, int step = 64,
                                 int base = 1000)
{
    const int64_t elapsed = static_cast<int64_t>(tick) * frame_interval(dt);
    nlohmann::json m;
    m["deviceType"]  = dt;
    m["lineID"]      = line;
    m["alarms"]      = 0;
    m["checksum"]    = 42;
    m["gatewayTime"] = iso_utc(kBaseEpoch + elapsed);
    // timer1Hz es un contador libre a 1 Hz: avanza exactamente los segundos
    // transcurridos, no un incremento arbitrario.
    if (dt >= 3 && dt <= 7)
        m["timer1Hz"] = static_cast<uint16_t>(base + elapsed);
    for (const char* f : frame_fields(dt))
        m[f] = static_cast<uint16_t>(base + tick * step);
    if (dt == 8)
        m["freshBoot"] = (tick == 0);
    return m;
}

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

// Directorio temporal para artefactos de test (config, bases de estado).
inline const char* tmpdir()
{
    if (const char* t = std::getenv("TMPDIR"))
        if (t[0] != '\0') return t;
    return "/tmp";
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
