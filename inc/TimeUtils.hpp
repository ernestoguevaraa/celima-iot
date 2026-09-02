#pragma once
#include <string>
#include <sstream>
#include <iomanip>
#include <chrono>
#include <ctime>
#include <optional>
#include <cstdio>
#include <cctype>
#include <nlohmann/json.hpp>

inline std::string iso8601_utc_now()
{
    using namespace std::chrono;

    const auto now = system_clock::now();
    const auto ms  = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;
    const std::time_t t = system_clock::to_time_t(now);

    std::tm utc{};
#if defined(_WIN32)
    gmtime_s(&utc, &t);
#else
    gmtime_r(&t, &utc);
#endif

    std::ostringstream oss;
    oss << std::put_time(&utc, "%Y-%m-%dT%H:%M:%S")
        << '.' << std::setfill('0') << std::setw(3) << ms.count() << 'Z';
    return oss.str();
}

// ---------------------------------------------------------------------------
// Convierte un gatewayTime ISO8601 con offset a UTC en el MISMO formato que
// iso8601_utc_now():  "YYYY-MM-DDTHH:MM:SS.mmmZ"
//
// Acepta:   "2026-06-12T17:52:24-05:00"  (offset negativo, caso UG65)
//           "2026-06-12T10:00:00+02:00"  (offset positivo)
//           "2026-06-12T22:52:24Z"       (ya UTC)
//           con o sin fracciones de segundo (.NNN)
// Devuelve: el string UTC, o std::nullopt si el formato no es parseable.
//
// gatewayTime del UG65 viene con resolución de segundos -> se emite ".000Z".
// Si trae fracciones, se preservan los primeros 3 dígitos (ms).
// ---------------------------------------------------------------------------
// Parseo común de gatewayTime: devuelve la época UTC en segundos y la parte
// fraccionaria en ms. Lo usan gateway_time_to_iso8601_utc() (para el campo
// publicado) y device_epoch_s() (para medir huecos). Un solo parser para que
// las dos lecturas no puedan divergir.
inline bool parse_gateway_time(const std::string& gw, std::time_t& epoch_s, int& frac_ms)
{
    if (gw.size() < 19) return false;          // mínimo "YYYY-MM-DDTHH:MM:SS"

    int year, mon, day, hour, min, sec;
    if (std::sscanf(gw.c_str(), "%d-%d-%dT%d:%d:%d",
                    &year, &mon, &day, &hour, &min, &sec) != 6)
        return false;

    size_t pos = 19;
    frac_ms = 0;
    if (pos < gw.size() && gw[pos] == '.') {
        size_t fstart = pos + 1, fend = fstart;
        while (fend < gw.size() && std::isdigit((unsigned char)gw[fend])) ++fend;
        std::string fs = gw.substr(fstart, std::min<size_t>(3, fend - fstart));
        while (fs.size() < 3) fs += '0';
        frac_ms = std::stoi(fs);
        pos = fend;
    }

    int off_sec = 0;
    if (pos < gw.size()) {
        char c = gw[pos];
        if (c == 'Z') {
            off_sec = 0;
        } else if (c == '+' || c == '-') {
            int oh = 0, om = 0;
            if (std::sscanf(gw.c_str() + pos + 1, "%d:%d", &oh, &om) >= 1)
                off_sec = (oh * 3600 + om * 60) * (c == '-' ? -1 : 1);
            else
                return false;
        } else {
            return false;
        }
    }

    std::tm tm{};
    tm.tm_year = year - 1900; tm.tm_mon = mon - 1; tm.tm_mday = day;
    tm.tm_hour = hour; tm.tm_min = min; tm.tm_sec = sec;
    tm.tm_isdst = 0;

#if defined(_WIN32)
    std::time_t local_as_utc = _mkgmtime(&tm);
#else
    std::time_t local_as_utc = timegm(&tm);
#endif
    epoch_s = local_as_utc - off_sec;
    return true;
}

// ---------------------------------------------------------------------------
// Época UTC en segundos del gatewayTime del mensaje.
//
// Devuelve nullopt si el campo falta o no parsea, y en ese caso NO cae a la
// hora del servidor: sin timestamp de dispositivo fiable el hueco no es
// medible, y un delta sin cota medible debe tratarse como implausible.
// Es deliberadamente distinto de device_timestamp(), que sí tiene fallback
// porque solo alimenta el campo publicado. No unifiques los dos.
inline std::optional<int64_t> device_epoch_s(const nlohmann::json& msg)
{
    if (!msg.contains("gatewayTime") || !msg["gatewayTime"].is_string())
        return std::nullopt;

    std::time_t epoch = 0;
    int frac_ms = 0;
    if (!parse_gateway_time(msg.value("gatewayTime", std::string{}), epoch, frac_ms))
        return std::nullopt;
    return static_cast<int64_t>(epoch);
}

inline std::optional<std::string> gateway_time_to_iso8601_utc(const std::string& gw)
{
    std::time_t real_utc = 0;
    int frac_ms = 0;
    if (!parse_gateway_time(gw, real_utc, frac_ms))
        return std::nullopt;

    std::tm out{};
#if defined(_WIN32)
    gmtime_s(&out, &real_utc);
#else
    gmtime_r(&real_utc, &out);
#endif
    std::ostringstream oss;
    oss << std::put_time(&out, "%Y-%m-%dT%H:%M:%S")
        << '.' << std::setfill('0') << std::setw(3) << frac_ms << 'Z';
    return oss.str();
}

// ---------------------------------------------------------------------------
// Helper de conveniencia: extrae "gatewayTime" del mensaje y lo convierte a UTC.
// Si el campo falta o no parsea, hace fallback a iso8601_utc_now() (hora del
// servidor), garantizando que NINGÚN registro quede sin timestamp.
//
// Uso en cada procesador, reemplazando iso8601_utc_now():
//     prod["timestamp_device"] = device_timestamp(msg);
// ---------------------------------------------------------------------------
inline std::string device_timestamp(const nlohmann::json& msg)
{
    if (msg.contains("gatewayTime") && msg["gatewayTime"].is_string()) {
        auto converted = gateway_time_to_iso8601_utc(
            msg.value("gatewayTime", std::string{}));
        if (converted) return *converted;
    }
    return iso8601_utc_now();   // fallback: hora del servidor
}