#pragma once
#include <ctime>

enum class Shift : int { S1 = 1, S2 = 2, S3 = 3 };

// shift_mode: 2 = dos turnos (06:00–17:59 / 18:00–05:59)
//             3 = tres turnos de 8h (06:00–13:59 / 14:00–21:59 / 22:00–05:59)
inline Shift current_shift_localtime(int shift_mode = 3) {
    std::time_t now = std::time(nullptr);
    std::tm lt{};
#if defined(_WIN32)
    localtime_s(&lt, &now);
#else
    localtime_r(&now, &lt);
#endif

    int h = lt.tm_hour; // 0–23

    if (shift_mode == 2) {
        // Turno 1: 06:00–17:59  |  Turno 2: 18:00–05:59
        return (h >= 6 && h < 18) ? Shift::S1 : Shift::S2;
    }

    // shift_mode == 3 (por defecto)
    if (h >= 6 && h < 14)  return Shift::S1;  // 06:00–13:59
    if (h >= 14 && h < 22) return Shift::S2;  // 14:00–21:59
    return Shift::S3;                          // 22:00–05:59
}

// Hora local en la que empieza el turno que contiene esa hora.
inline int shift_start_hour(int h, int shift_mode = 3)
{
    if (shift_mode == 2) return (h >= 6 && h < 18) ? 6 : 18;
    if (h >= 6  && h < 14) return 6;
    if (h >= 14 && h < 22) return 14;
    return 22;
}

// ---------------------------------------------------------------------------
// Identidad de la INSTANCIA de turno que contiene ese instante: la época del
// arranque del turno, en hora local.
//
// El número de turno (1/2/3) no basta para decidir si dos instantes pertenecen
// al mismo turno: se repite cada día. Un hueco de ~24 h —o los de 26 h y 31 h
// documentados en el análisis de D3— cae en el mismo número, y tratarlo como
// "mismo turno" arrastra los acumuladores del día anterior al turno de hoy.
// Comparar el arranque del turno sí distingue los dos casos.
inline int64_t shift_start_epoch(int64_t epoch_s, int shift_mode = 3)
{
    std::time_t t = static_cast<std::time_t>(epoch_s);
    std::tm lt{};
#if defined(_WIN32)
    localtime_s(&lt, &t);
#else
    localtime_r(&t, &lt);
#endif

    std::tm start = lt;
    start.tm_hour  = shift_start_hour(lt.tm_hour, shift_mode);
    start.tm_min   = 0;
    start.tm_sec   = 0;
    start.tm_isdst = -1;

    std::time_t s = std::mktime(&start);
    // Los turnos que cruzan medianoche (18:00 o 22:00) empezaron ayer.
    if (static_cast<int64_t>(s) > epoch_s)
        s -= 24 * 3600;
    return static_cast<int64_t>(s);
}
