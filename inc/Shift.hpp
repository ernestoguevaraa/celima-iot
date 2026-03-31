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
