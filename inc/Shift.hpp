#pragma once
#include <ctime>

enum class Shift : int { S1 = 1, S2 = 2, S3 = 3 };

inline Shift current_shift_localtime() {
    // Uses process TZ (set by environment/config).
    std::time_t now = std::time(nullptr);
    std::tm lt{};
#if defined(_WIN32)
    localtime_s(&lt, &now);
#else
    localtime_r(&now, &lt);
#endif
    int h = lt.tm_hour; // 0..23
    // S1: 07:00–14:59:59, S2: 15:00–22:59:59, S3: 23:00–06:59:59
    if (h >= 7  && h < 15) return Shift::S1;
    if (h >= 15 && h < 23) return Shift::S2;
    return Shift::S3; // 23..24 or 0..7
}
