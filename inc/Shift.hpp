#pragma once
#include <ctime>

enum class Shift : int { S1 = 1, S2 = 2, S3 = 3 };

inline Shift current_shift_localtime() {
    std::time_t now = std::time(nullptr);
    std::tm lt{};
#if defined(_WIN32)
    localtime_s(&lt, &now);
#else
    localtime_r(&now, &lt);
#endif

    int h = lt.tm_hour; // 0–23

    // Shift 1: 07:00–14:59
    if (h >= 6 && h < 14)
        return Shift::S1;

    // Shift 2: 15:00–22:59
    if (h >= 14 && h < 22)
        return Shift::S2;

    // Shift 3: 23:00–06:59
    return Shift::S3;
}
