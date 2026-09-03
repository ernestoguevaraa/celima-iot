// Fronteras de turno y zona horaria.
//
// current_shift_localtime() lee la hora local del SO. La PC de planta está en
// America/Lima; Ubuntu Server se aprovisiona en UTC por omisión. Si la VM queda
// en UTC las fronteras se corren cinco horas y la producción se atribuye al
// turno equivocado sin generar ningún error, así que aquí se fija la hora y se
// deja constancia explícita de esa divergencia.
#include "doctest.h"
#include "support.hpp"

TEST_CASE("SHIFT_MODE=3: tres turnos de 8 h") {
    struct { int hour; int shift; } cases[] = {
        {6, 1}, {10, 1}, {13, 1},
        {14, 2}, {18, 2}, {21, 2},
        {22, 3}, {2, 3}, {5, 3},
    };
    for (const auto& c : cases) {
        CAPTURE(c.hour);
        testsup::pin_local_hour(c.hour);
        REQUIRE(testsup::local_hour_now() == c.hour);
        CHECK(static_cast<int>(current_shift_localtime(3)) == c.shift);
    }
}

TEST_CASE("SHIFT_MODE=2: dos turnos de 12 h") {
    struct { int hour; int shift; } cases[] = {
        {6, 1}, {12, 1}, {17, 1},
        {18, 2}, {23, 2}, {0, 2}, {5, 2},
    };
    for (const auto& c : cases) {
        CAPTURE(c.hour);
        testsup::pin_local_hour(c.hour);
        REQUIRE(testsup::local_hour_now() == c.hour);
        CHECK(static_cast<int>(current_shift_localtime(2)) == c.shift);
    }
}

TEST_CASE("los dos modos discrepan donde debe: 14–17 y 22–05") {
    testsup::pin_local_hour(15);
    CHECK(static_cast<int>(current_shift_localtime(2)) == 1);
    CHECK(static_cast<int>(current_shift_localtime(3)) == 2);

    testsup::pin_local_hour(23);
    CHECK(static_cast<int>(current_shift_localtime(2)) == 2);
    CHECK(static_cast<int>(current_shift_localtime(3)) == 3);
}

TEST_CASE("TZ=UTC corre las fronteras cinco horas respecto a America/Lima") {
    // El mismo instante, con las dos zonas: el turno reportado NO coincide.
    // Es el fallo silencioso que hay que evitar al aprovisionar la VM; si algún
    // día este CHECK deja de cumplirse, es que alguien cambió las fronteras.
    setenv("TZ", "UTC", 1);
    tzset();
    const int utc_hour = testsup::local_hour_now();
    const int shift_utc = static_cast<int>(current_shift_localtime(2));

    setenv("TZ", "America/Lima", 1);
    tzset();
    const int lima_hour = testsup::local_hour_now();
    const int shift_lima = static_cast<int>(current_shift_localtime(2));

    CHECK(((utc_hour - lima_hour + 24) % 24) == 5);

    // En las cinco horas de desfase los turnos difieren; fuera de ellas no.
    const bool boundary_crossed = (lima_hour >= 1 && lima_hour < 6)
                               || (lima_hour >= 13 && lima_hour < 18);
    if (boundary_crossed)
        CHECK(shift_utc != shift_lima);
    else
        CHECK(shift_utc == shift_lima);
}

TEST_CASE("shift_start_epoch identifica la instancia de turno, no su número") {
    // El número (1/2/3) se repite cada día: un hueco de 24 h vuelve al mismo y
    // arrastraba los acumuladores del día anterior. La instancia es la época en
    // que arrancó ese turno concreto.
    // pin_local_hour fija la HORA y conserva los minutos reales, así que las
    // comprobaciones van por rango de una hora, no por igualdad exacta.
    testsup::pin_local_hour(10);
    const int64_t ahora = static_cast<int64_t>(std::time(nullptr));

    SUBCASE("modo 3: turnos de 8 h") {
        const int64_t inicio = shift_start_epoch(ahora, 3);
        // Entre las 10:00 y las 10:59 el turno empezó a las 06:00.
        CHECK(ahora - inicio >= 4 * 3600);
        CHECK(ahora - inicio <  5 * 3600);
        // Dentro del mismo turno, la instancia no cambia.
        CHECK(shift_start_epoch(ahora + 3 * 3600, 3) == inicio);
        // Al cruzar las 14:00, sí.
        CHECK(shift_start_epoch(ahora + 5 * 3600, 3) != inicio);
        // Y 24 h después es OTRA instancia, aunque el número de turno coincida.
        CHECK(static_cast<int>(current_shift_localtime(3)) ==
              static_cast<int>(current_shift_localtime(3)));
        CHECK(shift_start_epoch(ahora + 24 * 3600, 3) == inicio + 24 * 3600);
        CHECK(shift_start_epoch(ahora + 24 * 3600, 3) != inicio);
    }

    SUBCASE("modo 2: turnos de 12 h") {
        const int64_t inicio = shift_start_epoch(ahora, 2);
        CHECK(ahora - inicio >= 4 * 3600);          // 10:xx, turno desde 06:00
        CHECK(ahora - inicio <  5 * 3600);
        CHECK(shift_start_epoch(ahora + 7 * 3600, 2) == inicio);   // 17:00, mismo
        CHECK(shift_start_epoch(ahora + 8 * 3600, 2) != inicio);   // 18:00, otro
        CHECK(shift_start_epoch(ahora + 24 * 3600, 2) == inicio + 24 * 3600);
    }

    SUBCASE("el turno que cruza medianoche arrancó ayer") {
        // 02:00 local: en modo 3 pertenece al turno que empezó a las 22:00 del
        // día anterior; en modo 2, al que empezó a las 18:00.
        testsup::pin_local_hour(2);
        const int64_t madrugada = static_cast<int64_t>(std::time(nullptr));
        CHECK(madrugada - shift_start_epoch(madrugada, 3) >= 4 * 3600);
        CHECK(madrugada - shift_start_epoch(madrugada, 3) <  5 * 3600);
        CHECK(madrugada - shift_start_epoch(madrugada, 2) >= 8 * 3600);
        CHECK(madrugada - shift_start_epoch(madrugada, 2) <  9 * 3600);
        // Y el arranque es anterior al instante, nunca posterior.
        CHECK(shift_start_epoch(madrugada, 3) < madrugada);
        CHECK(shift_start_epoch(madrugada, 2) < madrugada);
    }

    SUBCASE("en la hora de arranque, el turno acaba de empezar") {
        // Cada modo tiene sus fronteras: 06/14/22 en modo 3, 06/18 en modo 2.
        // Una hora que arranca turno en un modo no lo arranca en el otro.
        for (int h : {6, 14, 22}) {
            CAPTURE(h);
            testsup::pin_local_hour(h);
            const int64_t t = static_cast<int64_t>(std::time(nullptr));
            CHECK(t - shift_start_epoch(t, 3) < 3600);   // < 1 h desde el arranque
        }
        for (int h : {6, 18}) {
            CAPTURE(h);
            testsup::pin_local_hour(h);
            const int64_t t = static_cast<int64_t>(std::time(nullptr));
            CHECK(t - shift_start_epoch(t, 2) < 3600);
        }
        // Y una hora que NO es frontera queda lejos del arranque.
        testsup::pin_local_hour(13);
        const int64_t t = static_cast<int64_t>(std::time(nullptr));
        CHECK(t - shift_start_epoch(t, 3) >= 7 * 3600);   // turno S1 desde las 06
        CHECK(t - shift_start_epoch(t, 2) >= 7 * 3600);
    }
}
