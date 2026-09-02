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
