// Eventos [STATE] — criterios de aceptación 4 (re-siembras visibles) y
// 5 (re-anclajes visibles), más el shift_change_global de la tabla del PR 1.
//
// Los tests fijan la hora local en lugar de esperar al reloj: pin_local_hour()
// clava TZ a un offset calculado, así que el turno es el mismo se ejecute la
// suite a la hora que sea.
#include "doctest.h"
#include "support.hpp"

using json = nlohmann::json;
using testsup::count_lines;
using testsup::capture_streams;

static const std::string kPfx = "celima/punta_hermosa/planta/linea/";

TEST_CASE("reseed: primer mensaje y cambio de turno, una vez por procesador y línea") {
    testsup::pin_local_hour(10);          // 10:00 → S1 en modo 3
    REQUIRE(testsup::local_hour_now() == 10);
    reset_all_processor_states();
    testsup::rates_for_tests();

    auto secador = createProcessor(DeviceType::Entrada_secador);
    auto mds     = createProcessor(DeviceType::Salida_secador);

    std::string log = capture_streams([&] {
        secador->process(testsup::make_frame(3, 1, 0), kPfx, 3);
        mds->process(testsup::make_frame(4, 2, 0), kPfx, 3);
    });
    CHECK(count_lines(log, {"[STATE] reseed", "proc=entrada_secador", "line=1",
                            "reason=first_message", "shift=1"}) == 1);
    CHECK(count_lines(log, {"[STATE] reseed", "proc=salida_secador", "line=2",
                            "reason=first_message", "shift=1"}) == 1);
    CHECK(count_lines(log, {"reason=shift_change"}) == 0);

    testsup::pin_local_hour(15);          // 15:00 → S2 en modo 3
    REQUIRE(testsup::local_hour_now() == 15);
    log = capture_streams([&] {
        secador->process(testsup::make_frame(3, 1, 1), kPfx, 3);
        mds->process(testsup::make_frame(4, 2, 1), kPfx, 3);
    });
    // Exactamente uno por procesador y línea, no uno por campo.
    CHECK(count_lines(log, {"[STATE] reseed", "proc=entrada_secador", "line=1",
                            "reason=shift_change", "shift_prev=1", "shift_new=2"}) == 1);
    CHECK(count_lines(log, {"[STATE] reseed", "proc=salida_secador", "line=2",
                            "reason=shift_change", "shift_prev=1", "shift_new=2"}) == 1);
    CHECK(count_lines(log, {"[STATE] reseed"}) == 2);
}

TEST_CASE("delta_rejected y reanchor: tres rechazos consecutivos dejan rastro") {
    testsup::pin_local_hour(10);
    reset_all_processor_states();
    testsup::rates_for_tests();

    auto secador = createProcessor(DeviceType::Entrada_secador);

    // Baseline: la primera trama solo siembra el estado.
    secador->process(testsup::make_frame(3, 3, 0), kPfx, 3);

    // Tres tramas con el mismo salto anómalo en un solo campo. prev_ref no
    // avanza mientras se rechaza, así que las tres se rechazan y la tercera
    // fuerza el re-ancla. El resto de contadores avanza de forma plausible.
    const std::string log = capture_streams([&] {
        for (int tick = 1; tick <= 3; ++tick) {
            json m = testsup::make_frame(3, 3, tick);
            m["ingreso_elevador_cantidad"] = 21000;   // salto fuera de toda cota
            secador->process(m, kPfx, 3);
        }
    });

    CHECK(count_lines(log, {"[STATE] delta_rejected", "proc=entrada_secador", "line=3",
                            "field=ingreso_elevador_cantidad"}) == 3);
    CHECK(count_lines(log, {"[STATE] delta_rejected", "reject_count=1"}) == 1);
    CHECK(count_lines(log, {"[STATE] delta_rejected", "reject_count=2"}) == 1);
    CHECK(count_lines(log, {"[STATE] delta_rejected", "reject_count=3"}) == 1);
    CHECK(count_lines(log, {"[STATE] reanchor", "proc=entrada_secador", "line=3",
                            "field=ingreso_elevador_cantidad"}) == 1);
    // Ningún otro campo se rechazó.
    CHECK(count_lines(log, {"[STATE] delta_rejected"}) == 3);
    CHECK(count_lines(log, {"[STATE] reanchor"}) == 1);

    // Tras el re-ancla, el siguiente delta legítimo vuelve a contar.
    const std::string log2 = capture_streams([&] {
        json m = testsup::make_frame(3, 3, 4);
        m["ingreso_elevador_cantidad"] = 21064;
        secador->process(m, kPfx, 3);
    });
    CHECK(count_lines(log2, {"[STATE] delta_rejected"}) == 0);
    CHECK(count_lines(log2, {"[STATE] reanchor"}) == 0);
}

TEST_CASE("la rama de acumulación normal no loguea, en ningún procesador") {
    // Captura stdout Y stderr: con solo stdout este test daba un falso negativo
    // (el [ESM diag] de esmalte salía por stderr).
    testsup::pin_local_hour(10);
    testsup::rates_for_tests();

    for (int dt = 1; dt <= 8; ++dt) {
        CAPTURE(dt);
        reset_all_processor_states();
        auto proc = createProcessor(*deviceTypeFromInt(dt));

        // Siembra: primer mensaje (reseed) y, en calidad, el ancla del baseline.
        proc->process(testsup::make_frame(dt, 1, 0), kPfx, 3);

        const std::string log = capture_streams([&] {
            for (int tick = 1; tick <= 10; ++tick)
                proc->process(testsup::make_frame(dt, 1, tick), kPfx, 3);
        });
        CHECK_MESSAGE(log.empty(),
                      "deviceType " << dt << " loguea en el camino caliente: " << log);
    }
}

TEST_CASE("shift_change_global deja rastro una sola vez por cambio") {
    detect_global_shift_change(1);        // baseline conocido

    const std::string log = capture_streams([&] {
        CHECK(detect_global_shift_change(2) == true);
        CHECK(detect_global_shift_change(2) == false);
    });
    CHECK(count_lines(log, {"[STATE] shift_change_global", "proc=global",
                            "shift_prev=1", "shift_new=2"}) == 1);
    CHECK(count_lines(log, {"[STATE] shift_change_global"}) == 1);
}

TEST_CASE("calidad: el re-ancla del baseline deja rastro") {
    testsup::pin_local_hour(10);
    reset_all_processor_states();
    testsup::rates_for_tests();

    auto calidad = createProcessor(DeviceType::Calidad);

    std::vector<Publication> pubs;
    const std::string log = capture_streams([&] {
        pubs = calidad->process(testsup::make_frame(8, 1, 0), kPfx, 3);
    });
    CHECK(pubs.empty());                  // el baseline no publica
    CHECK(count_lines(log, {"[STATE] reanchor", "proc=calidad", "line=1",
                            "reason=fresh_boot"}) == 1);
}
