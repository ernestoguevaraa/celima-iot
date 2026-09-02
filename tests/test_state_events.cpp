// Eventos [STATE] — criterios de aceptación 4 (re-siembras visibles) y
// 5 (re-anclajes visibles), más el shift_change_global de la tabla del PR.
//
// Los tests fijan la hora local en lugar de esperar al reloj: pin_local_hour()
// clava TZ a un offset calculado, así que el turno es el mismo se ejecute la
// suite a la hora que sea.
#include "doctest.h"
#include "support.hpp"

using json = nlohmann::json;

// Trama de entrada_secador (deviceType 3) con todos sus contadores.
static json secador_frame(int line, uint16_t timer, uint16_t ingreso)
{
    json m;
    m["deviceType"] = 3;
    m["lineID"] = line;
    m["alarms"] = 0;
    m["checksum"] = 42;
    m["gatewayTime"] = "2026-09-02T10:00:00-05:00";
    m["timer1Hz"] = timer;
    m["paradas_cantidad"] = 10;
    m["paradas_tempo_s"] = 20;
    m["ingreso_elevador_cantidad"] = ingreso;
    m["ingreso_elevador_tiempo_ds"] = 30;
    m["bancalino_l1_cantidad"] = 40;
    m["bancalino_l1_tiempo_ds"] = 50;
    m["bancalino_l2_cantidad"] = 60;
    m["bancalino_l2_tiempo_ds"] = 70;
    return m;
}

// Trama de salida_secador (deviceType 4).
static json mds_frame(int line, uint16_t timer, uint16_t metrica)
{
    json m;
    m["deviceType"] = 4;
    m["lineID"] = line;
    m["alarms"] = 0;
    m["checksum"] = 7;
    m["gatewayTime"] = "2026-09-02T10:00:00-05:00";
    m["timer1Hz"] = timer;
    m["parada_mds_cantidad"] = 3;
    m["parada_mds_tiempo_s"] = 4;
    m["metrica_mds_cantidad"] = metrica;
    m["metrica_mds_tiempo_ds"] = 5;
    return m;
}

TEST_CASE("reseed: primer mensaje y cambio de turno, una vez por procesador y línea") {
    testsup::pin_local_hour(10);          // 10:00 → S1 en modo 3
    REQUIRE(testsup::local_hour_now() == 10);
    reset_all_processor_states();

    auto secador = createProcessor(DeviceType::Entrada_secador);
    auto mds     = createProcessor(DeviceType::Salida_secador);
    const std::string pfx = "celima/punta_hermosa/planta/linea/";

    {
        testsup::CoutCapture cap;
        secador->process(secador_frame(1, 1000, 500), pfx, 3);
        mds->process(mds_frame(2, 1000, 500), pfx, 3);

        CHECK(cap.count({"[STATE] reseed", "proc=entrada_secador", "line=1",
                         "reason=first_message", "shift=1"}) == 1);
        CHECK(cap.count({"[STATE] reseed", "proc=salida_secador", "line=2",
                         "reason=first_message", "shift=1"}) == 1);
        CHECK(cap.count({"reason=shift_change"}) == 0);
    }

    testsup::pin_local_hour(15);          // 15:00 → S2 en modo 3
    REQUIRE(testsup::local_hour_now() == 15);
    {
        testsup::CoutCapture cap;
        secador->process(secador_frame(1, 1180, 560), pfx, 3);
        mds->process(mds_frame(2, 1180, 560), pfx, 3);

        // Exactamente uno por procesador y línea, no uno por campo.
        CHECK(cap.count({"[STATE] reseed", "proc=entrada_secador", "line=1",
                         "reason=shift_change", "shift_prev=1", "shift_new=2"}) == 1);
        CHECK(cap.count({"[STATE] reseed", "proc=salida_secador", "line=2",
                         "reason=shift_change", "shift_prev=1", "shift_new=2"}) == 1);
        CHECK(cap.count({"[STATE] reseed"}) == 2);
    }
}

TEST_CASE("delta_rejected y reanchor: tres rechazos consecutivos dejan rastro") {
    testsup::pin_local_hour(10);
    reset_all_processor_states();

    auto secador = createProcessor(DeviceType::Entrada_secador);
    const std::string pfx = "celima/punta_hermosa/planta/linea/";

    // Baseline: la primera trama solo siembra el estado.
    secador->process(secador_frame(3, 1000, 500), pfx, 3);

    testsup::CoutCapture cap;
    // Tres tramas con el mismo salto anómalo (> max_valid = 5000) en un solo
    // campo. prev_ref no avanza mientras se rechaza, así que las tres se
    // rechazan; la tercera fuerza el re-ancla.
    // timer1Hz avanza para que la deduplicación no descarte las tramas.
    for (int k = 1; k <= 3; ++k)
        secador->process(secador_frame(3, static_cast<uint16_t>(1000 + 180 * k), 20500), pfx, 3);

    CHECK(cap.count({"[STATE] delta_rejected", "proc=entrada_secador", "line=3",
                     "field=ingreso_elevador_cantidad"}) == 3);
    CHECK(cap.count({"[STATE] delta_rejected", "reject_count=1"}) == 1);
    CHECK(cap.count({"[STATE] delta_rejected", "reject_count=2"}) == 1);
    CHECK(cap.count({"[STATE] delta_rejected", "reject_count=3"}) == 1);
    CHECK(cap.count({"[STATE] reanchor", "proc=entrada_secador", "line=3",
                     "field=ingreso_elevador_cantidad"}) == 1);
    // Ningún otro campo se rechazó: el resto de contadores no se movió.
    CHECK(cap.count({"[STATE] delta_rejected"}) == 3);
    CHECK(cap.count({"[STATE] reanchor"}) == 1);

    // Tras el re-ancla, el siguiente delta legítimo vuelve a contar.
    testsup::CoutCapture cap2;
    secador->process(secador_frame(3, 1720, 20560), pfx, 3);
    CHECK(cap2.count({"[STATE] delta_rejected"}) == 0);
    CHECK(cap2.count({"[STATE] reanchor"}) == 0);
}

TEST_CASE("la rama de acumulación normal no loguea") {
    testsup::pin_local_hour(10);
    reset_all_processor_states();

    auto secador = createProcessor(DeviceType::Entrada_secador);
    const std::string pfx = "celima/punta_hermosa/planta/linea/";
    secador->process(secador_frame(4, 1000, 500), pfx, 3);   // siembra

    testsup::CoutCapture cap;
    for (int k = 1; k <= 10; ++k)
        secador->process(secador_frame(4, static_cast<uint16_t>(1000 + 180 * k),
                                       static_cast<uint16_t>(500 + 64 * k)), pfx, 3);
    CHECK(cap.count({"[STATE]"}) == 0);
}

TEST_CASE("shift_change_global deja rastro una sola vez por cambio") {
    detect_global_shift_change(1);        // baseline conocido

    testsup::CoutCapture cap;
    CHECK(detect_global_shift_change(2) == true);
    CHECK(cap.count({"[STATE] shift_change_global", "proc=global",
                     "shift_prev=1", "shift_new=2"}) == 1);

    CHECK(detect_global_shift_change(2) == false);
    CHECK(cap.count({"[STATE] shift_change_global"}) == 1);
}

TEST_CASE("calidad: el re-ancla del baseline deja rastro") {
    testsup::pin_local_hour(10);
    reset_all_processor_states();

    auto calidad = createProcessor(DeviceType::Calidad);
    const std::string pfx = "celima/punta_hermosa/planta/linea/";

    json m;
    m["deviceType"] = 8;
    m["lineID"] = 1;
    m["gatewayTime"] = "2026-09-02T10:00:00-05:00";
    m["boxesQ1"] = 100; m["boxesQ2"] = 50; m["boxesQ6"] = 10; m["totalBroken"] = 5;
    m["freshBoot"] = true;

    testsup::CoutCapture cap;
    auto pubs = calidad->process(m, pfx, 3);
    CHECK(pubs.empty());                  // el baseline no publica
    CHECK(cap.count({"[STATE] reanchor", "proc=calidad", "line=1",
                     "reason=fresh_boot"}) == 1);
}
