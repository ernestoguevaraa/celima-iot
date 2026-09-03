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

TEST_CASE("una retransmisión nunca suma dos veces, en ningún procesador") {
    // Los Arduino reintentan hasta 3 veces si no les llega el ACK del gateway
    // LoRaWAN, y esos ACK se pierden de vuelta, así que la trama repetida es un
    // hecho cotidiano en planta. El gateway re-sella gatewayTime al recibirla,
    // de modo que la copia NO llega con la misma marca de tiempo: la
    // deduplicación por ventana (prensas) y la de calidad (compara gatewayTime)
    // pueden no reconocerla. Lo que no puede pasar bajo ninguna circunstancia
    // es que el total de turno crezca por una retransmisión.
    testsup::pin_local_hour(10);
    testsup::rates_for_tests();

    for (int dt = 1; dt <= 8; ++dt) {
        CAPTURE(dt);
        reset_all_processor_states();
        auto proc = createProcessor(*deviceTypeFromInt(dt));

        proc->process(testsup::make_frame(dt, 1, 0), kPfx, 3);   // siembra
        auto pubs = proc->process(testsup::make_frame(dt, 1, 1), kPfx, 3);
        REQUIRE(!pubs.empty());
        const json antes = json::parse(pubs.back().payload);

        // Misma trama —contadores idénticos— con gatewayTime 30 s más tarde,
        // que es el reintento tal como lo ve el servicio.
        json retry = testsup::make_frame(dt, 1, 1);
        retry["gatewayTime"] = testsup::iso_utc(testsup::kBaseEpoch + testsup::kInterval + 30);

        auto pubs2 = proc->process(retry, kPfx, 3);
        if (pubs2.empty()) continue;        // descartada por dedup: caso ideal

        // Aceptada: entonces todos los totales de turno deben seguir igual.
        const json despues = json::parse(pubs2.back().payload);
        for (const auto& [key, val] : antes.items()) {
            if (key.find("_turno") == std::string::npos &&
                key != "extra_c1" && key != "extra_c2" &&
                key != "comercial" && key != "quebrados")
                continue;
            CAPTURE(key);
            CHECK_MESSAGE(despues[key] == val,
                          "deviceType " << dt << ": la retransmisión movió " << key);
        }
    }
}

TEST_CASE("las tramas que el decoder no pudo interpretar no publican nada") {
    // Los Arduino mandan un ping de 1 byte para que el gateway no los
    // desconecte; su decoder intenta parsearlo y emite un _error sin
    // deviceType. Son el 10,7% del tráfico medido en 24 h, y cada uno publicaba
    // dos mensajes de relleno en tópicos con doble barra.
    auto def = createDefaultProcessor();

    json ping;
    ping["_error"] = "11 bytes requeridos, recibidos: 1";
    ping["applicationID"] = 1;
    ping["devEUI"] = "a8610a35392a7f05";
    ping["deviceName"] = "e6-cal";
    ping["gatewayTime"] = "2026-09-02T14:27:37-05:00";

    std::vector<Publication> pubs;
    std::string log = capture_streams([&] { pubs = def->process(ping, kPfx, 3); });
    CHECK(pubs.empty());
    CHECK(count_lines(log, {"[STATE] frame_ignored", "reason=decoder_error",
                            "devEUI=a8610a35392a7f05"}) == 1);

    // El mismo error no se vuelve a registrar: son ~1.200 pings al día.
    log = capture_streams([&] { pubs = def->process(ping, kPfx, 3); });
    CHECK(pubs.empty());
    CHECK(log.empty());

    // Un error distinto del decoder sí se ve: es señal de algo nuevo.
    json otro = ping;
    otro["_error"] = "checksum inválido";
    log = capture_streams([&] { pubs = def->process(otro, kPfx, 3); });
    CHECK(pubs.empty());
    CHECK(count_lines(log, {"[STATE] frame_ignored", "err=\"checksum inválido\""}) == 1);

    // Un deviceType desconocido SIN _error mantiene el comportamiento anterior:
    // esto no cambia el enrutado, solo descarta lo que no se pudo decodificar.
    json desconocido;
    desconocido["deviceType"] = 99;
    desconocido["lineID"] = 1;
    desconocido["gatewayTime"] = "2026-09-02T14:27:37-05:00";
    CHECK(def->process(desconocido, kPfx, 3).size() == 2);
}
