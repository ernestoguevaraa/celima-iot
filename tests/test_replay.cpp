// Test de replay determinista — criterio de aceptación 1 (payloads idénticos).
//
// Alimenta los procesadores con la secuencia grabada de payloads celima/data y
// compara el vector de publicaciones contra un golden byte a byte.
//
// Regenerar el golden (solo cuando el cambio de payload sea intencionado, o al
// sustituir el fixture por tráfico real de planta):
//
//     make test GOLDEN_OUT=tests/data/celima_data_replay.golden
//
// El golden que está en el repo se generó con el código de main@0ac7a9c, es
// decir ANTES de este PR: es lo que demuestra que la instrumentación no cambió
// ningún valor publicado.
#include "doctest.h"
#include "support.hpp"

#include <cstdlib>
#include <map>
#include "TimeUtils.hpp"

static const char* kReplay = "tests/data/celima_data_replay.jsonl";
static const char* kGolden = "tests/data/celima_data_replay.golden";

TEST_CASE("replay: las publicaciones son idénticas al golden") {
    // Turno fijado a las 10:00 locales: dentro de S1 tanto en modo 2 como en
    // modo 3, y lejos de cualquier frontera, así que ni el turno ni los
    // acumuladores dependen de cuándo se ejecute la prueba.
    testsup::pin_local_hour(10);
    REQUIRE(testsup::local_hour_now() == 10);

    reset_all_processor_states();
    const std::string dump = testsup::replay_file(kReplay, 3);
    REQUIRE_MESSAGE(!dump.empty(), "fixture vacío o ilegible: " << kReplay);

    if (const char* out = std::getenv("GOLDEN_OUT")) {
        REQUIRE(testsup::write_file(out, dump));
        MESSAGE("golden escrito en " << std::string(out));
        return;
    }

    const std::string golden = testsup::read_file(kGolden);
    REQUIRE_MESSAGE(!golden.empty(), "golden ausente: " << kGolden);

    if (dump != golden) {
        // Primera línea divergente, para que el fallo sea legible.
        std::istringstream a(dump), b(golden);
        std::string la, lb;
        int n = 1;
        while (std::getline(a, la)) {
            if (!std::getline(b, lb)) { FAIL("el replay emitió más líneas que el golden, desde la " << n); }
            if (la != lb) { FAIL("divergencia en la línea " << n << "\n  replay: " << la << "\n  golden: " << lb); }
            ++n;
        }
        if (std::getline(b, lb)) FAIL("el replay emitió menos líneas que el golden (falta la " << n << ")");
    }
    CHECK(dump == golden);
}

TEST_CASE("replay: el fixture cubre los 8 deviceType y las 4 líneas") {
    const std::string raw = testsup::read_file(kReplay);
    REQUIRE(!raw.empty());
    for (int dt = 1; dt <= 8; ++dt)
        CHECK_MESSAGE(raw.find("\"deviceType\":" + std::to_string(dt)) != std::string::npos,
                      "falta deviceType " << dt << " en el fixture");
    for (int line = 1; line <= 4; ++line)
        CHECK_MESSAGE(raw.find("\"lineID\":" + std::to_string(line)) != std::string::npos,
                      "falta lineID " << line << " en el fixture");
}

TEST_CASE("el fixture es físicamente coherente") {
    // timer1Hz es un contador libre a 1 Hz, así que su delta tiene que ser
    // exactamente los segundos que separan las dos tramas. Antes avanzaba al
    // azar entre 0 y 360 por trama: superaba la cota de la familia tiempo_s y
    // pasaba solo porque manda el techo mínimo de max_valid, tapando la
    // comprobación. Y el intervalo depende de la clase: entrada_horno publica
    // cada ~120 s, el resto cada ~180 s.
    std::ifstream in(kReplay);
    REQUIRE(in.good());

    struct Prev { int64_t epoch; int timer; bool has_timer; };
    std::map<std::pair<int, int>, Prev> prev;
    std::string line;
    int checked = 0, dups = 0;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        std::string err;
        auto j = jsonu::parse(line, err);
        REQUIRE(j);

        const int dt   = j->value("deviceType", 0);
        const int lid  = j->value("lineID", 0);
        const auto ep  = device_epoch_s(*j);
        REQUIRE_MESSAGE(ep.has_value(), "toda trama debe llevar gatewayTime parseable");

        const bool has_timer = j->contains("timer1Hz");
        const int  timer     = j->value("timer1Hz", 0);
        const auto key       = std::make_pair(lid, dt);

        const auto it = prev.find(key);
        if (it != prev.end()) {
            const int64_t d_epoch = *ep - it->second.epoch;
            if (d_epoch == 0) { ++dups; continue; }   // retransmisión del fixture

            CAPTURE(dt); CAPTURE(lid); CAPTURE(d_epoch);
            // Intervalo nominal de su clase. En planta el real es 187 s (127 en
            // entrada_horno, 180 en calidad): el reloj del Arduino corre algo
            // largo. La tolerancia cubre eso sin dejar pasar un hueco.
            CHECK(std::abs(d_epoch - testsup::frame_interval(dt)) <= 15);

            if (has_timer && it->second.has_timer) {
                const int d_timer = static_cast<uint16_t>(timer - it->second.timer);
                // 1 tick por segundo, con tolerancia: en tráfico real de planta
                // el reloj del Arduino y el sello del gateway discrepan ±1 s de
                // forma habitual y hasta ±6 s ocasionalmente (medido sobre 24 h,
                // 8.000 pares). La tolerancia deja pasar eso y sigue atrapando
                // un contador que avanza de forma arbitraria.
                CHECK(std::abs(d_timer - static_cast<int>(d_epoch)) <= 10);
                ++checked;
            }
        }
        prev[key] = Prev{*ep, timer, has_timer};
    }
    CHECK(checked > 300);                            // no pasar en vacío
    CHECK(dups == 1);                                // la trama repetida sigue ahí
}
