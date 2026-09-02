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
