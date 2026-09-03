// Persistencia del estado de turno (D1) y lógica de restauración.
//
// "Reiniciar el proceso" en estas pruebas es reset_all_processor_states() +
// reset_global_shift_state() manteniendo el store: eso es exactamente lo que
// pierde un reinicio real (los static) y lo que sobrevive (la base).
#include "doctest.h"
#include "support.hpp"

using json = nlohmann::json;
using testsup::count_lines;
using testsup::capture_streams;

static const std::string kPfx = "celima/punta_hermosa/planta/linea/";

// Total de turno de un campo de entrada_secador, tal como se publica.
static int64_t turno_ingreso(const std::vector<Publication>& pubs)
{
    REQUIRE(pubs.size() == 2);
    return json::parse(pubs[1].payload)["ingreso_elevador_turno"].get<int64_t>();
}

namespace {

// Un "arranque": pierde los static, conserva el store.
void simulate_restart()
{
    reset_all_processor_states();
    reset_global_shift_state();
}

celima::MemoryStateStore* install_memory_store()
{
    auto store = std::make_unique<celima::MemoryStateStore>();
    auto* raw = store.get();
    celima::set_state_store(std::move(store));
    return raw;
}

} // namespace

TEST_CASE("reinicio limpio a mitad de turno: el acumulador continúa") {
    testsup::pin_local_hour(10);
    testsup::rates_for_tests();
    install_memory_store();
    simulate_restart();

    {
        auto proc = createProcessor(DeviceType::Entrada_secador);
        proc->process(testsup::make_frame(3, 1, 0), kPfx, 3);          // siembra
        for (int tick = 1; tick <= 5; ++tick)
            proc->process(testsup::make_frame(3, 1, tick), kPfx, 3);
        // 5 intervalos * 64 = 320
        CHECK(turno_ingreso(proc->process(testsup::make_frame(3, 1, 6), kPfx, 3)) == 384);
    }

    simulate_restart();   // <-- caída y arranque

    auto proc = createProcessor(DeviceType::Entrada_secador);
    const std::string log = capture_streams([&] {
        proc->process(testsup::make_frame(3, 1, 7), kPfx, 3);
    });
    CHECK(count_lines(log, {"[STATE] restored", "proc=entrada_secador", "line=1"}) == 1);
    CHECK(count_lines(log, {"[STATE] reseed"}) == 0);

    // Continúa donde estaba: 384 + 64 (la trama del restore) + 64.
    CHECK(turno_ingreso(proc->process(testsup::make_frame(3, 1, 8), kPfx, 3)) == 512);

    celima::set_state_store(nullptr);
}

TEST_CASE("el primer mensaje tras arrancar no pone los acumuladores a cero") {
    // La trampa del apartado F: detect_global_shift_change() devolvía true en
    // el primer mensaje y su reset_all_processor_states() borraba lo restaurado.
    testsup::pin_local_hour(10);
    testsup::rates_for_tests();
    install_memory_store();
    simulate_restart();

    {
        auto proc = createProcessor(DeviceType::Entrada_secador);
        proc->process(testsup::make_frame(3, 2, 0), kPfx, 3);
        proc->process(testsup::make_frame(3, 2, 1), kPfx, 3);
        CHECK(turno_ingreso(proc->process(testsup::make_frame(3, 2, 2), kPfx, 3)) == 128);
    }

    simulate_restart();

    // Enrutado completo, igual que MqttApp: detect_global_shift_change() y su
    // reset son parte del camino que se está probando.
    const int shiftNum = static_cast<int>(current_shift_localtime(3));
    const bool changed = detect_global_shift_change(shiftNum);
    CHECK_FALSE(changed);                 // primer mensaje ≠ cambio de turno
    if (changed) reset_all_processor_states();

    auto proc = createProcessor(DeviceType::Entrada_secador);
    CHECK(turno_ingreso(proc->process(testsup::make_frame(3, 2, 3), kPfx, 3)) == 192);

    celima::set_state_store(nullptr);
}

TEST_CASE("se guarda en cada mensaje aceptado, no solo al salir") {
    // Es la propiedad que hace que un SIGKILL no pueda costar más de un
    // mensaje: el estado en disco va siempre un mensaje por detrás como mucho.
    testsup::pin_local_hour(10);
    testsup::rates_for_tests();
    auto* store = install_memory_store();
    simulate_restart();

    auto proc = createProcessor(DeviceType::Entrada_secador);
    for (int tick = 0; tick <= 4; ++tick)
        proc->process(testsup::make_frame(3, 3, tick), kPfx, 3);
    CHECK(store->save_count() == 5);

    // Una trama repetida no se acepta y por tanto no guarda.
    proc->process(testsup::make_frame(3, 3, 4), kPfx, 3);
    CHECK(store->save_count() == 5);

    celima::set_state_store(nullptr);
}

TEST_CASE("hueco largo dentro del turno: recupera y marca los no observados") {
    testsup::pin_local_hour(10);
    testsup::rates_for_tests();
    install_memory_store();
    simulate_restart();

    {
        auto proc = createProcessor(DeviceType::Entrada_secador);
        proc->process(testsup::make_frame(3, 4, 0), kPfx, 3);
        proc->process(testsup::make_frame(3, 4, 1), kPfx, 3);      // acc = 64
    }
    simulate_restart();

    // Vuelve 5 h después (tick 101), con la producción de esas 5 h en el raw.
    json m = testsup::make_frame(3, 4, 101);
    m["ingreso_elevador_cantidad"] = 1000 + 64 + 6000;

    auto proc = createProcessor(DeviceType::Entrada_secador);
    std::vector<Publication> pubs;
    const std::string log = capture_streams([&] { pubs = proc->process(m, kPfx, 3); });

    CHECK(count_lines(log, {"[STATE] gap", "proc=entrada_secador", "line=4",
                            "reason=restart"}) == 1);
    CHECK(count_lines(log, {"[STATE] delta_rejected",
                            "field=ingreso_elevador_cantidad"}) == 0);
    CHECK(turno_ingreso(pubs) == 64 + 6000);       // el hueco se recupera

    celima::set_state_store(nullptr);
}

TEST_CASE("hueco que excede el módulo: re-siembra sin sumar") {
    testsup::pin_local_hour(10);
    testsup::rates_for_tests();
    install_memory_store();
    simulate_restart();

    {
        auto proc = createProcessor(DeviceType::Entrada_secador);
        proc->process(testsup::make_frame(3, 5, 0), kPfx, 3);
        proc->process(testsup::make_frame(3, 5, 1), kPfx, 3);      // acc = 64
    }
    simulate_restart();

    // 15 h después: la cota alcanzaría el módulo de 65.536, así que el delta es
    // ambiguo por construcción y no se intenta recuperar.
    json m = testsup::make_frame(3, 5, 300);
    m["ingreso_elevador_cantidad"] = 40000;

    auto proc = createProcessor(DeviceType::Entrada_secador);
    std::vector<Publication> pubs;
    const std::string log = capture_streams([&] { pubs = proc->process(m, kPfx, 3); });

    CHECK(count_lines(log, {"[STATE] delta_rejected", "reason=ambiguous_module"}) >= 1);
    CHECK(turno_ingreso(pubs) == 64);              // se conserva el total, no se infla

    celima::set_state_store(nullptr);
}

TEST_CASE("cambio de turno durante el hueco: acumuladores a cero, sin arrastre") {
    testsup::pin_local_hour(10);                   // S1 en modo 3
    testsup::rates_for_tests();
    install_memory_store();
    simulate_restart();

    {
        auto proc = createProcessor(DeviceType::Entrada_secador);
        proc->process(testsup::make_frame(3, 6, 0), kPfx, 3);
        proc->process(testsup::make_frame(3, 6, 1), kPfx, 3);      // acc = 64, turno 1
    }
    simulate_restart();
    testsup::pin_local_hour(15);                   // S2: el turno cambió estando caído

    json m = testsup::make_frame(3, 6, 120);
    m["ingreso_elevador_cantidad"] = 1000 + 5000;

    auto proc = createProcessor(DeviceType::Entrada_secador);
    std::vector<Publication> pubs;
    const std::string log = capture_streams([&] { pubs = proc->process(m, kPfx, 3); });

    CHECK(count_lines(log, {"[STATE] reseed", "proc=entrada_secador", "line=6",
                            "reason=shift_change_across_restart",
                            "shift_prev=1", "shift_new=2"}) == 1);
    CHECK(count_lines(log, {"[STATE] reseed"}) == 1);   // una sola traza, no dos
    CHECK(turno_ingreso(pubs) == 0);                   // turno nuevo, sin arrastre

    celima::set_state_store(nullptr);
}

TEST_CASE("estado con versión de esquema desconocida: se ignora y re-siembra") {
    testsup::pin_local_hour(10);
    testsup::rates_for_tests();
    auto* store = install_memory_store();
    simulate_restart();

    {
        auto proc = createProcessor(DeviceType::Entrada_secador);
        proc->process(testsup::make_frame(3, 7, 0), kPfx, 3);
        proc->process(testsup::make_frame(3, 7, 1), kPfx, 3);
    }
    simulate_restart();

    // Reescribir la fila con una versión futura.
    celima::StoredState row;
    REQUIRE(store->load("entrada_secador", 7, row));
    row.state["v"] = celima::kStateSchemaVersion + 1;
    REQUIRE(store->save("entrada_secador", 7, row.shift, row.updated_at, row.state));

    // MemoryStateStore no filtra por versión (eso lo hace el store SQLite), así
    // que aquí se comprueba en el lado que importa: la ruta real con SQLite.
    celima::set_state_store(nullptr);

    const std::string db = std::string(testsup::tmpdir()) + "/celima_state_v_test.db";
    std::remove(db.c_str());
    {
        auto sq = celima::make_sqlite_state_store(db);
        REQUIRE(sq != nullptr);
        json bad;
        bad["v"] = celima::kStateSchemaVersion + 1;
        bad["acc_ingreso_elevador_cantidad"] = 999999;
        REQUIRE(sq->save("entrada_secador", 8, 1, testsup::kBaseEpoch, bad));

        celima::StoredState out;
        const std::string log = capture_streams([&] {
            CHECK_FALSE(sq->load("entrada_secador", 8, out));
        });
        CHECK(count_lines(log, {"[STATE] stored_state_ignored", "reason=schema_version"}) == 1);
    }
    std::remove(db.c_str());
}

TEST_CASE("SQLite: el estado sobrevive a cerrar y reabrir la base") {
    testsup::pin_local_hour(10);
    testsup::rates_for_tests();

    const std::string db = std::string(testsup::tmpdir()) + "/celima_state_rt.db";
    std::remove(db.c_str());

    {
        auto sq = celima::make_sqlite_state_store(db);
        REQUIRE(sq != nullptr);
        celima::set_state_store(std::move(sq));
        simulate_restart();

        auto proc = createProcessor(DeviceType::Entrada_secador);
        proc->process(testsup::make_frame(3, 9, 0), kPfx, 3);
        proc->process(testsup::make_frame(3, 9, 1), kPfx, 3);
        CHECK(turno_ingreso(proc->process(testsup::make_frame(3, 9, 2), kPfx, 3)) == 128);
        celima::set_state_store(nullptr);        // cierra la base
    }

    {
        auto sq = celima::make_sqlite_state_store(db);
        REQUIRE(sq != nullptr);
        celima::set_state_store(std::move(sq));
        simulate_restart();

        auto proc = createProcessor(DeviceType::Entrada_secador);
        CHECK(turno_ingreso(proc->process(testsup::make_frame(3, 9, 3), kPfx, 3)) == 192);
        celima::set_state_store(nullptr);
    }
    std::remove(db.c_str());
}

TEST_CASE("base corrupta o no abrible: el servicio se comporta como hoy") {
    testsup::pin_local_hour(10);
    testsup::rates_for_tests();

    // Ruta imposible.
    std::string log = capture_streams([&] {
        CHECK(celima::make_sqlite_state_store("/proc/no/se/puede/state.db") == nullptr);
    });
    CHECK(count_lines(log, {"[CONFIG] state db not usable"}) == 1);

    // Archivo que no es una base SQLite.
    const std::string bad = std::string(testsup::tmpdir()) + "/celima_not_a_db.db";
    REQUIRE(testsup::write_file(bad, "esto no es una base de datos"));
    log = capture_streams([&] {
        CHECK(celima::make_sqlite_state_store(bad) == nullptr);
    });
    CHECK(count_lines(log, {"[CONFIG] state db not usable"}) == 1);
    std::remove(bad.c_str());

    // Sin store, los acumuladores funcionan en memoria como antes de PR 2.
    celima::set_state_store(nullptr);
    simulate_restart();
    auto proc = createProcessor(DeviceType::Entrada_secador);
    proc->process(testsup::make_frame(3, 10, 0), kPfx, 3);
    CHECK(turno_ingreso(proc->process(testsup::make_frame(3, 10, 1), kPfx, 3)) == 64);
}

TEST_CASE("calidad también persiste: es tan vulnerable a D1 como las demás") {
    testsup::pin_local_hour(10);
    testsup::rates_for_tests();
    install_memory_store();
    simulate_restart();

    auto quebrados = [](const std::vector<Publication>& pubs) {
        REQUIRE(pubs.size() == 1);
        return json::parse(pubs[0].payload)["quebrados"].get<int64_t>();
    };

    {
        auto proc = createProcessor(DeviceType::Calidad);
        proc->process(testsup::make_frame(8, 1, 0), kPfx, 3);      // ancla baseline
        proc->process(testsup::make_frame(8, 1, 1), kPfx, 3);
        CHECK(quebrados(proc->process(testsup::make_frame(8, 1, 2), kPfx, 3)) == 128);
    }
    simulate_restart();

    auto proc = createProcessor(DeviceType::Calidad);
    CHECK(quebrados(proc->process(testsup::make_frame(8, 1, 3), kPfx, 3)) == 192);

    celima::set_state_store(nullptr);
}

TEST_CASE("CELIMA_STATE_PERSISTENCE=0 desactiva carga y guardado") {
    setenv("CELIMA_STATE_PERSISTENCE", "0", 1);
    const std::string log = capture_streams([&] { celima::init_state_store_from_env(); });
    CHECK(count_lines(log, {"[CONFIG] persistencia de estado DESACTIVADA"}) == 1);
    CHECK(celima::state_store() == nullptr);
    unsetenv("CELIMA_STATE_PERSISTENCE");
}

TEST_CASE("contador que retrocede a 0 con hueco corto: se detecta, no suma") {
    testsup::pin_local_hour(10);
    testsup::rates_for_tests();
    install_memory_store();
    simulate_restart();

    auto proc = createProcessor(DeviceType::Entrada_secador);
    proc->process(testsup::make_frame(3, 11, 0), kPfx, 3);
    proc->process(testsup::make_frame(3, 11, 1), kPfx, 3);         // acc = 64

    // El PLC volvió a 0 (no retentivo) y el hueco es corto: la resta sin
    // máscara da ~64.472, muy por encima de la cota, así que se descarta.
    json m = testsup::make_frame(3, 11, 2);
    m["ingreso_elevador_cantidad"] = 0;

    std::vector<Publication> pubs;
    const std::string log = capture_streams([&] { pubs = proc->process(m, kPfx, 3); });

    CHECK(count_lines(log, {"[STATE] delta_rejected", "field=ingreso_elevador_cantidad",
                            "reason=over_bound"}) == 1);
    CHECK(turno_ingreso(pubs) == 64);              // ni suma 64.472 ni pierde lo previo

    celima::set_state_store(nullptr);
}

TEST_CASE("marcador de turno incompleto: apagado por defecto, publicable a demanda") {
    testsup::pin_local_hour(10);
    testsup::rates_for_tests();
    install_memory_store();
    simulate_restart();

    {
        auto proc = createProcessor(DeviceType::Entrada_secador);
        proc->process(testsup::make_frame(3, 12, 0), kPfx, 3);
        proc->process(testsup::make_frame(3, 12, 1), kPfx, 3);
    }
    simulate_restart();

    // Vuelve 5 h después: el turno queda incompleto en esas ~5 h.
    json m = testsup::make_frame(3, 12, 101);

    auto proc = createProcessor(DeviceType::Entrada_secador);

    set_incomplete_shift_marker_for_tests(false);
    json prod = json::parse(proc->process(m, kPfx, 3)[1].payload);
    CHECK_FALSE(prod.contains("turno_segundos_no_observados"));

    set_incomplete_shift_marker_for_tests(true);
    prod = json::parse(proc->process(testsup::make_frame(3, 12, 102), kPfx, 3)[1].payload);
    REQUIRE(prod.contains("turno_segundos_no_observados"));
    // 100 intervalos de 180 s entre la última trama vista y el regreso.
    CHECK(prod["turno_segundos_no_observados"].get<int64_t>() == 100 * 180);

    clear_incomplete_shift_marker_override();
    celima::set_state_store(nullptr);
}
