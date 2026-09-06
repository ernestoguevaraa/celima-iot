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
    // 07:00 + 5 h = 12:03, dentro del mismo turno (S1 = 06–14 en modo 3). Si se
    // fijara a las 10:00 el hueco cruzaría las 14:00 y sería el caso 2.
    testsup::pin_local_hour(7);
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
    // El módulo solo es alcanzable dentro de un mismo turno para los contadores
    // de tiempo puros: a 10 ticks/s la cota llega a 65.536 en 1,2 h. Un contador
    // de evento necesitaría 12 h, y a esas alturas ya cambió el turno, que es
    // otro caso (el 2). 07:00 + 2 h = 09:00, mismo turno.
    //
    // Se usa salida_horno/sentido_escolha_tiempo porque es el único tiempo_ds
    // que queda: los demás migraron a LatchedTime en PR 3 y se acotan contra el
    // reloj del turno, no contra la tasa.
    testsup::pin_local_hour(7);
    testsup::rates_for_tests();
    install_memory_store();
    simulate_restart();

    {
        auto proc = createProcessor(DeviceType::Salida_horno);
        proc->process(testsup::make_frame(7, 5, 0), kPfx, 3);
        proc->process(testsup::make_frame(7, 5, 1), kPfx, 3);      // acc = 64
    }
    simulate_restart();

    json m = testsup::make_frame(7, 5, 41);        // +2 h
    m["sentido_escolha_tiempo"] = 40000;           // familia tiempo_ds
    // Los dos contadores que vigila la detección de picos se dejan quietos: si
    // saltan a la vez, salida horno descarta la trama entera y no se llega a la
    // aritmética que se quiere probar.
    m["metrica_mdf_ciclos"] = 1000 + 64;
    m["barreira1_cantidad"] = 1000 + 64;

    auto proc = createProcessor(DeviceType::Salida_horno);
    std::vector<Publication> pubs;
    const std::string log = capture_streams([&] { pubs = proc->process(m, kPfx, 3); });

    CHECK(count_lines(log, {"[STATE] delta_rejected", "family=tiempo_ds",
                            "reason=ambiguous_module"}) >= 1);
    // Conserva lo restaurado pero NO suma el delta del hueco, que es ambiguo:
    // se queda en los 64 que traía de antes del corte.
    CHECK(json::parse(pubs[1].payload)["cambioSentidoTotal_turno"] == 64);

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
        REQUIRE(sq->save("entrada_secador", 8, 1, testsup::base_epoch(), bad));

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
    testsup::rates_for_tests(1350.0);
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
    // El hueco de 5 h cruzó la frontera de las 14:00, así que el turno en curso
    // empezó ahí y lo no observado es desde su arranque hasta la primera trama
    // —no el hueco completo, que incluye tiempo del turno anterior y podía
    // acabar superando la duración del turno.
    const int64_t no_obs = prod["turno_segundos_no_observados"].get<int64_t>();
    CHECK(no_obs > 0);
    CHECK(no_obs < 8 * 3600);                    // nunca más que el turno
    CHECK(no_obs == 3600 + 101 * 180 - 5 * 3600 + 0);

    clear_incomplete_shift_marker_override();
    celima::set_state_store(nullptr);
}

TEST_CASE("un hueco que abarca un turno completo no arrastra el total anterior") {
    // El turno se identificaba solo por su número (1/2/3), sin fecha. Un hueco
    // de ~24 h cae en el MISMO número de turno, así que la restauración lo
    // tomaba por "mismo turno" y devolvía los acumuladores del día anterior
    // como total del turno de hoy. Los huecos documentados de D3 son de 26 h y
    // 31 h: es el caso real, no uno teórico.
    testsup::pin_local_hour(10);
    testsup::rates_for_tests();
    install_memory_store();
    simulate_restart();

    {
        auto proc = createProcessor(DeviceType::Entrada_secador);
        proc->process(testsup::make_frame(3, 20, 0), kPfx, 3);
        for (int tick = 1; tick <= 20; ++tick)
            proc->process(testsup::make_frame(3, 20, tick), kPfx, 3);
        // 20 intervalos * 64 = 1280 acumulados en el turno de ayer
        CHECK(turno_ingreso(proc->process(testsup::make_frame(3, 20, 21), kPfx, 3)) == 1344);
    }
    simulate_restart();

    // Vuelve 24 h después: mismo número de turno, otro turno.
    auto proc = createProcessor(DeviceType::Entrada_secador);
    const std::string log = capture_streams([&] {
        proc->process(testsup::make_frame(3, 20, 21 + 480), kPfx, 3);   // +24 h
    });
    CHECK(count_lines(log, {"[STATE] reseed", "reason=shift_change_across_restart"}) == 1);
    // La trama que re-siembra publica 0, y la siguiente suma un intervalo: el
    // total de ayer no vuelve.
    CHECK(turno_ingreso(proc->process(testsup::make_frame(3, 20, 22 + 480), kPfx, 3)) == 64);

    celima::set_state_store(nullptr);
}

TEST_CASE("calidad no arrastra el delta del hueco al turno nuevo") {
    // OJO con la tasa: con la de los tests (3.600 u/h) un hueco de 14 h da una
    // cota que alcanza el módulo, el delta sale ambiguo y el fallo queda
    // tapado. Con la tasa real de planta para calidad (1.350 u/h) la cota es
    // 28.350, el delta del corte se acepta, y se ve el arrastre.
    // Los 7 procesadores de máquina dejan el estado sin inicializar en el caso
    // 2, así que la rama normal lo re-siembra con la trama actual y el delta
    // del hueco se descarta. Calidad restauraba su RawTrack con baseline_set
    // puesto, de modo que el delta de todo el corte se acreditaba al turno
    // nuevo. Debe comportarse igual que los demás.
    testsup::pin_local_hour(10);
    testsup::rates_for_tests(1350.0);
    install_memory_store();
    simulate_restart();

    auto quebrados = [](const std::vector<Publication>& pubs) {
        REQUIRE(pubs.size() == 1);
        return json::parse(pubs[0].payload)["quebrados"].get<int64_t>();
    };

    {
        auto proc = createProcessor(DeviceType::Calidad);
        proc->process(testsup::make_frame(8, 21, 0), kPfx, 3);      // ancla
        proc->process(testsup::make_frame(8, 21, 1), kPfx, 3);
        CHECK(quebrados(proc->process(testsup::make_frame(8, 21, 2), kPfx, 3)) == 128);
    }
    simulate_restart();
    testsup::pin_local_hour(15);            // el turno cambió durante el corte

    auto proc = createProcessor(DeviceType::Calidad);
    std::vector<Publication> pubs;
    const std::string log = capture_streams([&] {
        pubs = proc->process(testsup::make_frame(8, 21, 280), kPfx, 3);   // +14 h
    });
    CHECK(count_lines(log, {"[STATE] reseed", "proc=calidad",
                            "reason=shift_change_across_restart"}) == 1);
    // Turno nuevo: cero, y sin el delta del corte.
    if (!pubs.empty()) CHECK(quebrados(pubs) == 0);
    CHECK(quebrados(proc->process(testsup::make_frame(8, 21, 281), kPfx, 3)) == 64);

    celima::set_state_store(nullptr);
}

TEST_CASE("los acumuladores de nivel sobreviven al reinicio") {
    // Sin esto se reintroduce D1 en los campos nuevos: el turno los pondría a
    // cero en cada reinicio mientras numero_grades_turno continúa.
    testsup::pin_local_hour(10);
    testsup::rates_for_tests();
    install_memory_store();
    simulate_restart();

    auto nivel_de = [](const std::vector<Publication>& pubs, const char* campo) {
        REQUIRE(pubs.size() == 2);
        return json::parse(pubs[1].payload)[campo].get<int64_t>();
    };

    {
        auto horno = createProcessor(DeviceType::Entrada_horno);
        const std::vector<int> nivel = {10, 14, 9, 0};   // +4, -5, -9 y un vacío
        std::vector<Publication> pubs;
        for (size_t i = 0; i < nivel.size(); ++i) {
            json m = testsup::make_frame(6, 7, static_cast<int>(i));
            m["numero_grades"] = nivel[i];
            pubs = horno->process(m, kPfx, 3);
        }
        CHECK(nivel_de(pubs, "numero_grades_turno") == 4);
        CHECK(nivel_de(pubs, "numero_grades_bajadas_turno") == 5 + 9);
        CHECK(nivel_de(pubs, "buffer_vacio_turno_s") == 120);
    }

    simulate_restart();   // <-- caída y arranque

    auto horno = createProcessor(DeviceType::Entrada_horno);
    json m = testsup::make_frame(6, 7, 4);
    m["numero_grades"] = 6;                 // +6 desde el 0 de antes del corte
    const auto pubs = horno->process(m, kPfx, 3);

    CHECK(nivel_de(pubs, "numero_grades_turno") == 4 + 6);
    CHECK(nivel_de(pubs, "numero_grades_bajadas_turno") == 14);   // restaurado
    CHECK(nivel_de(pubs, "buffer_vacio_turno_s") == 120);         // restaurado

    celima::set_state_store(nullptr);
}

TEST_CASE("LatchedTime tras una restauración: el acumulado viene de la base") {
    // La cota latcheada compara acc_current contra el reloj del turno, así que
    // depende de que el acumulado restaurado llegue a diff_counter_scaled(). Si
    // el call site pasara 0, la cota sería demasiado laxa justo tras un
    // reinicio, que es cuando más importa.
    testsup::pin_local_hour(10);
    testsup::rates_for_tests();
    install_memory_store();
    simulate_restart();

    auto banc = [](const std::vector<Publication>& pubs) {
        REQUIRE(pubs.size() == 2);
        return json::parse(pubs[1].payload)["bancalino_l1_tiempo_turno_ds"].get<int64_t>();
    };

    {
        auto proc = createProcessor(DeviceType::Entrada_secador);
        json m0 = testsup::make_frame(3, 30, 0);
        m0["bancalino_l1_tiempo_ds"] = 1000;
        proc->process(m0, kPfx, 3);
        json m1 = testsup::make_frame(3, 30, 1);
        m1["bancalino_l1_tiempo_ds"] = 1000 + 1870;      // un intervalo de reloj
        CHECK(banc(proc->process(m1, kPfx, 3)) == 1870);
    }

    simulate_restart();   // <-- caída y arranque, mismo turno

    auto proc = createProcessor(DeviceType::Entrada_secador);
    json m2 = testsup::make_frame(3, 30, 2);
    m2["bancalino_l1_tiempo_ds"] = 1000 + 1870 + 1870;
    std::vector<Publication> pubs;
    const std::string log = capture_streams([&] { pubs = proc->process(m2, kPfx, 3); });

    CHECK(count_lines(log, {"[STATE] restored", "proc=entrada_secador"}) == 1);
    CHECK(count_lines(log, {"delta_rejected", "field=bancalino_l1_tiempo"}) == 0);
    CHECK(banc(pubs) == 1870 * 2);                       // continúa, no reinicia

    // La cota sigue viva tras restaurar, pero es un guardarraíl contra un
    // acumulado desbocado, no un filtro de deltas: al principio del turno el
    // término de arrastre admite casi cualquier salto. Aquí se comprueba lo
    // primero, que es lo que la cota sí garantiza.
    CHECK(count_lines(log, {"no_shift_elapsed"}) == 0);

    celima::set_state_store(nullptr);
}
