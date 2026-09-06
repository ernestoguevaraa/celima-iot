// Cota de plausibilidad escalada por tiempo (D2) y configuración de tasas.
//
// El caso que justifica el PR es el hueco de 4–6 h: hoy el techo fijo de 5000
// lo descarta; con la cota escalada debe recuperarse.
#include "doctest.h"
#include "support.hpp"

using json = nlohmann::json;
using testsup::count_lines;
using testsup::capture_streams;

static const std::string kPfx = "celima/punta_hermosa/planta/linea/";

static CounterCtx ctx_for(double elapsed_s, double rate_per_h = 3600.0,
                          double margin = 1.5, uint16_t max_valid = 5000)
{
    CounterCtx c;
    c.line = 1;
    c.proc = "test";
    c.field = "campo";
    c.elapsed_s = elapsed_s;
    c.rate_max_per_s = rate_per_h / 3600.0;
    c.margin = margin;
    c.max_valid = max_valid;
    return c;
}

TEST_CASE("diff_counter_scaled: el camino normal no cambia") {
    // Intervalo de 180 s: la cota escalada (270) queda por debajo del techo
    // mínimo de 5000, así que manda max_valid y el comportamiento es el de
    // siempre. Es lo que mantiene el golden intacto.
    const auto ctx = ctx_for(180);
    CHECK(diff_counter_scaled(1064, 1000, ctx).plausible);          // delta 64
    CHECK(diff_counter_scaled(6000, 1000, ctx).plausible);          // delta 5000, en el límite
    CHECK_FALSE(diff_counter_scaled(6001, 1000, ctx).plausible);    // delta 5001
    CHECK(diff_counter_scaled(1000, 1000, ctx).plausible);          // sin movimiento
    CHECK(diff_counter_scaled(1000, 1000, ctx).value == 0);
}

TEST_CASE("diff_counter_scaled: rollover de 16 bits") {
    const auto ctx = ctx_for(180);
    const auto r = diff_counter_scaled(12, 65530, ctx);
    CHECK(r.plausible);
    CHECK(r.value == 18);
}

TEST_CASE("diff_counter_scaled: hueco de 5 h recupera un delta que hoy se descarta") {
    const double gap = 5 * 3600.0;
    const auto ctx = ctx_for(gap);                 // 1/s * 18000 * 1,5 = 27.000
    const auto r = diff_counter_scaled(1000 + 7000, 1000, ctx);
    CHECK(r.value == 7000);
    CHECK(r.plausible);                            // con el techo fijo de 5000 se perdía
    CHECK(r.max_plausible == doctest::Approx(27000.0));

    // Pero no cualquier cosa: por encima de la cota sigue siendo implausible.
    const auto over = diff_counter_scaled(1000 + 30000, 1000, ctx);
    CHECK_FALSE(over.plausible);
    CHECK(std::string(over.reason) == "over_bound");
}

TEST_CASE("diff_counter_scaled: hueco que excede el módulo es ambiguo") {
    // 1/s * margen 1,5: la cota alcanza 65.536 pasadas ~12,1 h.
    const auto ctx = ctx_for(13 * 3600.0);
    const auto r = diff_counter_scaled(1500, 1000, ctx);
    CHECK_FALSE(r.plausible);                      // aunque el delta sea pequeño
    CHECK(std::string(r.reason) == "ambiguous_module");
}

TEST_CASE("diff_counter_scaled: sin hueco medible o sin tasa, implausible") {
    CHECK_FALSE(diff_counter_scaled(1100, 1000, ctx_for(0)).plausible);
    CHECK(std::string(diff_counter_scaled(1100, 1000, ctx_for(0)).reason) == "no_elapsed");

    // Reloj hacia atrás: no se "arregla", se descarta.
    const auto back = diff_counter_scaled(1100, 1000, ctx_for(-600));
    CHECK_FALSE(back.plausible);
    CHECK(std::string(back.reason) == "no_elapsed");

    const auto no_rate = diff_counter_scaled(1100, 1000, ctx_for(180, 0.0));
    CHECK_FALSE(no_rate.plausible);
    CHECK(std::string(no_rate.reason) == "no_rate");

    // Un contador quieto sigue siendo un cero legítimo aunque falte el tiempo.
    CHECK(diff_counter_scaled(1000, 1000, ctx_for(0)).plausible);
}

TEST_CASE("un hueco legítimo no dispara el camino de los 3 rechazos") {
    testsup::pin_local_hour(10);
    reset_all_processor_states();
    testsup::rates_for_tests();

    auto secador = createProcessor(DeviceType::Entrada_secador);
    secador->process(testsup::make_frame(3, 1, 0), kPfx, 3);

    // Siguiente trama 5 h después (tick 100 = 18.000 s) con la producción
    // acumulada de esas 5 h: 6.000 unidades, por encima del techo fijo.
    json m = testsup::make_frame(3, 1, 100);
    m["ingreso_elevador_cantidad"] = 1000 + 6000;

    std::vector<Publication> pubs;
    const std::string log = capture_streams([&] { pubs = secador->process(m, kPfx, 3); });

    // El contador de producción se recupera sin disparar el re-anclaje.
    CHECK(count_lines(log, {"[STATE] delta_rejected",
                            "field=ingreso_elevador_cantidad"}) == 0);
    CHECK(count_lines(log, {"[STATE] reanchor"}) == 0);
    REQUIRE(pubs.size() == 2);
    const json prod = json::parse(pubs[1].payload);
    CHECK(prod["ingreso_elevador_turno"] == 6000);   // recuperado, no perdido

    // Los acumuladores latcheados también se recuperan: su cota no es la tasa
    // ni el módulo, es el reloj del turno, y 5 h de paro caben de sobra. Antes
    // de PR 3 estos tres se descartaban por ambiguous_module.
    CHECK(count_lines(log, {"[STATE] delta_rejected", "family=tiempo_latcheado"}) == 0);
    // Y el salto queda registrado como lo que es, un paro con su duración.
    CHECK(count_lines(log, {"[STATE] paro_latched", "proc=entrada_secador"}) == 3);
    // Los de segundos también: 1 tick/s no agota el módulo hasta las ~12 h.
    CHECK(count_lines(log, {"[STATE] delta_rejected", "family=tiempo_s"}) == 0);
}

TEST_CASE("gatewayTime ausente: implausible, sin caer a la hora del servidor") {
    testsup::pin_local_hour(10);
    reset_all_processor_states();
    testsup::rates_for_tests();

    auto secador = createProcessor(DeviceType::Entrada_secador);
    secador->process(testsup::make_frame(3, 2, 0), kPfx, 3);

    json m = testsup::make_frame(3, 2, 1);
    m.erase("gatewayTime");

    std::vector<Publication> pubs;
    const std::string log = capture_streams([&] { pubs = secador->process(m, kPfx, 3); });

    CHECK(count_lines(log, {"[STATE] delta_rejected", "reason=no_elapsed"}) >= 1);
    REQUIRE(pubs.size() == 2);
    const json prod = json::parse(pubs[1].payload);
    CHECK(prod["ingreso_elevador_turno"] == 0);      // subcontar, no inventar
}

TEST_CASE("RateConfig: resolución por línea y máquina, y fallbacks") {
    celima::rates().reset_for_tests();
    CHECK(celima::rates().default_rate_per_h() == doctest::Approx(600.0));
    CHECK(celima::rates().margin() == doctest::Approx(1.5));
    // Sin entradas, cualquier clave cae al valor por defecto.
    CHECK(celima::rates().rate_per_s(1, "prensa_hidraulica1")
          == doctest::Approx(600.0 / 3600.0));

    const std::string path = std::string(testsup::tmpdir()) + "/rates_ok.json";
    REQUIRE(testsup::write_file(path, R"({
        "default_rate_per_h": 700,
        "margin": 2.0,
        "lines": { "1": { "prensa_hidraulica1": 1500 }, "2": { "salida_horno": 900 } }
    })"));
    REQUIRE(celima::rates().load_file(path));
    CHECK(celima::rates().rate_per_s(1, "prensa_hidraulica1") == doctest::Approx(1500.0 / 3600.0));
    CHECK(celima::rates().rate_per_s(2, "salida_horno") == doctest::Approx(900.0 / 3600.0));
    CHECK(celima::rates().rate_per_s(3, "salida_horno") == doctest::Approx(700.0 / 3600.0));
    CHECK(celima::rates().margin() == doctest::Approx(2.0));

    celima::rates().reset_for_tests();
}

TEST_CASE("RateConfig: archivo ausente o corrupto no tumba el servicio") {
    celima::rates().reset_for_tests();

    std::string log = capture_streams([&] {
        CHECK_FALSE(celima::rates().load_file("/no/existe/rates.json"));
    });
    CHECK(count_lines(log, {"[CONFIG] rates file not usable"}) == 1);
    CHECK(celima::rates().default_rate_per_h() == doctest::Approx(600.0));

    const std::string bad = std::string(testsup::tmpdir()) + "/rates_bad.json";
    REQUIRE(testsup::write_file(bad, "{ esto no es json"));
    log = capture_streams([&] { CHECK_FALSE(celima::rates().load_file(bad)); });
    CHECK(count_lines(log, {"[CONFIG] rates file not usable"}) == 1);

    // JSON estricto: un comentario se rechaza. Es deliberado — rates.json tiene
    // que abrirse con jq y con los linters de CI, y un archivo que el servicio
    // acepta pero jq no es peor que uno que se rechaza con un log visible.
    const std::string commented = std::string(testsup::tmpdir()) + "/rates_comment.json";
    REQUIRE(testsup::write_file(commented,
        "// tasas\n{\"default_rate_per_h\": 900}\n"));
    log = capture_streams([&] { CHECK_FALSE(celima::rates().load_file(commented)); });
    CHECK(count_lines(log, {"[CONFIG] rates file not usable"}) == 1);

    // Una tasa absurda tampoco se acepta.
    const std::string zero = std::string(testsup::tmpdir()) + "/rates_zero.json";
    REQUIRE(testsup::write_file(zero, R"({"default_rate_per_h": 0})"));
    log = capture_streams([&] { CHECK_FALSE(celima::rates().load_file(zero)); });
    CHECK(count_lines(log, {"[CONFIG] rates file not usable"}) == 1);
    CHECK(celima::rates().default_rate_per_h() == doctest::Approx(600.0));

    celima::rates().reset_for_tests();
}

TEST_CASE("el rates.json que se instala en /etc es válido y lleva tasas medidas") {
    // Comprueba la forma, no los números: las tasas se re-derivan del journal
    // cada cierto tiempo (pendiente P5 de docs/design/cota-plausibilidad-y-tasas.md)
    // y este test no debe obligar a tocarlo en cada revisión.
    celima::rates().reset_for_tests();
    const std::string log = capture_streams([&] {
        REQUIRE(celima::rates().load_file("packaging/rates.json"));
    });
    CHECK(count_lines(log, {"[CONFIG] rates cargadas de"}) == 1);
    CHECK(celima::rates().default_rate_per_h() > 0.0);
    CHECK(celima::rates().margin() > 0.0);

    // El archivo trae tasas por línea y máquina, no es ya la plantilla vacía:
    // al menos una clave conocida resuelve a algo distinto del valor por defecto.
    const double def = celima::rates().default_rate_per_h() / 3600.0;
    int overrides = 0;
    for (const auto& [line, machine] : std::vector<std::pair<int, const char*>>{
             {1, "prensa_hidraulica1"}, {2, "salida_horno"},
             {3, "esmalte"}, {4, "entrada_secador"}}) {
        const double r = celima::rates().rate_per_s(line, machine);
        CHECK(r > 0.0);
        if (r != doctest::Approx(def)) ++overrides;
    }
    CHECK_MESSAGE(overrides > 0,
                  "packaging/rates.json no trae tasas por línea: todo cae al valor por defecto");

    celima::rates().reset_for_tests();
}

TEST_CASE("el horizonte de recuperación se acorta cuando sube la tasa") {
    // Contraintuitivo y documentado en cota-plausibilidad-y-tasas.md §2: una
    // tasa alta NO es más segura, recorta la ventana recuperable, porque la
    // cota alcanza antes el módulo de 65.536 y el delta pasa a ser ambiguo.
    const double margin = 1.5;
    auto recovers = [&](double rate_per_h, double gap_h) {
        CounterCtx c = ctx_for(gap_h * 3600.0, rate_per_h, margin);
        return diff_counter_scaled(1500, 1000, c).plausible;   // delta pequeño
    };

    // 500 u/h: un corte de 28 h sigue siendo recuperable.
    CHECK(recovers(500, 28));
    // 6.000 u/h (esmalte): pasadas ~7 h el delta ya es ambiguo.
    CHECK(recovers(6000, 6));
    CHECK_FALSE(recovers(6000, 8));
}

TEST_CASE("familias de contador: la tabla clasifica lo que debe") {
    using F = CounterFamily;
    // Evento: usa la tasa medida de rates.json.
    CHECK(counter_family_for("entrada_secador", "ingreso_elevador_cantidad") == F::Event);
    CHECK(counter_family_for("prensa_hidraulica1", "pisadas") == F::Event);
    CHECK(counter_family_for("calidad", "totalBroken") == F::Event);
    // Segundos: 1 tick/s.
    CHECK(counter_family_for("entrada_secador", "timer1Hz") == F::TimeSeconds);
    CHECK(counter_family_for("entrada_horno", "falha_forno_tiempo") == F::TimeSeconds);
    // Decisegundos puro: hoy solo queda sentido_escolha_tiempo, que NO migró a
    // LatchedTime por no tener firma (100% de deltas en cero durante 24 h).
    CHECK(counter_family_for("salida_horno", "sentido_escolha_tiempo") == F::TimeDeciseconds);
    // Latcheados: avanzan a saltos que valen el paro anterior. En las prensas el
    // nombre no lo delata (se acumula con ×0,1), y es justo el caso que un
    // clasificador por sufijo habría fallado.
    CHECK(counter_family_for("prensa_hidraulica1", "metrica_tiempo") == F::LatchedTime);
    CHECK(counter_family_for("entrada_secador", "bancalino_l1_tiempo") == F::LatchedTime);
    // Lo desconocido cae en Event, que es el lado conservador.
    CHECK(counter_family_for("entrada_secador", "campo_que_no_existe") == F::Event);
    CHECK(counter_family_for("procesador_nuevo", "timer1Hz") == F::Event);
    CHECK(counter_family_for(nullptr, nullptr) == F::Event);
}

TEST_CASE("ningún contador de tiempo se queda sin clasificar") {
    // Guarda estructural contra el defecto P1: deriva la expectativa de los
    // propios sitios de llamada del código, no de la tabla. Si alguien añade un
    // contador de tiempo y olvida clasificarlo, esto se pone rojo en lugar de
    // dejar que su acumulador se descarte en silencio tras cada hueco.
    const std::string src = testsup::read_file("src/MessageProcessor.cpp");
    REQUIRE_MESSAGE(!src.empty(), "ejecuta la suite desde la raíz del repo");

    std::istringstream in(src);
    std::string line, proc;
    int sites = 0, unclassified = 0;
    std::string missing;
    while (std::getline(in, line)) {
        const auto p = line.find("ctx.proc = \"");
        if (p != std::string::npos) {
            const auto a = p + 12;
            proc = line.substr(a, line.find('"', a) - a);
            continue;
        }
        for (const char* forma : {"ctx.with(\"", "ctx.with_acc(\""}) {
        const size_t largo = std::string(forma).size();
        size_t w = 0;
        while ((w = line.find(forma, w)) != std::string::npos) {
            const auto a = w + largo;
            const std::string field = line.substr(a, line.find('"', a) - a);
            w = a;
            ++sites;
            const bool looks_like_time =
                field.find("tiempo") != std::string::npos ||
                field.find("tempo")  != std::string::npos ||
                field.find("timer")  != std::string::npos;
            if (looks_like_time &&
                counter_family_for(proc.c_str(), field.c_str()) == CounterFamily::Event) {
                ++unclassified;
                missing += " " + proc + "/" + field;
            }
        }
        }
    }
    // Si el patrón de llamada cambia, este test no debe pasar en vacío.
    CHECK(sites >= 50);
    CHECK_MESSAGE(unclassified == 0, "contadores de tiempo sin familia:" << missing);
}

TEST_CASE("P1: un hueco corto ya recupera los acumuladores de tiempo") {
    // Antes, la tasa de producción de la máquina (450 u/h en L1/entrada_secador)
    // dejaba la cota 40x corta para un contador de 10 ticks/s: el tiempo de
    // operación se descartaba en cualquier hueco de más de unos minutos.
    const double gap = 30 * 60.0;                  // 30 min
    const uint16_t delta_real = 18000;             // 10 ticks/s * 1800 s

    CounterCtx as_event = ctx_for(gap, 450.0);     // tasa de producción
    as_event.proc = "salida_horno";
    as_event.field = "bancalinos_total";           // familia Event
    CHECK_FALSE(diff_counter_scaled(1000 + delta_real, 1000, as_event).plausible);

    CounterCtx as_ds = as_event;
    as_ds.field = "sentido_escolha_tiempo";        // familia tiempo_ds
    const auto r = diff_counter_scaled(1000 + delta_real, 1000, as_ds);
    CHECK(r.plausible);                            // ahora sí se recupera
    CHECK(r.max_plausible == doctest::Approx(10.0 * gap * 1.5));

    // Pero el módulo sigue mandando: a 10 ticks/s la ventana recuperable acaba
    // hacia 1,2 h, y más allá el delta es ambiguo por construcción.
    CounterCtx beyond = as_ds;
    beyond.elapsed_s = 1.5 * 3600.0;
    CHECK(std::string(diff_counter_scaled(1500, 1000, beyond).reason) == "ambiguous_module");
}

// ---------------------------------------------------------------------------
// Familia Level (defecto D5): numero_grades es el nivel del buffer de filas que
// esperan entrar al horno. Sube al cargar y baja al desocuparse, así que la
// resta sin signo convertía cada bajada en ~65.532: 836 rechazos al día, el 78%
// de todos los del servicio.

static CounterCtx ctx_level(uint16_t max_valid = 500)
{
    CounterCtx c = ctx_for(127, 900.0);        // intervalo real de entrada horno
    c.proc = "entrada_horno";
    c.field = "numero_grades";
    c.max_valid = max_valid;
    return c;
}

TEST_CASE("Level: el nivel se interpreta con signo") {
    REQUIRE(counter_family_for("entrada_horno", "numero_grades") == CounterFamily::Level);
    const auto ctx = ctx_level();

    SUBCASE("bajada normal") {
        // El caso medido en las cuatro líneas: curr = prev - 4.
        const auto r = diff_counter_scaled(36, 40, ctx);
        CHECK(r.signed_value == -4);
        CHECK(r.value == 4);
        CHECK(r.plausible);                    // antes daba 65.532 y se rechazaba
    }
    SUBCASE("subida normal") {
        const auto r = diff_counter_scaled(41, 36, ctx);
        CHECK(r.signed_value == 5);
        CHECK(r.value == 5);
        CHECK(r.plausible);
    }
    SUBCASE("sin movimiento") {
        const auto r = diff_counter_scaled(40, 40, ctx);
        CHECK(r.signed_value == 0);
        CHECK(r.plausible);
    }
    SUBCASE("salto imposible") {
        const auto r = diff_counter_scaled(9000, 40, ctx);
        CHECK_FALSE(r.plausible);
        CHECK(std::string(r.reason) == "level_jump");
    }
    SUBCASE("cruce del módulo con signo") {
        // prev=2, curr=65534 es una bajada de 4, no una subida de 65.532. Es lo
        // que hace correcto el int16_t frente a una resta condicional.
        const auto r = diff_counter_scaled(65534, 2, ctx);
        CHECK(r.signed_value == -4);
        CHECK(r.plausible);
    }
    SUBCASE("un nivel no depende de la tasa ni del hueco") {
        CounterCtx sin_tasa = ctx_level();
        sin_tasa.rate_max_per_s = 0.0;         // configuración ausente
        sin_tasa.elapsed_s = 0.0;              // sin hueco medible
        const auto r = diff_counter_scaled(36, 40, sin_tasa);
        CHECK(r.plausible);
        CHECK(r.signed_value == -4);
        CHECK(std::string(r.reason) != "no_rate");
        CHECK(std::string(r.reason) != "no_elapsed");
    }
}

TEST_CASE("Level: subidas, bajadas y buffer vacío en entrada horno") {
    testsup::pin_local_hour(10);
    reset_all_processor_states();
    testsup::rates_for_tests();
    celima::set_state_store(nullptr);

    auto horno = createProcessor(DeviceType::Entrada_horno);

    // Secuencia del nivel: siembra en 10, sube a 14 (+4), baja a 9 (-5),
    // sube a 12 (+3), baja a 0 (-12), sigue en 0, sube a 6 (+6).
    const std::vector<int> nivel = {10, 14, 9, 12, 0, 0, 6};
    std::vector<Publication> pubs;
    const std::string log = capture_streams([&] {
        for (size_t i = 0; i < nivel.size(); ++i) {
            json m = testsup::make_frame(6, 1, static_cast<int>(i));
            m["numero_grades"] = nivel[i];
            pubs = horno->process(m, kPfx, 3);
        }
    });

    // Ni un solo rechazo: es justo el ruido que este cambio elimina.
    CHECK(count_lines(log, {"delta_rejected", "field=numero_grades"}) == 0);

    const json prod = json::parse(pubs[1].payload);
    CHECK(prod["numero_grades_instantaneo"] == 6);          // crudo, sin tocar
    CHECK(prod["numero_grades_turno"] == 4 + 3 + 6);        // subidas: 13
    CHECK(prod["numero_grades_bajadas_turno"] == 5 + 12);   // bajadas: 17
    // Dos tramas con el nivel a cero. Es muestreo: cada una cuenta su intervalo
    // completo (120 s en entrada horno).
    CHECK(prod["buffer_vacio_turno_s"] == 2 * 120);
}

TEST_CASE("Level: un salto imposible se rechaza y re-ancla como los demás") {
    testsup::pin_local_hour(10);
    reset_all_processor_states();
    testsup::rates_for_tests();
    celima::set_state_store(nullptr);

    auto horno = createProcessor(DeviceType::Entrada_horno);
    json m = testsup::make_frame(6, 2, 0);
    m["numero_grades"] = 10;
    horno->process(m, kPfx, 3);

    const std::string log = capture_streams([&] {
        for (int tick = 1; tick <= 3; ++tick) {
            json f = testsup::make_frame(6, 2, tick);
            f["numero_grades"] = 9000;         // fuera de cualquier nivel físico
            horno->process(f, kPfx, 3);
        }
    });
    CHECK(count_lines(log, {"delta_rejected", "field=numero_grades",
                            "family=nivel", "reason=level_jump"}) == 3);
    CHECK(count_lines(log, {"reanchor", "field=numero_grades"}) == 1);
}

// ---------------------------------------------------------------------------
// Familia LatchedTime (defecto D6): el ladder le suma un contador libre y lo
// reinicia en cada evento, así que se congela mientras la máquina está parada y
// suelta el paro entero de golpe. Su incremento vale el paro ANTERIOR, no el
// intervalo entre tramas: ninguna cota por tasa aplica.

static CounterCtx ctx_latched(double shift_elapsed_s, uint32_t acc_current,
                              double elapsed_s = 187.0)
{
    CounterCtx c = ctx_for(elapsed_s, 900.0);
    c.proc = "entrada_secador";
    c.field = "bancalino_l1_tiempo";
    c.shift_elapsed_s = shift_elapsed_s;
    c.acc_current = acc_current;
    return c;
}

TEST_CASE("LatchedTime: la cota es el reloj del turno más un módulo de arrastre") {
    REQUIRE(counter_family_for("entrada_secador", "bancalino_l1_tiempo")
            == CounterFamily::LatchedTime);

    SUBCASE("operación normal") {
        const auto r = diff_counter_scaled(1870, 0, ctx_latched(4000, 39000));
        CHECK(r.plausible);
        CHECK(r.max_plausible == doctest::Approx(10.0 * 4000 * 1.05 + 65536.0));
    }
    SUBCASE("salto de paro") {
        // El caso que hoy se rechaza y que es el corazón del PR: 37 min de paro
        // soltados de golpe en el primer evento tras el arranque.
        const auto r = diff_counter_scaled(22400, 0, ctx_latched(4000, 39000));
        CHECK(r.value == 22400);
        CHECK(r.plausible);
    }
    SUBCASE("arrastre de turno") {
        // El caso que rompió la primera versión de esta cota. Turno recién
        // empezado y un paro de 40 min que arrancó ANTES de la frontera: el
        // contador libre del PLC no sabe de turnos y lo suelta entero aquí.
        // Sin el término de arrastre esto se rechazaba: 10 x 1200 x 1,05 =
        // 12.600 < 24.000.
        const auto r = diff_counter_scaled(24000, 0, ctx_latched(1200, 0));
        CHECK(r.plausible);
    }
    SUBCASE("por encima de todo lo posible") {
        // Ni con el arrastre cabe: el acumulado se habría desbocado.
        const auto r = diff_counter_scaled(60000, 0, ctx_latched(40000, 450000));
        CHECK_FALSE(r.plausible);
        CHECK(std::string(r.reason) == "over_shift_clock");
    }
    SUBCASE("sin turno no hay cota") {
        const auto r = diff_counter_scaled(1870, 0, ctx_latched(0, 39000));
        CHECK_FALSE(r.plausible);
        CHECK(std::string(r.reason) == "no_shift_elapsed");
    }
    SUBCASE("el intervalo entre tramas es irrelevante") {
        // Si el resultado dependiera de elapsed_s, la familia estaría mal
        // implementada: el incremento no guarda relación con el intervalo.
        const auto corto = diff_counter_scaled(22400, 0, ctx_latched(4000, 39000, 127.0));
        const auto largo = diff_counter_scaled(22400, 0, ctx_latched(4000, 39000, 4000.0));
        CHECK(corto.plausible == largo.plausible);
        CHECK(corto.max_plausible == doctest::Approx(largo.max_plausible));
        CHECK(corto.value == largo.value);
    }
    SUBCASE("la tasa configurada es irrelevante") {
        CounterCtx sin_tasa = ctx_latched(4000, 39000);
        sin_tasa.rate_max_per_s = 0.0;
        const auto r = diff_counter_scaled(1870, 0, sin_tasa);
        CHECK(r.plausible);
        CHECK(std::string(r.reason) != "no_rate");
    }
}

TEST_CASE("LatchedTime: el caso L2 completo, sin un solo rechazo") {
    // Reproduce lo medido en planta: doce tramas con el campo congelado —la
    // línea parada— y luego el paro entero de golpe. Antes de PR 3 ese salto se
    // rechazaba, y como el re-anclaje fija prev_ref al valor posterior, el paro
    // no se recuperaba nunca: L2 arrastraba un déficit fijo de ~23.600 ticks.
    testsup::pin_local_hour(10);
    reset_all_processor_states();
    testsup::rates_for_tests();
    celima::set_state_store(nullptr);

    auto secador = createProcessor(DeviceType::Entrada_secador);
    const int kParo = 22400;                       // 37 min 20 s

    json m0 = testsup::make_frame(3, 1, 0);
    m0["bancalino_l1_tiempo_ds"] = 1000;
    secador->process(m0, kPfx, 3);

    std::vector<Publication> pubs;
    const std::string log = capture_streams([&] {
        for (int tick = 1; tick <= 12; ++tick) {   // congelado: la línea no produce
            json m = testsup::make_frame(3, 1, tick);
            m["bancalino_l1_tiempo_ds"] = 1000;
            pubs = secador->process(m, kPfx, 3);
        }
        json m = testsup::make_frame(3, 1, 13);    // primer evento tras el arranque
        m["bancalino_l1_tiempo_ds"] = 1000 + kParo;
        pubs = secador->process(m, kPfx, 3);
    });

    CHECK(count_lines(log, {"delta_rejected", "field=bancalino_l1_tiempo"}) == 0);
    CHECK(count_lines(log, {"[STATE] paro_latched", "field=bancalino_l1_tiempo",
                            "duracion_s=2240"}) == 1);
    REQUIRE(pubs.size() == 2);
    const json prod = json::parse(pubs[1].payload);
    CHECK(prod["bancalino_l1_tiempo_turno_ds"] == kParo);

    // Sin regresión en los campos de familia tiempo_s del mismo procesador: la
    // secuencia no los toca y siguen acumulando su propio ritmo.
    CHECK(count_lines(log, {"delta_rejected", "family=tiempo_s"}) == 0);
    CHECK(prod["paradas_tiempo_turno_s"] == 12 * 64 + 64);
}

TEST_CASE("LatchedTime: el guardarraíl salta con el acumulado desbocado") {
    // La cota NO decide sobre un delta suelto —al principio del turno el
    // arrastre admite casi cualquiera, y así está documentado en el diseño—,
    // pero sí impide que el acumulado se desboque. Se comprueba la propiedad,
    // no el instante exacto: cuántos saltos caben depende de en qué punto del
    // turno esté el reloj, y eso no es lo que se quiere fijar.
    testsup::pin_local_hour(6);                    // turno recién empezado
    reset_all_processor_states();
    testsup::rates_for_tests();
    celima::set_state_store(nullptr);

    auto secador = createProcessor(DeviceType::Entrada_secador);
    json m0 = testsup::make_frame(3, 2, 0);
    m0["bancalino_l1_tiempo_ds"] = 0;
    secador->process(m0, kPfx, 3);

    // Saltos de 16.000 ticks (26 min de "paro") cada 3 minutos de reloj: un
    // acumulado imposible, que la cota tiene que acabar frenando.
    int primer_rechazo = -1;
    for (int k = 1; k <= 12 && primer_rechazo < 0; ++k) {
        json m = testsup::make_frame(3, 2, k);
        m["bancalino_l1_tiempo_ds"] = static_cast<uint16_t>(16000 * k);
        const std::string log = capture_streams([&] { secador->process(m, kPfx, 3); });
        if (count_lines(log, {"delta_rejected", "field=bancalino_l1_tiempo",
                              "family=tiempo_latcheado", "reason=over_shift_clock"}) > 0)
            primer_rechazo = k;
    }
    CHECK_MESSAGE(primer_rechazo > 0,
                  "el acumulado creció sin freno: la cota no está haciendo de guardarraíl");
}

TEST_CASE("LatchedTime: un paro a caballo de la frontera de turno se acepta") {
    // ES LA PRUEBA DE REGRESIÓN del fallo de diseño que destapó el replay del
    // log de planta. El contador libre del PLC no sabe nada de turnos: sigue
    // corriendo a través de las 06:00, así que un paro que empieza a las 05:40
    // se suelta ENTERO en el turno nuevo, cuyo reloj lleva minutos. Sin el
    // término de arrastre, ese salto se rechazaba y el paro se perdía — que es
    // exactamente el déficit fijo que se midió en L2.
    testsup::pin_local_hour(6);                    // turno recién empezado
    reset_all_processor_states();
    testsup::rates_for_tests();
    celima::set_state_store(nullptr);

    auto secador = createProcessor(DeviceType::Entrada_secador);
    json m0 = testsup::make_frame(3, 9, 0);
    m0["bancalino_l1_tiempo_ds"] = 5000;
    secador->process(m0, kPfx, 3);

    // Primer evento tras el arranque: suelta 40 min de paro, la mayoría de
    // antes de las 06:00. El reloj del turno lleva 180 s.
    json m1 = testsup::make_frame(3, 9, 1);
    m1["bancalino_l1_tiempo_ds"] = 5000 + 24000;
    std::vector<Publication> pubs;
    const std::string log = capture_streams([&] { pubs = secador->process(m1, kPfx, 3); });

    CHECK(count_lines(log, {"delta_rejected", "field=bancalino_l1_tiempo"}) == 0);
    CHECK(count_lines(log, {"[STATE] paro_latched", "field=bancalino_l1_tiempo",
                            "duracion_s=2400"}) == 1);
    REQUIRE(pubs.size() == 2);
    CHECK(json::parse(pubs[1].payload)["bancalino_l1_tiempo_turno_ds"] == 24000);

    // Y el resto del turno sigue acumulando a su ritmo, sin rechazos: el
    // arrastre se absorbe según crece el reloj del turno.
    const std::string log2 = capture_streams([&] {
        for (int k = 2; k <= 20; ++k) {
            json m = testsup::make_frame(3, 9, k);
            m["bancalino_l1_tiempo_ds"] = static_cast<uint16_t>(5000 + 24000 + 1870 * (k - 1));
            pubs = secador->process(m, kPfx, 3);
        }
    });
    CHECK(count_lines(log2, {"delta_rejected"}) == 0);
    CHECK(json::parse(pubs[1].payload)["bancalino_l1_tiempo_turno_ds"] == 24000 + 1870 * 19);

    // Sin regresión en los campos de familia tiempo_s del mismo procesador.
    CHECK(count_lines(log2, {"family=tiempo_s"}) == 0);
}

TEST_CASE("LatchedTime: el reloj del turno sale de la trama, no del host") {
    // ESTE TEST EXISTE POR UN BUG CONCRETO. Al implementar PR 3 se calculó
    // shift_elapsed_s con std::time(nullptr) en lugar de con dev_epoch. En una
    // suite que corre en segundos las dos versiones dan lo mismo, así que no
    // falló nada; solo se vio reproduciendo 24 h de tráfico real, donde el
    // acumulado de N horas se comparaba contra un turno de M y se rechazaba
    // todo.
    //
    // Aquí se separan los dos relojes a propósito: el host va por la mitad del
    // turno y las tramas llegan del principio. Si la cota usara el reloj del
    // host, el techo sería 25x mayor y no habría rechazo.
    testsup::pin_local_hour(13);                   // host: 7 h de turno S1 (06–14)
    reset_all_processor_states();
    testsup::rates_for_tests();
    celima::set_state_store(nullptr);

    auto secador = createProcessor(DeviceType::Entrada_secador);
    const int64_t manana = testsup::base_epoch() - 7 * 3600;   // 06:00 local

    auto trama = [&](int k, int nivel) {
        json m = testsup::make_frame(3, 40, k);
        m["gatewayTime"] = testsup::iso_utc(manana + k * 180);
        m["bancalino_l1_tiempo_ds"] = static_cast<uint16_t>(nivel);
        return m;
    };

    secador->process(trama(0, 0), kPfx, 3);        // siembra a las 06:00
    // Dos saltos que caben bajo el techo de la trama (06:03 y 06:06):
    // 10 x ~360 s x 1,05 + 65.536 ≈ 69.300.
    secador->process(trama(1, 34000), kPfx, 3);
    secador->process(trama(2, 34000 + 34000 - 65536), kPfx, 3);

    // El tercero ya no cabe con el reloj de la trama (~68.000 + 5.000), pero
    // sobraría con el del host (techo ≈ 330.000).
    const std::string log = capture_streams([&] {
        secador->process(trama(3, 34000 + 34000 + 5000 - 65536), kPfx, 3);
    });
    CHECK_MESSAGE(count_lines(log, {"delta_rejected", "field=bancalino_l1_tiempo",
                                    "reason=over_shift_clock"}) == 1,
                  "la cota está usando el reloj del host y no el de la trama");
    // Y la traza tiene que reportar el turno de la TRAMA, no el del host.
    CHECK(count_lines(log, {"turno_s=540"}) == 1);
}
