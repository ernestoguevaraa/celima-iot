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

    // Los acumuladores de tiempo en decisegundos SÍ se descartan aquí, y es
    // correcto: a 10 ticks/s el contador de 16 bits da la vuelta en 1,8 h, así
    // que tras 5 h su delta es ambiguo por construcción. No hay configuración
    // que lo arregle; es el módulo del contador contra la velocidad del tick.
    CHECK(count_lines(log, {"[STATE] delta_rejected", "family=tiempo_ds",
                            "reason=ambiguous_module"}) == 3);
    // Los de segundos, en cambio, sí se recuperan: 1 tick/s no agota el módulo
    // hasta las ~12 h.
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
    // Decisegundos: 10 ticks/s. En las prensas el nombre no lo delata (se
    // acumula con ×0,1), y es justo el caso que un clasificador por sufijo
    // habría fallado.
    CHECK(counter_family_for("prensa_hidraulica1", "metrica_tiempo") == F::TimeDeciseconds);
    CHECK(counter_family_for("entrada_secador", "bancalino_l1_tiempo") == F::TimeDeciseconds);
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
        size_t w = 0;
        while ((w = line.find("ctx.with(\"", w)) != std::string::npos) {
            const auto a = w + 10;
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
    as_event.proc = "entrada_secador";
    as_event.field = "ingreso_elevador_cantidad";  // familia Event
    CHECK_FALSE(diff_counter_scaled(1000 + delta_real, 1000, as_event).plausible);

    CounterCtx as_ds = as_event;
    as_ds.field = "ingreso_elevador_tiempo";       // familia tiempo_ds
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
