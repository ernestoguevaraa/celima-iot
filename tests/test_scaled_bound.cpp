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

    CHECK(count_lines(log, {"[STATE] delta_rejected"}) == 0);
    CHECK(count_lines(log, {"[STATE] reanchor"}) == 0);
    REQUIRE(pubs.size() == 2);
    const json prod = json::parse(pubs[1].payload);
    CHECK(prod["ingreso_elevador_turno"] == 6000);   // recuperado, no perdido
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

    // Una tasa absurda tampoco se acepta.
    const std::string zero = std::string(testsup::tmpdir()) + "/rates_zero.json";
    REQUIRE(testsup::write_file(zero, R"({"default_rate_per_h": 0})"));
    log = capture_streams([&] { CHECK_FALSE(celima::rates().load_file(zero)); });
    CHECK(count_lines(log, {"[CONFIG] rates file not usable"}) == 1);
    CHECK(celima::rates().default_rate_per_h() == doctest::Approx(600.0));

    celima::rates().reset_for_tests();
}

TEST_CASE("la plantilla que se instala en /etc se carga tal cual") {
    // packaging/rates.json documenta el formato en comentarios; si el parser
    // deja de tolerarlos, el servicio caería al valor por defecto en silencio.
    celima::rates().reset_for_tests();
    const std::string log = capture_streams([&] {
        REQUIRE(celima::rates().load_file("packaging/rates.json"));
    });
    CHECK(count_lines(log, {"[CONFIG] rates cargadas de"}) == 1);
    CHECK(celima::rates().default_rate_per_h() == doctest::Approx(600.0));
    CHECK(celima::rates().margin() == doctest::Approx(1.5));
    // "lines" vacío a propósito: sin tasas medidas, todo cae al valor conservador.
    CHECK(celima::rates().rate_per_s(1, "prensa_hidraulica1")
          == doctest::Approx(600.0 / 3600.0));
    celima::rates().reset_for_tests();
}
