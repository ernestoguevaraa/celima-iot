// Salida horno tras un hueco: la producción acumulada durante el hueco se
// cuenta, y la corrupción real la sigue parando la cota por tasa.
//
// Reproduce el caso de L1 del 2026-10-27, T1: tras ~56 min sin tramas, la
// siguiente llegó con +375 ciclos y +373 en barreira1. El detector de picos por
// EMA (umbral 10x la media, suelo 300) lo marcó como «Frame corrupto» por subir
// a la vez los dos contadores y descartó la trama entera: la producción del
// hueco se perdió aunque la cota escalada la daba por plausible. Y no solo esa
// trama: el descarte no avanzaba prev, así que cada trama siguiente traía un
// delta mayor y también se descartaba, congelando los 15 contadores.
//
// Desde 1.09.1 esos dos campos llevan max_valid = 300 en diff_counter_safe: tan
// estricto como el detector en un intervalo normal, pero escalando con el hueco
// y re-anclando tras max_rejects.
#include "doctest.h"
#include "support.hpp"

using json = nlohmann::json;
using testsup::count_lines;
using testsup::capture_streams;

static const std::string kPfx = "celima/punta_hermosa/planta/linea/";

namespace {

// Trama de salida horno (L1 por defecto) a `t` segundos de las 07:00 locales, con los dos
// contadores del caso. El resto de campos no se mueve; timer1Hz avanza con el
// reloj, como en planta.
json frame_sh(int64_t t, uint16_t ciclos, uint16_t barreira1, int line = 1)
{
    json m = testsup::make_frame(7, line, 0);
    m["gatewayTime"]        = testsup::iso_utc(testsup::base_epoch() + t);
    m["timer1Hz"]           = static_cast<uint16_t>(1000 + t);
    m["metrica_mdf_ciclos"] = ciclos;
    m["barreira1_cantidad"] = barreira1;
    return m;
}

int64_t produccion_turno(const std::vector<Publication>& pubs)
{
    REQUIRE(pubs.size() == 2);
    return json::parse(pubs[1].payload)["cantidad_produccion_turno"].get<int64_t>();
}

} // namespace

TEST_CASE("salida horno: la producción de un hueco de 56 min se cuenta, no se descarta") {
    testsup::pin_local_hour(7);            // T1 en modo 2 (06–18)
    REQUIRE(testsup::local_hour_now() == 7);
    reset_all_processor_states();
    reset_global_shift_state();
    testsup::rates_for_tests(950.0, 1.5);  // la tasa de L1/salida_horno en rates.json

    auto proc = createProcessor(DeviceType::Salida_horno);
    int64_t t = 20 * 60;                   // 07:20
    uint16_t ciclos = 34462, b1 = 20889;
    int64_t total = 0;

    const std::string log = capture_streams([&] {
        proc->process(frame_sh(t, ciclos, b1), kPfx, 2);            // reseed

        // Tramas normales antes del hueco: le dan historia a la EMA. El ritmo
        // importa: el umbral antiguo era max(300, 10 × EMA), así que con +42 la
        // EMA llega a 42, el umbral a 420 y +375 pasaba sin disparar nada. Hace
        // falta una EMA por debajo de 37,5, y es lo normal en planta: la media
        // de metrica_mdf_ciclos en salida horno era 36–37 por trama en la
        // captura de 24 h de septiembre (L2 37,3; L3 36,1; L4 22,4).
        for (int i = 0; i < 6; ++i) {
            t += 186; ciclos += 36; b1 += 36;
            proc->process(frame_sh(t, ciclos, b1), kPfx, 2);
        }

        t += 3357; ciclos += 375; b1 += 373;                         // tras el hueco
        proc->process(frame_sh(t, ciclos, b1), kPfx, 2);

        t += 186; ciclos += 42; b1 += 42;
        total = produccion_turno(proc->process(frame_sh(t, ciclos, b1), kPfx, 2));
    });

    CHECK(total == 6 * 36 + 375 + 42);
    CHECK(count_lines(log, {"[SalidaHorno] Frame corrupto"}) == 0);
    CHECK(count_lines(log, {"[STATE] delta_rejected"}) == 0);
}

TEST_CASE("salida horno: un salto de +20.000 en 186 s lo sigue rechazando la cota") {
    testsup::pin_local_hour(7);
    REQUIRE(testsup::local_hour_now() == 7);
    reset_all_processor_states();
    reset_global_shift_state();
    testsup::rates_for_tests(950.0, 1.5);

    auto proc = createProcessor(DeviceType::Salida_horno);
    int64_t t = 20 * 60;
    uint16_t ciclos = 34462, b1 = 20889;
    int64_t total = 0;

    const std::string log = capture_streams([&] {
        proc->process(frame_sh(t, ciclos, b1), kPfx, 2);
        t += 186; ciclos += 42; b1 += 42;
        proc->process(frame_sh(t, ciclos, b1), kPfx, 2);

        // Corrupción: +20.000 en un intervalo normal. 950/h × 186 s × 1,5 ≈ 74,
        // así que manda el techo mínimo de estos dos campos, 300.
        t += 186;
        total = produccion_turno(proc->process(
            frame_sh(t, static_cast<uint16_t>(ciclos + 20000), b1), kPfx, 2));
    });

    CHECK(total == 42);                    // el salto no suma
    CHECK(count_lines(log, {"[STATE] delta_rejected", "proc=salida_horno", "line=1",
                            "field=metrica_ciclos", "max_plausible=300"}) == 1);
}

TEST_CASE("salida horno: +642 en 186 s se rechaza y la línea sigue contando") {
    // El caso del 11 de octubre: +642 en un intervalo normal, por encima de 300.
    // Lo que importa es que el rechazo no bloquee la línea, como hacía el
    // detector: aquí se cubren las dos formas que puede tomar el salto.
    testsup::pin_local_hour(7);
    REQUIRE(testsup::local_hour_now() == 7);
    testsup::rates_for_tests(950.0, 1.5);

    SUBCASE("salto transitorio: la trama siguiente vuelve a su valor y se suma") {
        reset_all_processor_states();
        reset_global_shift_state();
        auto proc = createProcessor(DeviceType::Salida_horno);
        int64_t t = 20 * 60;
        uint16_t ciclos = 34462, b1 = 20889;
        int64_t tras_salto = -1, total = 0;

        const std::string log = capture_streams([&] {
            proc->process(frame_sh(t, ciclos, b1), kPfx, 2);
            t += 186; ciclos += 36; b1 += 36;
            proc->process(frame_sh(t, ciclos, b1), kPfx, 2);

            t += 186;
            tras_salto = produccion_turno(proc->process(
                frame_sh(t, static_cast<uint16_t>(ciclos + 642), static_cast<uint16_t>(b1 + 640)),
                kPfx, 2));

            // La siguiente, a su ritmo: dos intervalos de producción real desde
            // la última aceptada, porque prev no avanzó con el salto.
            t += 186; ciclos += 72; b1 += 72;
            total = produccion_turno(proc->process(frame_sh(t, ciclos, b1), kPfx, 2));
        });

        CHECK(tras_salto == 36);           // el salto no suma
        CHECK(total == 36 + 72);           // y la trama normal siguiente sí
        CHECK(count_lines(log, {"[STATE] delta_rejected", "field=metrica_ciclos",
                                "max_plausible=300"}) == 1);
        CHECK(count_lines(log, {"[STATE] delta_rejected", "field=barreira1_cantidad"}) == 1);
        CHECK(count_lines(log, {"[STATE] reanchor"}) == 0);
    }

    SUBCASE("salto que se queda: tres rechazos, re-anclaje y vuelve a contar") {
        reset_all_processor_states();
        reset_global_shift_state();
        auto proc = createProcessor(DeviceType::Salida_horno);
        int64_t t = 20 * 60;
        uint16_t ciclos = 34462, b1 = 20889;
        std::vector<int64_t> totales;

        const std::string log = capture_streams([&] {
            proc->process(frame_sh(t, ciclos, b1), kPfx, 2);
            t += 186; ciclos += 36; b1 += 36;
            proc->process(frame_sh(t, ciclos, b1), kPfx, 2);

            t += 186; ciclos += 642; b1 += 640;      // el contador salta y se queda
            totales.push_back(produccion_turno(proc->process(frame_sh(t, ciclos, b1), kPfx, 2)));
            for (int i = 0; i < 4; ++i) {
                t += 186; ciclos += 36; b1 += 36;
                totales.push_back(produccion_turno(proc->process(frame_sh(t, ciclos, b1), kPfx, 2)));
            }
        });

        // Rechazo, rechazo, re-anclaje (suma 0) y, desde ahí, cuenta normal: el
        // salto se pierde —ante la duda, subcontar— pero la línea no se congela.
        CHECK(totales == std::vector<int64_t>{36, 36, 36, 72, 108});
        CHECK(count_lines(log, {"[STATE] delta_rejected", "field=metrica_ciclos"}) == 3);
        CHECK(count_lines(log, {"[STATE] reanchor", "field=metrica_ciclos"}) == 1);
    }
}

TEST_CASE("salida horno: L4, la tasa más baja, recupera un hueco de 30 min a ritmo normal") {
    // 500/h × 1800 s × 1,5 = 375: por encima del suelo de 300 y del delta que
    // deja la línea a su ritmo (~22 por trama de 186 s en la captura de
    // septiembre, ~213 en 30 min).
    testsup::pin_local_hour(7);
    REQUIRE(testsup::local_hour_now() == 7);
    reset_all_processor_states();
    reset_global_shift_state();
    testsup::rates_for_tests(500.0, 1.5);

    auto proc = createProcessor(DeviceType::Salida_horno);
    int64_t t = 20 * 60;
    uint16_t ciclos = 12000, b1 = 9000;
    int64_t total = 0;

    const std::string log = capture_streams([&] {
        proc->process(frame_sh(t, ciclos, b1, 4), kPfx, 2);
        for (int i = 0; i < 3; ++i) {
            t += 186; ciclos += 22; b1 += 22;
            proc->process(frame_sh(t, ciclos, b1, 4), kPfx, 2);
        }
        t += 1800; ciclos += 213; b1 += 211;                         // hueco de 30 min
        proc->process(frame_sh(t, ciclos, b1, 4), kPfx, 2);
        t += 186; ciclos += 22; b1 += 22;
        total = produccion_turno(proc->process(frame_sh(t, ciclos, b1, 4), kPfx, 2));
    });

    CHECK(total == 3 * 22 + 213 + 22);
    CHECK(count_lines(log, {"[STATE] delta_rejected"}) == 0);
}
