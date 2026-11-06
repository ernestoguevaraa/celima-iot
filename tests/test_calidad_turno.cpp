// Calidad en la frontera de turno.
//
// El tracking del raw de calidad sobrevive a los cambios de turno, incluido el
// reset global; el acumulador no. El primer delta del turno nuevo —el que cruza
// la frontera— se acredita al turno nuevo si el hueco es <= CELIMA_GAP_SHORT_S
// (900 s por defecto) y se descarta con rastro si es mayor o no es medible.
//
// Antes, CalidadProcessor::reset_states() borraba también el raw, así que la
// primera trama de cada turno re-anclaba con reason=first_message y su delta se
// perdía siempre. En planta (2026-11-05 07:35:51, L2) se llevó ~596 cajas.
//
// Los tests pasan por el mismo enrutado que MqttApp (cambio de turno global y
// reset incluidos) y mueven la hora local entre tramas con pin_local_hour().
#include "doctest.h"
#include "support.hpp"

using json = nlohmann::json;
using testsup::count_lines;
using testsup::capture_streams;

static const std::string kPfx = "celima/punta_hermosa/planta/linea/";
static constexpr int kMode = 2;          // producción: 06–18 / 18–06

namespace {

// Segundos desde las 05:00 locales. base_epoch() es la hora en punto UTC en
// que arrancó la suite; con pin_local_hour(5) esa hora cae a las 05:00.
int64_t at(int h, int m, int s = 0) { return (h - 5) * 3600 + m * 60 + s; }

json frame_cal(int line, int64_t t, uint16_t q1, uint16_t q2, uint16_t q6, uint16_t br)
{
    json m = testsup::make_frame(8, line, 0);
    m["freshBoot"]   = false;
    m["gatewayTime"] = testsup::iso_utc(testsup::base_epoch() + t);
    m["boxesQ1"]     = q1;
    m["boxesQ2"]     = q2;
    m["boxesQ6"]     = q6;
    m["totalBroken"] = br;
    return m;
}

// El enrutado de MqttApp::handle_celima_data, con la hora local fijada.
std::vector<Publication> route(const json& m, int local_hour)
{
    testsup::pin_local_hour(local_hour);
    REQUIRE(testsup::local_hour_now() == local_hour);
    const int sh = static_cast<int>(current_shift_localtime(kMode));
    if (detect_global_shift_change(sh))
        reset_all_processor_states();
    return createProcessor(*deviceTypeFromInt(m["deviceType"].get<int>()))->process(m, kPfx, kMode);
}

json payload(const std::vector<Publication>& pubs, size_t i = 0)
{
    REQUIRE(pubs.size() > i);
    return json::parse(pubs[i].payload);
}

void cold_start()
{
    celima::set_state_store(nullptr);
    reset_all_processor_states();
    reset_global_shift_state();
    testsup::rates_for_tests();
}

} // namespace

TEST_CASE("calidad en curso: el delta de frontera con hueco corto va al turno nuevo") {
    cold_start();
    auto owned = std::make_unique<celima::MemoryStateStore>();
    auto* store = owned.get();
    celima::set_state_store(std::move(owned));

    json p;
    const std::string log = capture_streams([&] {
        route(frame_cal(2, at(5, 54), 2000, 3700, 220, 5900), 5);    // siembra (turno 2)
        route(frame_cal(2, at(5, 57), 2010, 3702, 220, 5950), 5);
        p = payload(route(frame_cal(2, at(6, 0, 30), 2022, 3705, 220, 5960), 6));
    });

    // +12 / +3 / 0 / +10, en 210 s: íntegro al turno 1.
    CHECK(p["shift"] == 1);
    CHECK(p["extra_c1"] == 12);
    CHECK(p["extra_c2"] == 3);
    CHECK(p["comercial"] == 0);
    CHECK(p["quebrados"] == 10);
    CHECK(count_lines(log, {"[STATE] reanchor"}) == 1);           // solo la siembra
    CHECK(count_lines(log, {"[STATE] reanchor", "reason=first_message"}) == 1);
    CHECK(count_lines(log, {"[STATE] reseed", "proc=calidad", "line=2",
                            "reason=shift_change", "shift_prev=?", "shift_new=1"}) == 1);

    // El estado persistido tras el reset conserva el raw: no hubo re-anclaje.
    celima::StoredState row;
    REQUIRE(store->load("calidad", 2, row));
    CHECK(row.shift == 1);
    CHECK(row.state["raw"]["baseline_set"] == true);
    CHECK(row.state["raw"]["last_q1"] == 2022);
    CHECK(row.state["acc"]["q1"] == 12);

    celima::set_state_store(nullptr);
}

TEST_CASE("calidad: un hueco largo que cruza la frontera se descarta con rastro") {
    // El caso de planta: última trama a las 05:38, la siguiente a las 07:35.
    cold_start();
    json p, next;
    const std::string log = capture_streams([&] {
        route(frame_cal(2, at(5, 35), 1700, 3670, 222, 5947), 5);
        route(frame_cal(2, at(5, 38), 1700, 3672, 222, 5947), 5);
        p = payload(route(frame_cal(2, at(7, 35, 51), 2200, 3762, 228, 5977), 7));
        next = payload(route(frame_cal(2, at(7, 38, 51), 2205, 3762, 228, 5977), 7));
    });

    CHECK(p["shift"] == 1);
    CHECK(p["extra_c1"] == 0);                 // se publica, pero en 0
    CHECK(p["extra_c2"] == 0);
    CHECK(p["comercial"] == 0);
    CHECK(p["quebrados"] == 0);
    CHECK(count_lines(log, {"[STATE] reanchor", "proc=calidad", "line=2",
                            "field=boxes_raw", "reason=shift_boundary_gap",
                            "elapsed_s=7071", "d_q1=500", "d_q2=90", "d_q6=6",
                            "d_broken=30"}) == 1);
    CHECK(count_lines(log, {"[STATE] delta_rejected"}) == 0);

    // El raw avanzó: la trama siguiente suma solo lo suyo.
    CHECK(next["extra_c1"] == 5);
}

TEST_CASE("calidad: el umbral es CELIMA_GAP_SHORT_S, inclusive") {
    cold_start();
    const int gap = celima::gap_short_seconds();
    json at_limit, over;
    const std::string log = capture_streams([&] {
        route(frame_cal(3, at(5, 50), 100, 0, 0, 0), 5);
        at_limit = payload(route(frame_cal(3, at(5, 50) + gap, 107, 0, 0, 0), 6));

        // Otra línea, un segundo más allá del límite.
        route(frame_cal(4, at(5, 50), 100, 0, 0, 0), 5);
        over = payload(route(frame_cal(4, at(5, 50) + gap + 1, 107, 0, 0, 0), 6));
    });
    REQUIRE(at(5, 50) + gap >= at(6, 0));      // ambas cruzan la frontera
    CHECK(at_limit["extra_c1"] == 7);
    CHECK(over["extra_c1"] == 0);
    CHECK(count_lines(log, {"reason=shift_boundary_gap", "line=4"}) == 1);
    CHECK(count_lines(log, {"reason=shift_boundary_gap", "line=3"}) == 0);
}

TEST_CASE("calidad: sin gatewayTime el hueco de frontera no es medible y se descarta") {
    cold_start();
    json p;
    const std::string log = capture_streams([&] {
        route(frame_cal(1, at(5, 57), 100, 0, 0, 0), 5);
        json m = frame_cal(1, at(6, 0, 30), 112, 0, 0, 0);
        m.erase("gatewayTime");
        p = payload(route(m, 6));
    });
    CHECK(p["extra_c1"] == 0);
    CHECK(count_lines(log, {"reason=shift_boundary_gap", "elapsed_s=?", "d_q1=12"}) == 1);
}

TEST_CASE("calidad: un cambio de turno a través de un reinicio sigue sin anclar el raw") {
    // restore_calidad_state, caso 2: no cambia con este arreglo. El raw del
    // disco no se restaura, la trama re-ancla y el delta del hueco se pierde.
    cold_start();
    celima::set_state_store(std::make_unique<celima::MemoryStateStore>());
    capture_streams([&] {
        route(frame_cal(2, at(5, 50), 1000, 0, 0, 0), 5);
        route(frame_cal(2, at(5, 53), 1010, 0, 0, 0), 5);
    });

    reset_all_processor_states();              // reinicio: pierde los static…
    reset_global_shift_state();                // …y el raw en memoria

    std::vector<Publication> pubs;
    const std::string log = capture_streams([&] {
        pubs = route(frame_cal(2, at(6, 0, 30), 1020, 0, 0, 0), 6);
    });
    CHECK(pubs.empty());                       // la trama que re-ancla no publica
    CHECK(count_lines(log, {"[STATE] reseed", "reason=shift_change_across_restart"}) == 1);
    CHECK(count_lines(log, {"[STATE] reanchor", "reason=first_message"}) == 1);
    CHECK(count_lines(log, {"reason=shift_boundary_gap"}) == 0);
    celima::set_state_store(nullptr);
}

TEST_CASE("los procesadores de PLC siguen descartando el delta de frontera") {
    cold_start();
    for (int dt = 1; dt <= 7; ++dt) {
        CAPTURE(dt);
        reset_all_processor_states();
        reset_global_shift_state();

        auto plc = [&](int64_t t, int tick) {
            json m = testsup::make_frame(dt, 3, tick);
            m["gatewayTime"] = testsup::iso_utc(testsup::base_epoch() + t);
            if (m.contains("timer1Hz")) m["timer1Hz"] = static_cast<uint16_t>(1000 + t);
            return m;
        };
        std::vector<Publication> pubs;
        capture_streams([&] {
            route(plc(at(5, 54), 0), 5);
            route(plc(at(5, 57), 1), 5);
            pubs = route(plc(at(6, 0, 30), 2), 6);  // +64 en todos los contadores
        });

        const json prod = json::parse(pubs.back().payload);
        int checked = 0;
        for (auto it = prod.begin(); it != prod.end(); ++it) {
            if (it.key().find("_turno") == std::string::npos || !it->is_number()) continue;
            CAPTURE(it.key());
            CHECK(it->get<double>() == 0);
            ++checked;
        }
        CHECK(checked > 0);
    }
}
