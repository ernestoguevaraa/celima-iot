// Mensajes de arranque y diagnóstico del firmware v6 (msgType).
//
// Lo que se protege aquí: que lleguen al tópico /diagnostico con sus campos,
// que los deltas de diag se calculen contra la base correcta y se anulen cuando
// no la hay, y —lo más importante— que intercalarlos en el tráfico normal no
// cambie ni una publicación de producción ni el estado persistido.
#include "doctest.h"
#include "support.hpp"

#include <map>

using json = nlohmann::json;
using testsup::count_lines;
using testsup::capture_streams;

static const std::string kPfx = "celima/punta_hermosa/planta/linea/";

namespace {

json arranque(int dt, int line, const char* causa = "WDT", int codigo = 4, int joins = 2)
{
    json m;
    m["msgType"]             = "arranque";
    m["lineID"]              = line;
    m["deviceType"]          = dt;
    m["causaReinicioCodigo"] = codigo;
    m["causaReinicio"]       = causa;
    m["joinsFallidos"]       = joins;
    m["applicationID"]       = 1;
    m["devEUI"]              = "a8610a34324f740f";
    m["deviceName"]          = "e6-shf";
    m["gatewayTime"]         = "2026-10-25T10:15:53-05:00";
    return m;
}

json diag(int dt, int line, int g, int rc, int ri, int d, const char* tipo = "periodico")
{
    json m;
    m["msgType"]            = "diag";
    m["lineID"]             = line;
    m["deviceType"]         = dt;
    m["tipoDiag"]           = tipo;
    m["glitches"]           = g;
    m["rechazosConteo"]     = rc;
    m["rechazosIntegridad"] = ri;
    m["descartadas"]        = d;
    m["applicationID"]      = 1;
    m["devEUI"]             = "a8610a34324f740f";
    m["deviceName"]         = "e6-shf";
    m["gatewayTime"]        = "2026-10-25T10:15:53-05:00";
    return m;
}

// Una sola publicación esperada: devuelve su payload.
json route_one(const json& m, const std::string& expected_topic)
{
    const auto pubs = process_typed_message(m, kPfx);
    REQUIRE(pubs.has_value());
    REQUIRE(pubs->size() == 1);
    CHECK((*pubs)[0].topic == expected_topic);
    return json::parse((*pubs)[0].payload);
}

// Los cuatro _delta de un diag publicado, null incluidos.
std::vector<json> deltas(const json& p)
{
    return {p["glitches_delta"], p["rechazosConteo_delta"],
            p["rechazosIntegridad_delta"], p["descartadas_delta"]};
}

const std::vector<json> kNulls = {nullptr, nullptr, nullptr, nullptr};

} // namespace

TEST_CASE("diagnostico: arranque de Salida Horno publica en su tópico, con sus campos") {
    reset_diagnostico_state_for_tests();
    json p;
    const std::string log = capture_streams([&] {
        p = route_one(arranque(7, 3), kPfx + "3/salida_horno/diagnostico");
    });

    const json expected = {
        {"tipo", "arranque"}, {"causaReinicio", "WDT"}, {"causaReinicioCodigo", 4},
        {"joinsFallidos", 2}, {"devEUI", "a8610a34324f740f"}, {"deviceName", "e6-shf"},
        {"timestamp_device", "2026-10-25T15:15:53.000Z"}};
    CHECK(p == expected);
    CHECK(count_lines(log, {"[STATE] arranque", "line=3", "proc=salida_horno", "causa=WDT",
                            "joinsFallidos=2", "devEUI=a8610a34324f740f",
                            "deviceName=e6-shf"}) == 1);
}

TEST_CASE("diagnostico: diag de Calidad publica en /calidad/diagnostico, con sus campos") {
    reset_diagnostico_state_for_tests();
    json p;
    const std::string log = capture_streams([&] {
        p = route_one(diag(8, 2, 5, 6, 7, 8), kPfx + "2/calidad/diagnostico");
    });

    const json expected = {
        {"tipo", "periodico"},
        {"glitches", 5}, {"rechazosConteo", 6}, {"rechazosIntegridad", 7}, {"descartadas", 8},
        {"glitches_delta", nullptr}, {"rechazosConteo_delta", nullptr},
        {"rechazosIntegridad_delta", nullptr}, {"descartadas_delta", nullptr},
        {"devEUI", "a8610a34324f740f"}, {"deviceName", "e6-shf"},
        {"timestamp_device", "2026-10-25T15:15:53.000Z"}};
    CHECK(p == expected);
    // Un diag periódico no deja rastro: solo el latido es una alarma.
    CHECK(log.empty());
}

TEST_CASE("diagnostico: arranque de Calidad y diag de Salida Horno") {
    reset_diagnostico_state_for_tests();
    capture_streams([&] {
        const json a = route_one(arranque(8, 1, "BOD", 2, 0), kPfx + "1/calidad/diagnostico");
        CHECK(a["tipo"] == "arranque");
        CHECK(a["causaReinicio"] == "BOD");
        CHECK(a["joinsFallidos"] == 0);

        const json d = route_one(diag(7, 4, 1, 2, 3, 4), kPfx + "4/salida_horno/diagnostico");
        CHECK(d["tipo"] == "periodico");
        CHECK(d["descartadas"] == 4);
    });
}

TEST_CASE("diagnostico: diag → diag → arranque → diag, deltas y null tras el arranque") {
    reset_diagnostico_state_for_tests();
    const std::string topic = kPfx + "1/salida_horno/diagnostico";
    std::vector<json> p;
    capture_streams([&] {
        p.push_back(route_one(diag(7, 1, 10, 0, 1, 2), topic));
        p.push_back(route_one(diag(7, 1, 13, 0, 4, 2), topic));
        p.push_back(route_one(arranque(7, 1), topic));
        p.push_back(route_one(diag(7, 1, 1, 0, 0, 0), topic));
        p.push_back(route_one(diag(7, 1, 2, 3, 0, 1), topic));
    });

    CHECK(deltas(p[0]) == kNulls);                                  // sin base
    CHECK(deltas(p[1]) == std::vector<json>{3, 0, 3, 0});
    CHECK(deltas(p[3]) == kNulls);                                  // el arranque borró la base
    CHECK(deltas(p[4]) == std::vector<json>{1, 3, 0, 1});           // y el diag la re-sembró
    CHECK(p[4]["glitches"] == 2);
}

TEST_CASE("diagnostico: la base es por (línea, deviceType)") {
    reset_diagnostico_state_for_tests();
    capture_streams([&] {
        route_one(diag(7, 1, 10, 10, 10, 10), kPfx + "1/salida_horno/diagnostico");
        // Otra línea y otro equipo: sin base propia, y un arranque suyo no
        // toca la base de L1/salida_horno.
        CHECK(deltas(route_one(diag(7, 2, 50, 50, 50, 50), kPfx + "2/salida_horno/diagnostico"))
              == kNulls);
        route_one(arranque(8, 1), kPfx + "1/calidad/diagnostico");
        CHECK(deltas(route_one(diag(7, 1, 11, 10, 12, 10), kPfx + "1/salida_horno/diagnostico"))
              == std::vector<json>{1, 0, 2, 0});
    });
}

TEST_CASE("diagnostico: un contador que baja sin arranque es un reinicio, null y re-anclaje") {
    reset_diagnostico_state_for_tests();
    const std::string topic = kPfx + "2/salida_horno/diagnostico";
    std::vector<json> p;
    capture_streams([&] {
        p.push_back(route_one(diag(7, 2, 10, 5, 5, 5), topic));
        // Solo rechazosConteo baja: se anulan los cuatro, no solo ese.
        p.push_back(route_one(diag(7, 2, 12, 1, 6, 5), topic));
        p.push_back(route_one(diag(7, 2, 14, 2, 6, 5), topic));
    });

    CHECK(deltas(p[1]) == kNulls);
    CHECK(p[1]["rechazosConteo"] == 1);                             // el acumulado sí se publica
    CHECK(deltas(p[2]) == std::vector<json>{2, 1, 0, 0});           // contra la base re-anclada
}

TEST_CASE("diagnostico: el latido deja la alarma en el journal") {
    reset_diagnostico_state_for_tests();
    json p;
    const std::string log = capture_streams([&] {
        p = route_one(diag(7, 3, 9, 8, 7, 6, "latido"), kPfx + "3/salida_horno/diagnostico");
    });
    CHECK(p["tipo"] == "latido");
    CHECK(count_lines(log, {"[STATE] alarma_sin_datos_validos", "line=3", "proc=salida_horno",
                            "glitches=9", "rechazosConteo=8", "rechazosIntegridad=7",
                            "descartadas=6", "devEUI=a8610a34324f740f",
                            "deviceName=e6-shf"}) == 1);
}

TEST_CASE("diagnostico: sin devEUI ni deviceName publica igual, sin esos campos") {
    reset_diagnostico_state_for_tests();
    json a = arranque(7, 1);
    json d = diag(8, 1, 1, 1, 1, 1);
    for (json* m : {&a, &d}) { m->erase("devEUI"); m->erase("deviceName"); }

    capture_streams([&] {
        const json pa = route_one(a, kPfx + "1/salida_horno/diagnostico");
        const json pd = route_one(d, kPfx + "1/calidad/diagnostico");
        for (const json* p : {&pa, &pd}) {
            CHECK_FALSE(p->contains("devEUI"));
            CHECK_FALSE(p->contains("deviceName"));
            CHECK(p->contains("timestamp_device"));
        }
    });
}

TEST_CASE("diagnostico: msgType desconocido y deviceType desconocido no publican") {
    reset_diagnostico_state_for_tests();
    json raro = diag(7, 1, 1, 1, 1, 1);
    raro["msgType"] = "config";
    json sin_tipo = arranque(42, 1);

    std::optional<std::vector<Publication>> p1, p2, p3;
    const std::string log = capture_streams([&] {
        p1 = process_typed_message(raro, kPfx);
        p2 = process_typed_message(raro, kPfx);       // repetido: silencioso
        p3 = process_typed_message(sin_tipo, kPfx);
    });

    // has_value(): no caen al camino de producción, simplemente no publican.
    REQUIRE(p1.has_value()); CHECK(p1->empty());
    REQUIRE(p2.has_value()); CHECK(p2->empty());
    REQUIRE(p3.has_value()); CHECK(p3->empty());
    CHECK(count_lines(log, {"[STATE] frame_ignored", "reason=msgType_desconocido",
                            "msgType=config"}) == 1);
    CHECK(count_lines(log, {"[STATE] frame_ignored", "reason=deviceType_desconocido",
                            "deviceType=42"}) == 1);
}

TEST_CASE("diagnostico: una trama de producción v6, sin msgType, sigue el camino de siempre") {
    // Trama real de planta (2026-10-25), con los campos que añade el UG65.
    const json m = json::parse(R"({"alarms":7,"applicationID":1,"checksum":30329,)"
        R"("devEUI":"a8610a34324f740f","deviceName":"e6-esm","deviceType":5,)"
        R"("gatewayTime":"2026-10-25T10:15:53-05:00","lineID":3,"metrica_esm_cantidad":26801,)"
        R"("metrica_esm_tiempo_ds":610,"parada_esm_cantidad":27966,"parada_esm_tiempo_s":35472,)"
        R"("timer1Hz":52468})");
    CHECK_FALSE(process_typed_message(m, kPfx).has_value());
}

// ---------------------------------------------------------------------------
// Intercalados en el replay: ni una publicación de producción distinta, ni un
// byte de estado persistido distinto.

namespace {

struct ReplayRun {
    std::string dump;
    std::map<std::string, json> rows;   // "<proc>/<línea>" → estado persistido
    int saves = 0;
};

ReplayRun run_replay(const std::string& path)
{
    testsup::pin_local_hour(10);
    REQUIRE(testsup::local_hour_now() == 10);
    reset_all_processor_states();
    reset_global_shift_state();
    reset_diagnostico_state_for_tests();

    auto owned = std::make_unique<celima::MemoryStateStore>();
    auto* store = owned.get();
    celima::set_state_store(std::move(owned));

    ReplayRun r;
    r.dump = testsup::replay_file(path, 3);
    r.saves = store->save_count();
    for (int dt = 1; dt <= 8; ++dt)
        for (int line = 1; line <= 4; ++line) {
            celima::StoredState st;
            const std::string proc = stationSegment(*deviceTypeFromInt(dt));
            if (!store->load(proc, line, st)) continue;
            // Fuera de la comparación, la hora de pared del servidor: las prensas
            // guardan last_accepted_time = std::time(nullptr) para su ventana de
            // dedup, y dos corridas del replay pueden caer en segundos distintos
            // (fallaba ~1 de cada 6). Es el único campo de estado que no sale de
            // gatewayTime. updated_at también cae al reloj del servidor, pero
            // solo sin gatewayTime, y todas las tramas del fixture lo llevan:
            // aquí es determinista y se compara.
            st.state.erase("last_accepted_time");
            r.rows[proc + "/" + std::to_string(line)] =
                json{{"shift", st.shift}, {"updated_at", st.updated_at}, {"state", st.state}};
        }
    celima::set_state_store(nullptr);
    return r;
}

std::string without_diagnostico(const std::string& dump, int& removed)
{
    std::istringstream in(dump);
    std::ostringstream out;
    std::string line;
    removed = 0;
    while (std::getline(in, line)) {
        if (line.find("/diagnostico\t") != std::string::npos) { ++removed; continue; }
        out << line << '\n';
    }
    return out.str();
}

} // namespace

TEST_CASE("diagnostico: arranque y diag intercalados en el replay no cambian la producción") {
    const std::string fixture = testsup::read_file("tests/data/celima_data_replay.jsonl");
    const std::string golden  = testsup::read_file("tests/data/celima_data_replay.golden");
    REQUIRE(!fixture.empty());
    REQUIRE(!golden.empty());

    // Insertados entre tramas reales, en las mismas claves que van acumulando:
    // L1/salida_horno y L2/calidad, más uno con msgType desconocido.
    const std::vector<std::pair<int, json>> inserts = {
        {5,   arranque(7, 1)},
        {20,  diag(7, 1, 1, 0, 0, 0)},
        {40,  arranque(8, 2, "EXT", 3, 1)},
        {60,  diag(8, 2, 2, 1, 0, 0, "latido")},
        {80,  diag(7, 1, 3, 0, 1, 0)},
        {100, json{{"msgType", "config"}, {"lineID", 1}, {"deviceType", 7}}},
        {300, diag(8, 2, 1, 0, 0, 0)},             // bajó: reinicio sin reporte
    };

    std::istringstream in(fixture);
    std::ostringstream mixed;
    std::string line;
    size_t n = 0, next = 0;
    while (std::getline(in, line)) {
        while (next < inserts.size() && inserts[next].first == static_cast<int>(n))
            mixed << inserts[next++].second.dump() << '\n';
        mixed << line << '\n';
        ++n;
    }
    REQUIRE(next == inserts.size());   // todos dentro del fixture

    const std::string mixed_path = std::string(testsup::tmpdir()) + "/celima_replay_diag.jsonl";
    REQUIRE(testsup::write_file(mixed_path, mixed.str()));

    ReplayRun plain, with_diag;
    capture_streams([&] {
        plain     = run_replay("tests/data/celima_data_replay.jsonl");
        with_diag = run_replay(mixed_path);
    });
    std::remove(mixed_path.c_str());

    int removed = 0;
    const std::string production = without_diagnostico(with_diag.dump, removed);
    CHECK(removed == 6);                       // todos menos el msgType desconocido
    CHECK(plain.dump == golden);
    CHECK(production == golden);

    CHECK(plain.rows.size() == 32);            // 8 procesadores × 4 líneas: no en vacío
    CHECK(with_diag.rows == plain.rows);
    CHECK(with_diag.saves == plain.saves);
}
