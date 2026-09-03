#include "MessageProcessor.hpp"
#include "JsonUtils.hpp"
#include "Logging.hpp"
#include "RateConfig.hpp"
#include "StateStore.hpp"
#include "Shift.hpp"
#include "TimeUtils.hpp"
#include <memory>
#include <sstream>
#include <mutex>
#include <set>
#include <atomic>
#include <cstdlib>
#include <cstring>
#include <iostream>
using json = nlohmann::json;

static std::atomic<int> g_last_global_shift { -1 };

// Claves cuyo estado persistido ya se consultó en ESTE proceso.
//
// No puede vivir en el State: reset_all_processor_states() lo borra en cada
// cambio de turno, y entonces cada clave releería el disco y reportaría un
// "shift_change_across_restart" donde solo hubo un cambio de turno normal.
// Restaurar es un evento de arranque, no de turno.
static std::mutex g_restored_mtx;
static std::set<std::string> g_restored_keys;

static bool mark_restore_attempted(const char* proc, int line)
{
    std::lock_guard<std::mutex> lock(g_restored_mtx);
    return g_restored_keys.insert(std::string(proc) + "/" + std::to_string(line)).second;
}

bool is_decoder_error(const nlohmann::json& msg)
{
    if (!msg.contains("_error")) return false;

    const std::string err = msg["_error"].is_string()
                              ? msg.value("_error", std::string{})
                              : msg["_error"].dump();
    // Se registra el primer error de cada texto distinto y se calla el resto:
    // así un error nuevo del decoder se ve, y los ~1.200 pings diarios no
    // inundan el journal. Descartar en silencio un 10% del tráfico es justo lo
    // que luego nadie puede reconstruir.
    static std::mutex seen_mtx;
    static std::set<std::string> seen;
    bool first = false;
    {
        std::lock_guard<std::mutex> lock(seen_mtx);
        first = seen.insert(err).second;
    }
    if (first) {
        celima::log::state_event("frame_ignored", msg.value("lineID", -1), "decoder",
            "reason=decoder_error deviceType=" +
            std::to_string(msg.value("deviceType", 0)) +
            " devEUI=" + jsonu::get_opt<std::string>(msg, "devEUI").value_or("?") +
            " err=\"" + err + "\"");
    }
    return true;
}

void reset_global_shift_state()
{
    g_last_global_shift.store(-1, std::memory_order_relaxed);
    std::lock_guard<std::mutex> lock(g_restored_mtx);
    g_restored_keys.clear();
}

bool detect_global_shift_change(int currentShift)
{
    int prev = g_last_global_shift.exchange(currentShift, std::memory_order_relaxed);
    if (prev == currentShift)
        return false;

    // Primer mensaje tras arrancar: NO es un cambio de turno.
    //
    // Antes se devolvía true, y el reset_all_processor_states() que provoca
    // borraba el estado que la persistencia acaba de restaurar — sin fallar
    // ninguna prueba que no lo buscara a propósito. El cambio de turno a
    // través de un reinicio lo detecta cada clave por su cuenta, comparando el
    // turno guardado con el actual (caso 2 de la restauración), que además es
    // más preciso que un reset global.
    if (prev == -1) {
        celima::log::state_event("shift_first_observed", -1, "global",
            "shift=" + std::to_string(currentShift));
        return false;
    }

    celima::log::state_event("shift_change_global", -1, "global",
        "shift_prev=" + std::to_string(prev) +
        " shift_new=" + std::to_string(currentShift));
    return true;
}

// Firmware note: the previous Arduino firmware read pseudo-I2C words with a
// 1-bit right shift, halving all values. A workaround masked bit 15 and used
// 15-bit rollover (32768). With the corrected firmware, counters are full
// uint16_t — no masking, rollover at 65536.
// Example: prev=65530, curr=12 → delta=(uint16_t)(12-65530)=18  ✓
//
// El antiguo diff_counter(curr, prev, max_valid) desapareció: su techo fijo es
// justo el defecto D2. Toda la aritmética pasa ahora por diff_counter_scaled().

// FIX: Safe wrapper that only updates 'last' when delta is valid.
// When LoRa devices intermittently send data from alternate PLC register banks,
// the raw value jumps to a completely different range. diff_counter correctly
// rejects these (delta > max_valid), but if we unconditionally update 'last'
// to the anomalous value, the NEXT valid reading also produces a huge delta
// and gets rejected too -- losing two intervals of real counts per anomaly.
//
// FIX v2: Added stale recovery. If max_rejects consecutive messages produce
// rejected deltas (delta > max_valid), force-reset prev_ref to curr.
// This prevents permanent lockout where a single corrupted reading poisons
// prev_ref into a range that makes ALL subsequent valid readings appear as
// huge deltas (via 16-bit wraparound), freezing the accumulator forever.
// ---------------------------------------------------------------------------
// Clasificación de contadores por familia (pendiente P1).
//
// Se lista SOLO lo que es tiempo; todo lo demás es Event por defecto. La
// clasificación no se deduce del sufijo del nombre porque los nombres se
// contradicen entre procesadores: en las prensas `metrica_tiempo` son
// decisegundos (se multiplica por 0,1 al acumular), mientras que en salida
// horno `paradas_tempo` son segundos. La unidad la fija el acumulador destino
// (`acc_*_s` / `acc_*_ds`) y el comentario CF100 del código, no la etiqueta.
//
// REVISAR ANTES DE AMPLIAR: marcar como tiempo un contador de evento agranda su
// cota 40x y es el error peligroso. Ante la duda, no lo listes: Event usa la
// tasa medida y subcuenta, que es el lado seguro.
namespace {

struct FamilyEntry {
    const char*   proc;
    const char*   field;
    CounterFamily family;
};

constexpr CounterFamily kS  = CounterFamily::TimeSeconds;       // 1 tick/s
constexpr CounterFamily kDs = CounterFamily::TimeDeciseconds;   // 10 ticks/s

constexpr FamilyEntry kCounterFamilies[] = {
    // prensas: D29006 son decisegundos (acumulado con ×0,1), D29004 segundos
    {"prensa_hidraulica1", "metrica_tiempo",           kDs},
    {"prensa_hidraulica1", "paradas_tiempo",           kS },
    {"prensa_hidraulica2", "metrica_tiempo",           kDs},
    {"prensa_hidraulica2", "paradas_tiempo",           kS },

    {"entrada_secador",    "timer1Hz",                 kS },
    {"entrada_secador",    "paradas_tempo",            kS },
    {"entrada_secador",    "ingreso_elevador_tiempo",  kDs},
    {"entrada_secador",    "bancalino_l1_tiempo",      kDs},
    {"entrada_secador",    "bancalino_l2_tiempo",      kDs},

    {"salida_secador",     "timer1Hz",                 kS },
    {"salida_secador",     "parada_mds_tiempo",        kS },
    {"salida_secador",     "metrica_mds_tiempo",       kDs},

    {"esmalte",            "timer1Hz",                 kS },
    {"esmalte",            "parada_esm_tiempo",        kS },
    {"esmalte",            "metrica_esm_tiempo",       kDs},

    {"entrada_horno",      "timer1Hz",                 kS },
    {"entrada_horno",      "parada_mcf_tiempo",        kS },
    {"entrada_horno",      "metrica_mcf_tiempo",       kDs},
    {"entrada_horno",      "metrica_formador_tiempo",  kDs},
    {"entrada_horno",      "falha_forno_tiempo",       kS },

    // salida horno publica estos tres con nombres heredados que suenan a
    // cantidad (cantidad_total, cambioSentidoTotal, cambioBarreraTotal), pero
    // el código los anota como CF100 = 0,1 s por tick, igual que sus gemelos de
    // entrada horno. Si el mapa del PLC dijera otra cosa, quitarlos de aquí.
    {"salida_horno",       "timer1Hz",                 kS },
    {"salida_horno",       "paradas_tempo",            kS },
    {"salida_horno",       "metrica_tiempo",           kDs},
    {"salida_horno",       "parada_escolha_tempo",     kS },
    {"salida_horno",       "sentido_escolha_tiempo",   kDs},
    {"salida_horno",       "barreira1_tiempo",         kDs},
};

const char* family_name(CounterFamily f)
{
    switch (f) {
        case CounterFamily::TimeSeconds:      return "tiempo_s";
        case CounterFamily::TimeDeciseconds:  return "tiempo_ds";
        case CounterFamily::Event:            break;
    }
    return "evento";
}

} // namespace

CounterFamily counter_family_for(const char* proc, const char* field)
{
    if (!proc || !field) return CounterFamily::Event;
    for (const auto& e : kCounterFamilies)
        if (std::strcmp(e.proc, proc) == 0 && std::strcmp(e.field, field) == 0)
            return e.family;
    return CounterFamily::Event;
}

// ---------------------------------------------------------------------------
// Cota de plausibilidad escalada por tiempo (D2).
//
// El techo fijo de 5000 se agota en ~3,9 h de producción a la tasa medida de L1
// prensa, así que cualquier hueco mayor descartaba la recuperación legítima.
// Aquí la cota depende del hueco: rate_max_per_s * elapsed_s * margin.
//
// Regla de los empates: ante la duda, subcontar. Un total bajo se investiga;
// uno inflado se reporta como producción real y nadie lo cuestiona.
DeltaResult diff_counter_scaled(uint16_t curr, uint16_t prev, const CounterCtx &ctx)
{
    DeltaResult r;
    r.value = static_cast<uint16_t>(curr - prev);   // misma resta sin máscara que siempre

    // El contador no se movió: no hay nada que acotar.
    if (r.value == 0) {
        r.plausible = true;
        return r;
    }

    // 1) Sin hueco medible no hay cota posible. Cubre el reloj hacia atrás y
    //    los timestamps duplicados. No se "arregla" un tiempo negativo.
    if (ctx.elapsed_s <= 0.0) {
        r.reason = "no_elapsed";
        return r;
    }

    // 2) La tasa depende de la familia del contador. Los de tiempo tienen un
    //    máximo analítico (1 y 10 ticks/s) y NO usan la tasa configurada, que
    //    está dimensionada para producción y los dejaría 40x cortos: tras un
    //    hueco largo se recuperaba la producción pero no el tiempo de operación.
    const CounterFamily family = counter_family_for(ctx.proc, ctx.field);
    double rate_per_s = ctx.rate_max_per_s;
    switch (family) {
        case CounterFamily::TimeSeconds:     rate_per_s = 1.0;  break;
        case CounterFamily::TimeDeciseconds: rate_per_s = 10.0; break;
        case CounterFamily::Event:                              break;
    }

    // Configuración de tasa ausente o absurda: subcontar es el lado seguro.
    // Solo afecta a los contadores de evento; los de tiempo no dependen de ella.
    if (rate_per_s <= 0.0) {
        r.reason = "no_rate";
        return r;
    }

    const double scaled = rate_per_s * ctx.elapsed_s * ctx.margin;

    // 3) Límite duro: si la cota alcanza el módulo, el contador pudo dar más de
    //    una vuelta y el delta es ambiguo por construcción. Sin más aritmética.
    if (scaled >= 65536.0) {
        r.max_plausible = scaled;
        r.reason = "ambiguous_module";
        return r;
    }

    // 4) max_valid se conserva como techo mínimo: la cota escalada solo puede
    //    ser más permisiva que la de hoy, nunca más estricta. Así el camino
    //    normal —intervalos de ~180 s— se comporta exactamente igual que antes.
    r.max_plausible = std::max(scaled, static_cast<double>(ctx.max_valid));
    r.plausible = (static_cast<double>(r.value) <= r.max_plausible);
    if (!r.plausible)
        r.reason = "over_bound";
    return r;
}

// ---------------------------------------------------------------------------
// Persistencia por (procesador, línea): restauración al primer mensaje y
// guardado tras cada mensaje procesado.
//
// La persistencia no puede ser una causa de caída nueva: sin store configurado
// (o con CELIMA_STATE_PERSISTENCE=0) todo esto es un no-op y el servicio se
// comporta como antes de PR 2.

// Devuelve los segundos de turno no observados que aporta este arranque, 0 si
// el hueco fue corto o no hay nada restaurado.
template <typename StateT>
static int64_t restore_state_if_needed(StateT &st, const char* proc, int line,
                                       int shiftNum, int shift_mode,
                                       const std::optional<int64_t>& dev_epoch)
{
    // Sin época de dispositivo no se puede decidir si el estado guardado
    // pertenece a este turno. En lugar de adivinar, se deja el intento sin
    // consumir: lo resuelve la primera trama que traiga gatewayTime utilizable.
    if (!dev_epoch) return 0;

    if (!mark_restore_attempted(proc, line)) return 0;

    celima::IStateStore* store = celima::state_store();
    if (!store) return 0;

    celima::StoredState stored;
    if (!store->load(proc, line, stored)) return 0;   // caso 1: sin estado guardado

    // Mismo turno significa la MISMA instancia de turno, no el mismo número.
    // Un hueco de ~24 h vuelve al mismo número y arrastraría el total del día
    // anterior; los huecos reales de D3 son de 26 h y 31 h.
    const bool misma_instancia =
        stored.updated_at > 0 &&
        shift_start_epoch(stored.updated_at, shift_mode) ==
            shift_start_epoch(*dev_epoch, shift_mode);

    if (stored.shift != shiftNum || !misma_instancia) {
        // Caso 2: el turno cambió mientras el proceso no estaba. Acumuladores a
        // cero y sin arrastre — parte del turno transcurrió sin que nadie
        // escuchara, y ese delta no pertenece al turno nuevo. El estado queda
        // sin inicializar a propósito: lo siembra la rama normal con esta trama.
        celima::log::state_event("reseed", line, proc,
            "reason=shift_change_across_restart shift_prev=" + std::to_string(stored.shift) +
            " shift_new=" + std::to_string(shiftNum) +
            " gap_s=" + std::to_string(stored.updated_at > 0
                                         ? *dev_epoch - stored.updated_at : 0));
        st.suppress_reseed_log = true;    // no duplicar la traza de la rama normal

        // No observado de ESTE turno: desde su arranque hasta la primera trama,
        // no el hueco completo. El hueco puede abarcar turnos enteros y sus
        // segundos no pertenecen al turno en curso — con el hueco entero,
        // turno_segundos_no_observados podía superar la duración del turno.
        const int64_t desde_inicio = *dev_epoch - shift_start_epoch(*dev_epoch, shift_mode);
        return desde_inicio > 0 ? desde_inicio : 0;
    }

    // Casos 3 y 4: mismo turno. Se restaura TODO, incluido el tracking del raw
    // —sin él no hay contra qué diferenciar—, y es la cota escalada la que
    // decide qué hacer con el primer delta. La diferencia entre hueco corto y
    // largo es la traza y los segundos no observados, no la aritmética.
    st.from_json(stored.state);
    st.initialized = true;
    st.shift = shiftNum;

    int64_t gap = 0;
    if (dev_epoch && stored.updated_at > 0)
        gap = *dev_epoch - stored.updated_at;

    if (gap > celima::gap_short_seconds()) {
        celima::log::state_event("gap", line, proc,
            "reason=restart elapsed_s=" + std::to_string(gap) +
            " shift=" + std::to_string(shiftNum));
        return gap;
    }
    celima::log::state_event("restored", line, proc,
        "elapsed_s=" + std::to_string(gap) + " shift=" + std::to_string(shiftNum));
    return 0;
}

// Calidad guarda dos structs bajo la misma clave: RawTrack, que persiste a
// través de los cambios de turno, y ShiftAcc, que se resetea con ellos.
template <typename RawT, typename AccT>
static void persist_calidad_state(const RawT &rt, const AccT &sa, int line, int shift)
{
    celima::IStateStore* store = celima::state_store();
    if (!store) return;
    nlohmann::json j;
    j["v"]   = celima::kStateSchemaVersion;
    j["raw"] = rt.to_json();
    j["acc"] = sa.to_json();
    const int64_t updated_at = (rt.last_accepted_epoch_s > 0)
                                 ? rt.last_accepted_epoch_s
                                 : static_cast<int64_t>(std::time(nullptr));
    store->save("calidad", line, shift, updated_at, j);
}

// Devuelve los segundos no observados que aporta este arranque.
template <typename RawT, typename AccT>
static int64_t restore_calidad_state(RawT &rt, AccT &sa, int line, int shift,
                                     int shift_mode,
                                     const std::optional<int64_t>& dev_epoch)
{
    if (!dev_epoch) return 0;                 // se resuelve en la trama siguiente
    if (!mark_restore_attempted("calidad", line)) return 0;

    celima::IStateStore* store = celima::state_store();
    if (!store) return 0;

    celima::StoredState stored;
    if (!store->load("calidad", line, stored)) return 0;
    if (!stored.state.contains("raw") || !stored.state.contains("acc")) return 0;

    const int64_t gap = (stored.updated_at > 0) ? (*dev_epoch - stored.updated_at) : 0;
    const bool misma_instancia =
        stored.updated_at > 0 &&
        shift_start_epoch(stored.updated_at, shift_mode) ==
            shift_start_epoch(*dev_epoch, shift_mode);

    if (stored.shift != shift || !misma_instancia) {
        // Caso 2: el turno cambió con el proceso caído. Acumulador a cero y
        // SIN arrastre, igual que los siete procesadores de máquina.
        //
        // El raw tampoco se restaura aquí: con el baseline puesto, el delta de
        // todo el corte se calcularía contra el valor de antes del hueco y se
        // acreditaría íntegro al turno nuevo. Dejándolo sin anclar, la rama
        // normal re-ancla con esta trama y el delta del hueco se descarta, que
        // es lo que hacen los demás. (Fuera del caso 2 el raw sí persiste a
        // través de los cambios de turno, que es su diseño.)
        celima::log::state_event("reseed", line, "calidad",
            "reason=shift_change_across_restart shift_prev=" + std::to_string(stored.shift) +
            " shift_new=" + std::to_string(shift) +
            " gap_s=" + std::to_string(gap));
        sa.suppress_reseed_log = true;
        const int64_t desde_inicio = *dev_epoch - shift_start_epoch(*dev_epoch, shift_mode);
        return desde_inicio > 0 ? desde_inicio : 0;
    }

    // El tracking del raw persiste a través de los cambios de turno.
    rt.from_json(stored.state["raw"]);
    sa.from_json(stored.state["acc"]);
    sa.initialized = true;
    sa.shift = shift;

    if (gap > celima::gap_short_seconds()) {
        celima::log::state_event("gap", line, "calidad",
            "reason=restart elapsed_s=" + std::to_string(gap) +
            " shift=" + std::to_string(shift));
        return gap;
    }
    celima::log::state_event("restored", line, "calidad",
        "elapsed_s=" + std::to_string(gap) + " shift=" + std::to_string(shift));
    return 0;
}

template <typename StateT>
static void persist_state(const StateT &st, const char* proc, int line, int shiftNum)
{
    celima::IStateStore* store = celima::state_store();
    if (!store) return;
    const int64_t updated_at = (st.last_accepted_epoch_s > 0)
                                 ? st.last_accepted_epoch_s
                                 : static_cast<int64_t>(std::time(nullptr));
    store->save(proc, line, shiftNum, updated_at, st.to_json());
}

// ---------------------------------------------------------------------------
// Marcador de turno incompleto (apartado G del PR 2).
//
// SUJETO A UNA DECISIÓN ABIERTA: no se sabe si el boxer-patrol-edge-processor
// propaga campos desconocidos hacia InfluxDB y AWS o los descarta. Un marcador
// que se pierde en el siguiente salto no sirve de nada, y un campo nuevo en el
// payload no se puede retirar de lo ya enviado.
//
// Por eso queda tras CELIMA_INCOMPLETE_SHIFT_MARKER=1: la contabilidad de
// segundos no observados se lleva siempre (y se ve en los eventos [STATE] gap),
// pero el campo solo se publica cuando alguien lo activa a conciencia.
// Añadir un campo nuevo es seguro; redefinir uno existente, no.
static bool g_marker_override_set = false;
static bool g_marker_override = false;

void set_incomplete_shift_marker_for_tests(bool on)
{
    g_marker_override_set = true;
    g_marker_override = on;
}

void clear_incomplete_shift_marker_override()
{
    g_marker_override_set = false;
}

static bool publish_incomplete_shift_marker()
{
    if (g_marker_override_set) return g_marker_override;
    static const bool on = [] {
        const char* v = std::getenv("CELIMA_INCOMPLETE_SHIFT_MARKER");
        return v && v[0] == '1';
    }();
    return on;
}

static void add_unobserved_marker(nlohmann::json &prod, int64_t unobserved_s)
{
    if (publish_incomplete_shift_marker())
        prod["turno_segundos_no_observados"] = unobserved_s;
}

uint32_t safe_delta_u16(uint16_t prev, uint16_t curr, const CounterCtx &ctx)
{
    const DeltaResult r = diff_counter_scaled(curr, prev, ctx);
    if (r.plausible)
        return r.value;
    if (r.value == 0)
        return 0;

    celima::log::state_event("delta_rejected", ctx.line, ctx.proc,
        "field=" + std::string(ctx.field) +
        " prev=" + std::to_string(prev) +
        " curr=" + std::to_string(curr) +
        " raw_delta=" + std::to_string(r.value) +
        " max_plausible=" + std::to_string(r.max_plausible) +
        " elapsed_s=" + std::to_string(static_cast<int64_t>(ctx.elapsed_s)) +
        " family=" + family_name(counter_family_for(ctx.proc, ctx.field)) +
        " reason=" + r.reason);
    return 0;
}

//
// Observabilidad: los parámetros line/proc/field solo etiquetan los eventos
// [STATE]; no intervienen en la aritmética. Tienen valor por defecto para que
// una llamada nueva sin contexto compile y se delate en el log como
// "line=-1 proc=? field=?" en lugar de perderse en silencio.
static uint16_t diff_counter_safe(uint16_t curr, uint16_t &prev_ref,
                                   uint8_t &reject_count,
                                   const CounterCtx &ctx) {
    const DeltaResult r = diff_counter_scaled(curr, prev_ref, ctx);
    if (r.plausible) {
        prev_ref = curr;
        reject_count = 0;
        return r.value;
    }

    // Implausible: puede ser un cero genuino (el contador no se movió) o un
    // rechazo de la cota. Solo el rechazo cuenta para el re-anclaje.
    if (r.value == 0)
        return 0;

    const uint16_t prev_before = prev_ref;
    reject_count++;
    celima::log::state_event("delta_rejected", ctx.line, ctx.proc,
        "field=" + std::string(ctx.field) +
        " prev=" + std::to_string(prev_before) +
        " curr=" + std::to_string(curr) +
        " raw_delta=" + std::to_string(r.value) +
        " max_plausible=" + std::to_string(r.max_plausible) +
        " elapsed_s=" + std::to_string(static_cast<int64_t>(ctx.elapsed_s)) +
        " family=" + family_name(counter_family_for(ctx.proc, ctx.field)) +
        " reason=" + r.reason +
        " reject_count=" + std::to_string(static_cast<int>(reject_count)));
    if (reject_count >= ctx.max_rejects) {
        // Stale recovery: prev_ref is stuck in an unrecoverable range.
        // Force-reset so next message can compute a valid delta.
        prev_ref = curr;
        reject_count = 0;
        celima::log::state_event("reanchor", ctx.line, ctx.proc,
            "field=" + std::string(ctx.field) +
            " prev=" + std::to_string(prev_before) +
            " curr=" + std::to_string(curr));
    }
    return 0;
}


// Updates an EMA used for corrupt-frame detection.
// Ignores delta==0 (legitimate stall) so stops don't pull the average down.
static void spike_ema_update(float &ema, uint16_t delta) {
    if (delta == 0) return;
    constexpr float alpha = 0.2f;
    ema = (ema < 0.0f) ? static_cast<float>(delta)
                       : alpha * static_cast<float>(delta) + (1.0f - alpha) * ema;
}

// Returns true when raw_delta looks like a corruption spike.
// raw_delta == 0 is never a spike (stall). Above zero the threshold is
// max(floor_thresh, ema * factor); if ema is uninitialized (<0) only floor applies.
static bool spike_detected(uint16_t raw_delta, float ema,
                           uint16_t floor_thresh, float factor) {
    if (raw_delta == 0) return false;
    float thresh = (ema < 0.0f) ? static_cast<float>(floor_thresh)
                                : std::max(static_cast<float>(floor_thresh), ema * factor);
    return static_cast<float>(raw_delta) > thresh;
}

static Publication make_pub(const std::string &topic, const json &j)
{
    return Publication{topic, j.dump()};
}

/** Default processor: lightly normalize and forward a summary. */
class DefaultProcessor : public IMessageProcessor
{
public:
    std::vector<Publication> process(const json &msg, const std::string &isa95_prefix, int shift_mode = 3) override
    {

        json out;
        out["source"] = "celima/data";
        out["observed"] = msg;

        // Put some commonly useful fields if present
        if (auto dev = jsonu::get_opt<std::string>(msg, "devEUI"))
            out["devEUI"] = *dev;
        if (auto dn = jsonu::get_opt<std::string>(msg, "deviceName"))
            out["deviceName"] = *dn;
        if (auto dt = jsonu::get_opt<int>(msg, "deviceType"))
            out["deviceType"] = *dt;

        // Example: publish to a "production" topic
        auto t1 = isa95_prefix + "/production/line/quantity";
        json p1;
        p1["quantity"] = jsonu::get_opt<int>(msg, "cantidad").value_or(0);
        p1["ts"] = std::time(nullptr);

        // Example: publish to a "quality/alarms" topic
        auto t2 = isa95_prefix + "/quality/alarms";
        json p2;
        p2["alarms"] = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        p2["ts"] = std::time(nullptr);

        return {make_pub(t1, p1), make_pub(t2, p2)};
    }
};

/**
 * CalidadShiftAccumulatorProcessor (Updated for 3-minute accumulated data):
 * - Receives accumulated counts every 3 minutes (boxesQ1, boxesQ2, boxesQ6, totalBroken)
 * - Maintains per-shift accumulators for qualities 1, 2, 6 and discarded ("quebrados")
 * - Resets accumulators when the shift changes (S1->S2->S3->S1...)
 * - Thread-safe via a static mutex (safe even if processor instances are recreated)
 * 
 * Input JSON format (new):
 * {
 *   "lineID": 1,
 *   "deviceType": 8,
 *   "boxesQ1": 10,
 *   "boxesQ2": 5,
 *   "boxesQ6": 2,
 *   "totalBroken": 3
 * }
 * 
 * Output JSON format (unchanged):
 * {
 *   "maquina_id": 8,
 *   "timestamp_device": "2024-12-21T20:30:00Z",
 *   "shift": 1,
 *   "lineID": 1,
 *   "extra_c1": 150,    // accumulated Q1 for current shift
 *   "extra_c2": 80,     // accumulated Q2 for current shift
 *   "comercial": 40,    // accumulated Q6 for current shift
 *   "quebrados": 25     // accumulated broken for current shift
 * }
 */
class CalidadProcessor final : public IMessageProcessor {
    // ========================================================================
    //  v4 — ACUMULADORES MONOTONICOS
    //  El firmware ahora envia contadores monotonicos (no se resetean por TX).
    //  El delta por intervalo se calcula aqui con safe_delta_u16 (maneja rollover
    //  de 16 bits). Si se pierde un paquete, el siguiente recupera el conteo.
    //
    //  Separacion clave de estado:
    //   - RawTrack: tracking del raw monotonico. PERSISTE a traves de cambios de
    //     turno. Solo se re-ancla en freshBoot (reinicio del equipo) o primer pkt.
    //   - ShiftAcc: acumulador del turno. SE RESETEA en cambio de turno. El delta
    //     que cruza el limite de turno se asigna integro al turno nuevo (status
    //     quo: error <=1 intervalo) pero NUNCA se pierde.
    //
    //  Dedup: por gatewayTime + raw identico (duplicado republicado por el NS),
    //  reemplaza la antigua ventana de 120s (que borraba produccion estable).
    // ========================================================================

    // Umbrales de delta para recuperar hasta ~10 intervalos perdidos:
    static constexpr int MAXR_BOXES  = 300;   // ~25/int * 10 + margen
    static constexpr int MAXR_BROKEN = 1200;  // picos ~100/int * 10 + margen

    struct RawTrack {
        uint16_t last_q1 = 0, last_q2 = 0, last_q6 = 0, last_broken = 0;
        bool baseline_set = false;
        // Época del último mensaje ACEPTADO, para medir el hueco (D2).
        // Semántica y unidad distintas a last_accepted_time (dedup): no reutilizar.
        int64_t last_accepted_epoch_s = 0;
        std::string last_gateway_time;

        // Segundos del turno en curso que nadie observó (huecos por reinicio).
        int64_t acc_unobserved_s = 0;
        // Traza de re-siembra ya emitida por la restauración. No se serializa.
        bool suppress_reseed_log = false;

        // Serialización generada a partir de los miembros del struct: se
        // persisten TODOS (acumuladores, tracking de raw, contadores de
        // rechazo, EMA y estado de deduplicación), no solo los acc_*.
        nlohmann::json to_json() const {
            nlohmann::json j;
            j["v"] = celima::kStateSchemaVersion;
            j["acc_unobserved_s"] = acc_unobserved_s;
            j["last_q1"] = last_q1;
            j["last_q2"] = last_q2;
            j["last_q6"] = last_q6;
            j["last_broken"] = last_broken;
            j["baseline_set"] = baseline_set;
            j["last_accepted_epoch_s"] = last_accepted_epoch_s;
            j["last_gateway_time"] = last_gateway_time;
            return j;
        }

        void from_json(const nlohmann::json &j) {
            acc_unobserved_s = j.value("acc_unobserved_s", acc_unobserved_s);
            last_q1 = j.value("last_q1", last_q1);
            last_q2 = j.value("last_q2", last_q2);
            last_q6 = j.value("last_q6", last_q6);
            last_broken = j.value("last_broken", last_broken);
            baseline_set = j.value("baseline_set", baseline_set);
            last_accepted_epoch_s = j.value("last_accepted_epoch_s", last_accepted_epoch_s);
            last_gateway_time = j.value("last_gateway_time", last_gateway_time);
        }
    };
    struct ShiftAcc {
        uint64_t q1 = 0, q2 = 0, q6 = 0, discarded = 0;
        int shift = -1;
        bool initialized = false;

        // Segundos del turno en curso que nadie observó (huecos por reinicio).
        int64_t acc_unobserved_s = 0;
        // Traza de re-siembra ya emitida por la restauración. No se serializa.
        bool suppress_reseed_log = false;

        // Serialización generada a partir de los miembros del struct: se
        // persisten TODOS (acumuladores, tracking de raw, contadores de
        // rechazo, EMA y estado de deduplicación), no solo los acc_*.
        nlohmann::json to_json() const {
            nlohmann::json j;
            j["v"] = celima::kStateSchemaVersion;
            j["acc_unobserved_s"] = acc_unobserved_s;
            j["q1"] = q1;
            j["q2"] = q2;
            j["q6"] = q6;
            j["discarded"] = discarded;
            j["shift"] = shift;
            j["initialized"] = initialized;
            return j;
        }

        void from_json(const nlohmann::json &j) {
            acc_unobserved_s = j.value("acc_unobserved_s", acc_unobserved_s);
            q1 = j.value("q1", q1);
            q2 = j.value("q2", q2);
            q6 = j.value("q6", q6);
            discarded = j.value("discarded", discarded);
            shift = j.value("shift", shift);
            initialized = j.value("initialized", initialized);
        }
    };

    static std::mutex mtx_;
    static std::unordered_map<int, RawTrack> raw_;     // por lineID, persiste turnos
    static std::unordered_map<int, ShiftAcc> states_;  // por lineID, resetea turnos

public:
    static void reset_states();

    std::vector<Publication> process(const json& msg,
                                     const std::string& isa95_prefix,
                                     int shift_mode = 3) override {
        const int shift_now = static_cast<int>(current_shift_localtime(shift_mode));
        const int line_id   = msg.value("lineID", 0);
        const auto dev_epoch = device_epoch_s(msg);
        int64_t unobserved_s = 0;   // segundos del turno sin observar
        const std::string gw_time = msg.value("gatewayTime", std::string{});

        // ---- Extraer valores del payload ----
        // v4: contadores monotonicos uint16. Formato viejo (cajaCalidad) se trata
        // como delta directo de 1 evento por compatibilidad.
        bool   is_monotonic = false;
        uint16_t raw_q1 = 0, raw_q2 = 0, raw_q6 = 0, raw_broken = 0;
        bool   fresh_boot = msg.value("freshBoot", false);

        // delta directo (solo formato viejo cajaCalidad)
        uint64_t direct_q1 = 0, direct_q2 = 0, direct_q6 = 0, direct_broken = 0;

        if (msg.contains("boxesQ1")) {
            // NUEVO FORMATO v4: acumuladores monotonicos
            is_monotonic = true;
            raw_q1     = static_cast<uint16_t>(msg.value("boxesQ1", 0));
            raw_q2     = static_cast<uint16_t>(msg.value("boxesQ2", 0));
            raw_q6     = static_cast<uint16_t>(msg.value("boxesQ6", 0));
            raw_broken = static_cast<uint16_t>(msg.value("totalBroken", 0));
        }
        else if (msg.contains("cajaCalidad")) {
            // FORMATO VIEJO: un evento por mensaje (delta directo)
            const int cajaCalidad = msg.value("cajaCalidad", 0);
            if      (cajaCalidad == 1) direct_q1 = 1;
            else if (cajaCalidad == 2) direct_q2 = 1;
            else if (cajaCalidad == 6) direct_q6 = 1;
            const int quebrados = msg.contains("quebrados")
                                    ? msg.value("quebrados", 0)
                                    : msg.value("quebrado", 0);
            if (quebrados > 0) direct_broken = static_cast<uint64_t>(quebrados);
        }

        uint64_t q1, q2, q6, disc;
        {
            std::lock_guard<std::mutex> lock(mtx_);
            auto& rt = raw_[line_id];
            auto& sa = states_[line_id];

            // Restauración del estado persistido, una vez por clave y arranque.
            const int64_t restart_gap_s =
                restore_calidad_state(rt, sa, line_id, shift_now, shift_mode, dev_epoch);
            if (restart_gap_s > 0 && sa.initialized)
                sa.acc_unobserved_s += restart_gap_s;   // caso 4: hueco dentro del turno

            if (is_monotonic) {
                // ---- 1) Dedup por gatewayTime + raw identico ----
                if (!gw_time.empty() &&
                    gw_time == rt.last_gateway_time &&
                    rt.baseline_set &&
                    raw_q1 == rt.last_q1 && raw_q2 == rt.last_q2 &&
                    raw_q6 == rt.last_q6 && raw_broken == rt.last_broken) {
                    std::cout << "[Calidad] Duplicado NS descartado (lineID=" << line_id
                              << " gwTime=" << gw_time << ")\n";
                    return {};
                }

                // ---- 2) Reset del acumulador SOLO en cambio de turno ----
                if (!sa.initialized || sa.shift != shift_now) {
                    // reseed: una vez por procesador y línea, antes de pisar el estado.
                    // suppress_reseed_log lo pone la restauración cuando ya emitió la
                    // traza del cambio de turno a través del reinicio.
                    if (!sa.suppress_reseed_log)
                        celima::log::state_event("reseed", line_id, "calidad",
                            sa.initialized
                                ? ("reason=shift_change shift_prev=" + std::to_string(sa.shift) +
                                   " shift_new=" + std::to_string(shift_now))
                                : ("reason=first_message shift=" + std::to_string(shift_now)));
                    sa = ShiftAcc();
                    sa.acc_unobserved_s = restart_gap_s;
                    sa.initialized = true;
                    sa.shift = shift_now;
                }

                // ---- 3) freshBoot o sin baseline: re-anclar, no emitir delta ----
                if (fresh_boot || !rt.baseline_set) {
                    rt.last_q1 = raw_q1; rt.last_q2 = raw_q2;
                    rt.last_q6 = raw_q6; rt.last_broken = raw_broken;
                    rt.baseline_set = true;
                    rt.last_gateway_time = gw_time;
                    if (dev_epoch) rt.last_accepted_epoch_s = *dev_epoch;
                    // Re-ancla del tracking del raw: no es el acumulador de turno,
                    // pero descarta el delta de este mensaje y por eso deja rastro.
                    celima::log::state_event("reanchor", line_id, "calidad",
                        std::string("field=boxes_raw reason=") +
                        (fresh_boot ? "fresh_boot" : "first_message") +
                        " q1=" + std::to_string(raw_q1) +
                        " q2=" + std::to_string(raw_q2) +
                        " q6=" + std::to_string(raw_q6) +
                        " broken=" + std::to_string(raw_broken));
                    persist_calidad_state(rt, sa, line_id, shift_now);
                    return {};
                }

                // ---- 4) Delta normal via safe_delta_u16 (rollover-safe) ----
                // Cota escalada por tiempo: los MAXR_* quedan como techo mínimo.
                CounterCtx ctx{};
                ctx.line = line_id;
                ctx.proc = "calidad";
                ctx.rate_max_per_s = celima::rates().rate_per_s(line_id, ctx.proc);
                ctx.margin         = celima::rates().margin();
                if (dev_epoch && rt.last_accepted_epoch_s > 0)
                    ctx.elapsed_s = static_cast<double>(*dev_epoch - rt.last_accepted_epoch_s);

                uint32_t d_q1 = safe_delta_u16(rt.last_q1, raw_q1, ctx.with("boxesQ1", MAXR_BOXES));
                uint32_t d_q2 = safe_delta_u16(rt.last_q2, raw_q2, ctx.with("boxesQ2", MAXR_BOXES));
                uint32_t d_q6 = safe_delta_u16(rt.last_q6, raw_q6, ctx.with("boxesQ6", MAXR_BOXES));
                uint32_t d_br = safe_delta_u16(rt.last_broken, raw_broken,
                                               ctx.with("totalBroken", MAXR_BROKEN));

                // ---- 5) Actualizar tracking del raw (persiste turnos) ----
                rt.last_q1 = raw_q1; rt.last_q2 = raw_q2;
                rt.last_q6 = raw_q6; rt.last_broken = raw_broken;
                rt.last_gateway_time = gw_time;
                if (dev_epoch) rt.last_accepted_epoch_s = *dev_epoch;

                // ---- 6) Sumar deltas al acumulador del turno ----
                sa.q1 += d_q1; sa.q2 += d_q2; sa.q6 += d_q6; sa.discarded += d_br;
            }
            else {
                // FORMATO VIEJO: delta directo. Solo reset por turno.
                if (!sa.initialized || sa.shift != shift_now) {
                    // reseed: una vez por procesador y línea, antes de pisar el estado.
                    // suppress_reseed_log lo pone la restauración cuando ya emitió la
                    // traza del cambio de turno a través del reinicio.
                    if (!sa.suppress_reseed_log)
                        celima::log::state_event("reseed", line_id, "calidad",
                            sa.initialized
                                ? ("reason=shift_change shift_prev=" + std::to_string(sa.shift) +
                                   " shift_new=" + std::to_string(shift_now))
                                : ("reason=first_message shift=" + std::to_string(shift_now)));
                    sa = ShiftAcc();
                    sa.acc_unobserved_s = restart_gap_s;
                    sa.initialized = true;
                    sa.shift = shift_now;
                }
                sa.q1 += direct_q1; sa.q2 += direct_q2;
                sa.q6 += direct_q6; sa.discarded += direct_broken;
            }

            q1 = sa.q1; q2 = sa.q2; q6 = sa.q6; disc = sa.discarded;
            unobserved_s = sa.acc_unobserved_s;

            // Guardar tras procesar el mensaje, dentro del mismo mutex que
            // protege el estado. Calidad guarda sus dos structs bajo la misma
            // clave: RawTrack (persiste turnos) y ShiftAcc (se resetea).
            persist_calidad_state(rt, sa, line_id, shift_now);
        }

        // ---- Salida (formato sin cambios) ----
        json out;
        out["maquina_id"]       = 8;
        out["timestamp_device"] = device_timestamp(msg);
        out["shift"]            = shift_now;
        out["lineID"]           = line_id;
        out["extra_c1"]   = q1;
        out["extra_c2"]   = q2;
        out["comercial"]  = q6;
        out["quebrados"]  = disc;
        add_unobserved_marker(out, unobserved_s);

        const auto t1 = isa95_prefix + std::to_string(line_id) + "/calidad/production";
        return { make_pub(t1, out) };
    }
};

// Static definitions
std::mutex CalidadProcessor::mtx_;
std::unordered_map<int, CalidadProcessor::RawTrack> CalidadProcessor::raw_;
std::unordered_map<int, CalidadProcessor::ShiftAcc> CalidadProcessor::states_;

void CalidadProcessor::reset_states() {
    std::lock_guard<std::mutex> lock(mtx_);
    raw_.clear();
    states_.clear();
}

// ============================================================================
// PrensaHidraulica1Processor - Fixed with correct counter handling
// ============================================================================
/**
 * PLC Register Mapping (from salida_prensa_1.pdf Sección12):
 * 
 * Input fields (from decoder, TODO: update decoder field names):
 * - "cantidadProductos" = D29005 = PISADAS (press stroke count, NOT products!)
 * - "tiempoProduccion_ds" = D29006 = Metric time accumulator (DECISECONDS)
 * - "paradas" = D29003 = Stop event COUNT
 * - "tiempoParadas_s" = D29004 = Stop duration (SECONDS)
 * - "alarms" = D29002 = Status Lento (status bits)
 * 
 * The PLC calculates: Products = PISADAS × Fila × Pac
 * We apply: Products = PISADAS × factor_pisadas (per line)
 */
class PrensaHidraulica1Processor : public IMessageProcessor
{
    struct PH1State {
        bool initialized = false;
        int  shift       = -1;

        uint16_t    last_dd_pisadas        = 0;
        uint16_t    last_dd_tiempo         = 0;
        uint16_t    last_dd_paradas        = 0;
        uint16_t    last_dd_tiempo_paradas = 0;
        std::time_t last_accepted_time     = 0;
        // Época del último mensaje ACEPTADO, para medir el hueco (D2).
        // Semántica y unidad distintas a last_accepted_time (dedup): no reutilizar.
        int64_t last_accepted_epoch_s = 0;

        // Counters are 16-bit with bit-15 validation
        uint16_t last_pisadas = 0;        // D29005 - PISADAS (press strokes)
        uint8_t  rc_pisadas = 0;
        uint32_t acc_pisadas = 0;

        uint16_t last_metrica_tiempo = 0; // D29006 - Metric time (deciseconds)
        uint8_t  rc_metrica_tiempo = 0;
        double   acc_metrica_tiempo_s = 0.0;

        uint16_t last_paradas_count = 0;  // D29003 - Stop count
        uint8_t  rc_paradas_count = 0;
        uint32_t acc_paradas_count = 0;

        uint16_t last_paradas_tiempo = 0; // D29004 - Stop time (seconds)
        uint8_t  rc_paradas_tiempo = 0;
        uint32_t acc_paradas_tiempo_s = 0;

        // Segundos del turno en curso que nadie observó (huecos por reinicio).
        int64_t acc_unobserved_s = 0;
        // Traza de re-siembra ya emitida por la restauración. No se serializa.
        bool suppress_reseed_log = false;

        // Serialización generada a partir de los miembros del struct: se
        // persisten TODOS (acumuladores, tracking de raw, contadores de
        // rechazo, EMA y estado de deduplicación), no solo los acc_*.
        nlohmann::json to_json() const {
            nlohmann::json j;
            j["v"] = celima::kStateSchemaVersion;
            j["acc_unobserved_s"] = acc_unobserved_s;
            j["initialized"] = initialized;
            j["shift"] = shift;
            j["last_dd_pisadas"] = last_dd_pisadas;
            j["last_dd_tiempo"] = last_dd_tiempo;
            j["last_dd_paradas"] = last_dd_paradas;
            j["last_dd_tiempo_paradas"] = last_dd_tiempo_paradas;
            j["last_accepted_time"] = last_accepted_time;
            j["last_accepted_epoch_s"] = last_accepted_epoch_s;
            j["last_pisadas"] = last_pisadas;
            j["rc_pisadas"] = rc_pisadas;
            j["acc_pisadas"] = acc_pisadas;
            j["last_metrica_tiempo"] = last_metrica_tiempo;
            j["rc_metrica_tiempo"] = rc_metrica_tiempo;
            j["acc_metrica_tiempo_s"] = acc_metrica_tiempo_s;
            j["last_paradas_count"] = last_paradas_count;
            j["rc_paradas_count"] = rc_paradas_count;
            j["acc_paradas_count"] = acc_paradas_count;
            j["last_paradas_tiempo"] = last_paradas_tiempo;
            j["rc_paradas_tiempo"] = rc_paradas_tiempo;
            j["acc_paradas_tiempo_s"] = acc_paradas_tiempo_s;
            return j;
        }

        void from_json(const nlohmann::json &j) {
            acc_unobserved_s = j.value("acc_unobserved_s", acc_unobserved_s);
            initialized = j.value("initialized", initialized);
            shift = j.value("shift", shift);
            last_dd_pisadas = j.value("last_dd_pisadas", last_dd_pisadas);
            last_dd_tiempo = j.value("last_dd_tiempo", last_dd_tiempo);
            last_dd_paradas = j.value("last_dd_paradas", last_dd_paradas);
            last_dd_tiempo_paradas = j.value("last_dd_tiempo_paradas", last_dd_tiempo_paradas);
            last_accepted_time = j.value("last_accepted_time", last_accepted_time);
            last_accepted_epoch_s = j.value("last_accepted_epoch_s", last_accepted_epoch_s);
            last_pisadas = j.value("last_pisadas", last_pisadas);
            rc_pisadas = j.value("rc_pisadas", rc_pisadas);
            acc_pisadas = j.value("acc_pisadas", acc_pisadas);
            last_metrica_tiempo = j.value("last_metrica_tiempo", last_metrica_tiempo);
            rc_metrica_tiempo = j.value("rc_metrica_tiempo", rc_metrica_tiempo);
            acc_metrica_tiempo_s = j.value("acc_metrica_tiempo_s", acc_metrica_tiempo_s);
            last_paradas_count = j.value("last_paradas_count", last_paradas_count);
            rc_paradas_count = j.value("rc_paradas_count", rc_paradas_count);
            acc_paradas_count = j.value("acc_paradas_count", acc_paradas_count);
            last_paradas_tiempo = j.value("last_paradas_tiempo", last_paradas_tiempo);
            rc_paradas_tiempo = j.value("rc_paradas_tiempo", rc_paradas_tiempo);
            acc_paradas_tiempo_s = j.value("acc_paradas_tiempo_s", acc_paradas_tiempo_s);
        }
    };

    static std::mutex mtx_;
    static std::unordered_map<int, PH1State> states_;

public:
    static void reset_states() {
        std::lock_guard<std::mutex> lock(mtx_);
        states_.clear();
    }

    std::vector<Publication> process(const json &msg,
                                     const std::string &isa95_prefix,
                                     int shift_mode = 3) override
    {
        auto sh = current_shift_localtime(shift_mode);
        int shiftNum = (sh == Shift::S1 ? 1 : (sh == Shift::S2 ? 2 : 3));

        // Read inputs from decoder
        // NOTE: Field names will change when decoder is updated
        // Current: cantidadProductos → Should be: pisadas
        int line          = jsonu::get_opt<int>(msg, "lineID").value_or(0);
        const auto dev_epoch = device_epoch_s(msg);
        int64_t unobserved_s = 0;   // segundos del turno sin observar
        int alarms        = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        
        // D29005 - PISADAS (decoder currently calls this "cantidadProductos")
        int raw_pisadas   = jsonu::get_opt<int>(msg, "cantidadProductos").value_or(0);
        
        // D29006 - Metric time in deciseconds
        int raw_tiempo    = jsonu::get_opt<int>(msg, "tiempoProduccion_ds").value_or(0);
        
        // D29003 - Stop count
        int raw_paradas   = jsonu::get_opt<int>(msg, "paradas").value_or(0);
        
        // D29004 - Stop time in seconds
        int raw_tiempo_paradas = jsonu::get_opt<int>(msg, "tiempoParadas_s").value_or(0);

        // Cast to uint16_t
        uint16_t pisadas = static_cast<uint16_t>(raw_pisadas);
        uint16_t metrica_tiempo = static_cast<uint16_t>(raw_tiempo);
        uint16_t paradas_count = static_cast<uint16_t>(raw_paradas);
        uint16_t paradas_tiempo = static_cast<uint16_t>(raw_tiempo_paradas);

        // Output accumulators
        uint32_t acc_pisadas_out = 0;
        double   acc_metrica_tiempo_s_out = 0.0;
        uint32_t acc_paradas_count_out = 0;
        uint32_t acc_paradas_tiempo_s_out = 0;
        double   pisadas_min = 0.0;

        {
            std::lock_guard<std::mutex> lock(mtx_);
            PH1State &st = states_[line];
            // Restauración del estado persistido, una vez por clave y arranque.
            // Va PRIMERO: decide si este mensaje continúa el acumulador o lo pone a
            // cero, y deja last_accepted_epoch_s en su sitio para que el hueco de
            // abajo sea el real y no cero.
            const int64_t restart_gap_s =
                restore_state_if_needed(st, "prensa_hidraulica1", line, shiftNum, shift_mode, dev_epoch);
            if (restart_gap_s > 0 && st.initialized)
                st.acc_unobserved_s += restart_gap_s;   // caso 4: hueco dentro del turno

            CounterCtx ctx{};
            ctx.line = line;
            ctx.proc = "prensa_hidraulica1";
            ctx.rate_max_per_s = celima::rates().rate_per_s(line, ctx.proc);
            ctx.margin         = celima::rates().margin();
            // Hueco desde el último mensaje aceptado de esta clave. Sin epoch de
            // dispositivo queda en 0 y la cota declara el delta implausible.
            if (dev_epoch && st.last_accepted_epoch_s > 0)
                ctx.elapsed_s = static_cast<double>(*dev_epoch - st.last_accepted_epoch_s);

            if (!st.initialized || st.shift != shiftNum) {
                // reseed: una vez por procesador y línea, antes de pisar el estado.
                // suppress_reseed_log lo pone la restauración cuando ya emitió la
                // traza del cambio de turno a través del reinicio.
                if (!st.suppress_reseed_log)
                    celima::log::state_event("reseed", line, "prensa_hidraulica1",
                        st.initialized
                            ? ("reason=shift_change shift_prev=" + std::to_string(st.shift) +
                               " shift_new=" + std::to_string(shiftNum))
                            : ("reason=first_message shift=" + std::to_string(shiftNum)));
                // New shift - initialize
                st = PH1State();
                // Los segundos no observados pertenecen al turno: 0 en un cambio de
                // turno normal, el hueco en un cambio a través de un reinicio (caso 2).
                st.acc_unobserved_s = restart_gap_s;
                st.initialized = true;
                st.shift = shiftNum;
                st.last_pisadas = pisadas;
                st.last_metrica_tiempo = metrica_tiempo;
                st.last_paradas_count = paradas_count;
                st.last_paradas_tiempo = paradas_tiempo;
                st.last_dd_pisadas        = pisadas;
                st.last_dd_tiempo         = metrica_tiempo;
                st.last_dd_paradas        = paradas_count;
                st.last_dd_tiempo_paradas = paradas_tiempo;
                st.last_accepted_time     = std::time(nullptr);
                if (dev_epoch) st.last_accepted_epoch_s = *dev_epoch;
            }
            else {
                // Duplicate-frame rejection: no timer1Hz available; compare all
                // four counter values + 120 s window (retry @ 30 s, interval @ 180 s).
                constexpr int DEDUP_WINDOW_SECS = 120;
                const auto now = std::time(nullptr);
                if (pisadas        == st.last_dd_pisadas        &&
                    metrica_tiempo == st.last_dd_tiempo         &&
                    paradas_count  == st.last_dd_paradas        &&
                    paradas_tiempo == st.last_dd_tiempo_paradas &&
                    (now - st.last_accepted_time) < DEDUP_WINDOW_SECS) {
                    std::cout << "[PH1] Trama repetida descartada (lineID=" << line << ")\n";
                    return {};
                }
                st.last_dd_pisadas        = pisadas;
                st.last_dd_tiempo         = metrica_tiempo;
                st.last_dd_paradas        = paradas_count;
                st.last_dd_tiempo_paradas = paradas_tiempo;
                st.last_accepted_time     = now;
                if (dev_epoch) st.last_accepted_epoch_s = *dev_epoch;

                // Accumulate deltas using PLC-compatible validation

                // D29005 - PISADAS counter (use diff_counter with bit-15 validation)
                uint16_t delta_pisadas = diff_counter_safe(pisadas, st.last_pisadas, st.rc_pisadas, ctx.with("pisadas"));
                st.acc_pisadas += delta_pisadas;

                // D29006 - Metric time (deciseconds, bit-15 masked)
                uint16_t delta_tiempo = diff_counter_safe(metrica_tiempo, st.last_metrica_tiempo, st.rc_metrica_tiempo, ctx.with("metrica_tiempo"));
                st.acc_metrica_tiempo_s += delta_tiempo * 0.1;  // CF100 P_0_1s: 0.1s per tick (validated)

                // D29003 - Stop count counter
                uint16_t delta_paradas = diff_counter_safe(paradas_count, st.last_paradas_count, st.rc_paradas_count, ctx.with("paradas_count"));
                st.acc_paradas_count += delta_paradas;

                // D29004 - Stop time counter (seconds)
                uint16_t delta_tiempo_paradas = diff_counter_safe(paradas_tiempo, st.last_paradas_tiempo, st.rc_paradas_tiempo, ctx.with("paradas_tiempo"));
                st.acc_paradas_tiempo_s += delta_tiempo_paradas;  // firmware Arduino ya corrige alineamiento de bit
            }

            // Copy out accumulated values
            acc_pisadas_out = st.acc_pisadas;
            acc_metrica_tiempo_s_out = st.acc_metrica_tiempo_s;
            acc_paradas_count_out = st.acc_paradas_count;
            acc_paradas_tiempo_s_out = st.acc_paradas_tiempo_s;

            // Calculate rate (pisadas per minute)
            if (acc_metrica_tiempo_s_out > 1.0) {
                pisadas_min = acc_pisadas_out / (acc_metrica_tiempo_s_out / 60.0);
            }

            // Guardar tras procesar el mensaje, dentro del mismo mutex que protege
            // el estado: un corte de energía no da oportunidad de cerrar limpio, así
            // que no vale dejarlo solo en SIGTERM.
            unobserved_s = st.acc_unobserved_s;
            persist_state(st, "prensa_hidraulica1", line, shiftNum);
        }

        // Calculate products from pisadas using line-specific factor
        int factor_pisadas;
        switch (line) {
            case 1: factor_pisadas = L1_PIEZAS_PISADA; break;
            case 2: factor_pisadas = L2_PIEZAS_PISADA; break;
            case 3: factor_pisadas = L3_PIEZAS_PISADA; break;
            case 4: factor_pisadas = L4_PIEZAS_PISADA; break;
            case 5: factor_pisadas = L5_PIEZAS_PISADA; break;
            default: factor_pisadas = 3; break;
        }

        // Build output JSON
        json qual;
        qual["alarms"] = alarms;
        qual["timestamp_device"] = device_timestamp(msg);

        json prod;
        prod["maquina_id"] = 1;
        prod["turno"] = shiftNum;

        // Pisadas (primary counter from D29005)
        prod["cantidadProductos_raw"] = raw_pisadas;         // Raw value for debugging
        prod["cantidadProductos_instantaneo"] = pisadas;     // Current instantaneous
        prod["cantidadPisadas_turno"] = acc_pisadas_out;     // Accumulated pisadas
        prod["cantidadPisadas_min"] = static_cast<uint32_t>(pisadas_min);
        prod["cantidadProductos_turno"] = acc_pisadas_out * factor_pisadas;  // Calculated products

        // Production time (from D29006 - deciseconds converted to seconds)
        prod["tiempoProduccion_ds_instantaneo"] = metrica_tiempo;
        prod["tiempoProduccion_turno_s"] = static_cast<uint32_t>(acc_metrica_tiempo_s_out);

        // Paradas count (from D29003)
        prod["paradas_raw"] = raw_paradas;
        prod["paradas_instantaneo"] = paradas_count;
        prod["paradas_turno"] = acc_paradas_count_out;

        // Paradas time (from D29004 - already in seconds)
        prod["tiempoParadas_raw"] = raw_tiempo_paradas;
        prod["tiempoParadas_instantaneo"] = paradas_tiempo;
        prod["tiempoParadas_turno_s"] = acc_paradas_tiempo_s_out;

        prod["timestamp_device"] = device_timestamp(msg);
        add_unobserved_marker(prod, unobserved_s);

        auto t1 = isa95_prefix + std::to_string(line) + "/prensa_hidraulica1/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/prensa_hidraulica1/production";

        return {make_pub(t1, qual), make_pub(t2, prod)};
    }
};

// Static definitions
std::mutex PrensaHidraulica1Processor::mtx_;
std::unordered_map<int, PrensaHidraulica1Processor::PH1State>
    PrensaHidraulica1Processor::states_;

// ============================================================================
// PrensaHidraulica2Processor - Fixed with correct counter handling
// ============================================================================

class PrensaHidraulica2Processor : public IMessageProcessor
{
    struct PH2State {
        bool initialized = false;
        int  shift       = -1;

        uint16_t    last_dd_pisadas        = 0;
        uint16_t    last_dd_tiempo         = 0;
        uint16_t    last_dd_paradas        = 0;
        uint16_t    last_dd_tiempo_paradas = 0;
        std::time_t last_accepted_time     = 0;
        // Época del último mensaje ACEPTADO, para medir el hueco (D2).
        // Semántica y unidad distintas a last_accepted_time (dedup): no reutilizar.
        int64_t last_accepted_epoch_s = 0;

        uint16_t last_pisadas = 0;
        uint8_t  rc_pisadas = 0;
        uint32_t acc_pisadas = 0;

        uint16_t last_metrica_tiempo = 0;
        uint8_t  rc_metrica_tiempo = 0;
        double   acc_metrica_tiempo_s = 0.0;

        uint16_t last_paradas_count = 0;
        uint8_t  rc_paradas_count = 0;
        uint32_t acc_paradas_count = 0;

        uint16_t last_paradas_tiempo = 0;
        uint8_t  rc_paradas_tiempo = 0;
        uint32_t acc_paradas_tiempo_s = 0;

        // Segundos del turno en curso que nadie observó (huecos por reinicio).
        int64_t acc_unobserved_s = 0;
        // Traza de re-siembra ya emitida por la restauración. No se serializa.
        bool suppress_reseed_log = false;

        // Serialización generada a partir de los miembros del struct: se
        // persisten TODOS (acumuladores, tracking de raw, contadores de
        // rechazo, EMA y estado de deduplicación), no solo los acc_*.
        nlohmann::json to_json() const {
            nlohmann::json j;
            j["v"] = celima::kStateSchemaVersion;
            j["acc_unobserved_s"] = acc_unobserved_s;
            j["initialized"] = initialized;
            j["shift"] = shift;
            j["last_dd_pisadas"] = last_dd_pisadas;
            j["last_dd_tiempo"] = last_dd_tiempo;
            j["last_dd_paradas"] = last_dd_paradas;
            j["last_dd_tiempo_paradas"] = last_dd_tiempo_paradas;
            j["last_accepted_time"] = last_accepted_time;
            j["last_accepted_epoch_s"] = last_accepted_epoch_s;
            j["last_pisadas"] = last_pisadas;
            j["rc_pisadas"] = rc_pisadas;
            j["acc_pisadas"] = acc_pisadas;
            j["last_metrica_tiempo"] = last_metrica_tiempo;
            j["rc_metrica_tiempo"] = rc_metrica_tiempo;
            j["acc_metrica_tiempo_s"] = acc_metrica_tiempo_s;
            j["last_paradas_count"] = last_paradas_count;
            j["rc_paradas_count"] = rc_paradas_count;
            j["acc_paradas_count"] = acc_paradas_count;
            j["last_paradas_tiempo"] = last_paradas_tiempo;
            j["rc_paradas_tiempo"] = rc_paradas_tiempo;
            j["acc_paradas_tiempo_s"] = acc_paradas_tiempo_s;
            return j;
        }

        void from_json(const nlohmann::json &j) {
            acc_unobserved_s = j.value("acc_unobserved_s", acc_unobserved_s);
            initialized = j.value("initialized", initialized);
            shift = j.value("shift", shift);
            last_dd_pisadas = j.value("last_dd_pisadas", last_dd_pisadas);
            last_dd_tiempo = j.value("last_dd_tiempo", last_dd_tiempo);
            last_dd_paradas = j.value("last_dd_paradas", last_dd_paradas);
            last_dd_tiempo_paradas = j.value("last_dd_tiempo_paradas", last_dd_tiempo_paradas);
            last_accepted_time = j.value("last_accepted_time", last_accepted_time);
            last_accepted_epoch_s = j.value("last_accepted_epoch_s", last_accepted_epoch_s);
            last_pisadas = j.value("last_pisadas", last_pisadas);
            rc_pisadas = j.value("rc_pisadas", rc_pisadas);
            acc_pisadas = j.value("acc_pisadas", acc_pisadas);
            last_metrica_tiempo = j.value("last_metrica_tiempo", last_metrica_tiempo);
            rc_metrica_tiempo = j.value("rc_metrica_tiempo", rc_metrica_tiempo);
            acc_metrica_tiempo_s = j.value("acc_metrica_tiempo_s", acc_metrica_tiempo_s);
            last_paradas_count = j.value("last_paradas_count", last_paradas_count);
            rc_paradas_count = j.value("rc_paradas_count", rc_paradas_count);
            acc_paradas_count = j.value("acc_paradas_count", acc_paradas_count);
            last_paradas_tiempo = j.value("last_paradas_tiempo", last_paradas_tiempo);
            rc_paradas_tiempo = j.value("rc_paradas_tiempo", rc_paradas_tiempo);
            acc_paradas_tiempo_s = j.value("acc_paradas_tiempo_s", acc_paradas_tiempo_s);
        }
    };

    static std::mutex mtx_;
    static std::unordered_map<int, PH2State> states_;

public:
    static void reset_states() {
        std::lock_guard<std::mutex> lock(mtx_);
        states_.clear();
    }

    std::vector<Publication> process(const json &msg,
                                     const std::string &isa95_prefix,
                                     int shift_mode = 3) override
    {
        auto sh = current_shift_localtime(shift_mode);
        int shiftNum = (sh == Shift::S1 ? 1 : (sh == Shift::S2 ? 2 : 3));

        int line          = jsonu::get_opt<int>(msg, "lineID").value_or(0);
        const auto dev_epoch = device_epoch_s(msg);
        int64_t unobserved_s = 0;   // segundos del turno sin observar
        int alarms        = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int raw_pisadas   = jsonu::get_opt<int>(msg, "cantidadProductos").value_or(0);
        int raw_tiempo    = jsonu::get_opt<int>(msg, "tiempoProduccion_ds").value_or(0);
        int raw_paradas   = jsonu::get_opt<int>(msg, "paradas").value_or(0);
        int raw_tiempo_paradas = jsonu::get_opt<int>(msg, "tiempoParadas_s").value_or(0);

        uint16_t pisadas = static_cast<uint16_t>(raw_pisadas);
        uint16_t metrica_tiempo = static_cast<uint16_t>(raw_tiempo);
        uint16_t paradas_count = static_cast<uint16_t>(raw_paradas);
        uint16_t paradas_tiempo = static_cast<uint16_t>(raw_tiempo_paradas);

        uint32_t acc_pisadas_out = 0;
        double   acc_metrica_tiempo_s_out = 0.0;
        uint32_t acc_paradas_count_out = 0;
        uint32_t acc_paradas_tiempo_s_out = 0;
        double   pisadas_min = 0.0;

        {
            std::lock_guard<std::mutex> lock(mtx_);
            PH2State &st = states_[line];
            // Restauración del estado persistido, una vez por clave y arranque.
            // Va PRIMERO: decide si este mensaje continúa el acumulador o lo pone a
            // cero, y deja last_accepted_epoch_s en su sitio para que el hueco de
            // abajo sea el real y no cero.
            const int64_t restart_gap_s =
                restore_state_if_needed(st, "prensa_hidraulica2", line, shiftNum, shift_mode, dev_epoch);
            if (restart_gap_s > 0 && st.initialized)
                st.acc_unobserved_s += restart_gap_s;   // caso 4: hueco dentro del turno

            CounterCtx ctx{};
            ctx.line = line;
            ctx.proc = "prensa_hidraulica2";
            ctx.rate_max_per_s = celima::rates().rate_per_s(line, ctx.proc);
            ctx.margin         = celima::rates().margin();
            // Hueco desde el último mensaje aceptado de esta clave. Sin epoch de
            // dispositivo queda en 0 y la cota declara el delta implausible.
            if (dev_epoch && st.last_accepted_epoch_s > 0)
                ctx.elapsed_s = static_cast<double>(*dev_epoch - st.last_accepted_epoch_s);

            if (!st.initialized || st.shift != shiftNum) {
                // reseed: una vez por procesador y línea, antes de pisar el estado.
                // suppress_reseed_log lo pone la restauración cuando ya emitió la
                // traza del cambio de turno a través del reinicio.
                if (!st.suppress_reseed_log)
                    celima::log::state_event("reseed", line, "prensa_hidraulica2",
                        st.initialized
                            ? ("reason=shift_change shift_prev=" + std::to_string(st.shift) +
                               " shift_new=" + std::to_string(shiftNum))
                            : ("reason=first_message shift=" + std::to_string(shiftNum)));
                st = PH2State();
                // Los segundos no observados pertenecen al turno: 0 en un cambio de
                // turno normal, el hueco en un cambio a través de un reinicio (caso 2).
                st.acc_unobserved_s = restart_gap_s;
                st.initialized = true;
                st.shift = shiftNum;
                st.last_pisadas = pisadas;
                st.last_metrica_tiempo = metrica_tiempo;
                st.last_paradas_count = paradas_count;
                st.last_paradas_tiempo = paradas_tiempo;
                st.last_dd_pisadas        = pisadas;
                st.last_dd_tiempo         = metrica_tiempo;
                st.last_dd_paradas        = paradas_count;
                st.last_dd_tiempo_paradas = paradas_tiempo;
                st.last_accepted_time     = std::time(nullptr);
                if (dev_epoch) st.last_accepted_epoch_s = *dev_epoch;
            }
            else {
                // Duplicate-frame rejection
                constexpr int DEDUP_WINDOW_SECS = 120;
                const auto now = std::time(nullptr);
                if (pisadas        == st.last_dd_pisadas        &&
                    metrica_tiempo == st.last_dd_tiempo         &&
                    paradas_count  == st.last_dd_paradas        &&
                    paradas_tiempo == st.last_dd_tiempo_paradas &&
                    (now - st.last_accepted_time) < DEDUP_WINDOW_SECS) {
                    std::cout << "[PH2] Trama repetida descartada (lineID=" << line << ")\n";
                    return {};
                }
                st.last_dd_pisadas        = pisadas;
                st.last_dd_tiempo         = metrica_tiempo;
                st.last_dd_paradas        = paradas_count;
                st.last_dd_tiempo_paradas = paradas_tiempo;
                st.last_accepted_time     = now;
                if (dev_epoch) st.last_accepted_epoch_s = *dev_epoch;

                // Use PLC-compatible counter validation
                uint16_t delta_pisadas = diff_counter_safe(pisadas, st.last_pisadas, st.rc_pisadas, ctx.with("pisadas"));
                st.acc_pisadas += delta_pisadas;

                uint16_t delta_tiempo = diff_counter_safe(metrica_tiempo, st.last_metrica_tiempo, st.rc_metrica_tiempo, ctx.with("metrica_tiempo"));
                st.acc_metrica_tiempo_s += delta_tiempo * 0.1;  // CF100 P_0_1s: 0.1s per tick (validated)

                uint16_t delta_paradas = diff_counter_safe(paradas_count, st.last_paradas_count, st.rc_paradas_count, ctx.with("paradas_count"));
                st.acc_paradas_count += delta_paradas;

                uint16_t delta_tiempo_paradas = diff_counter_safe(paradas_tiempo, st.last_paradas_tiempo, st.rc_paradas_tiempo, ctx.with("paradas_tiempo"));
                st.acc_paradas_tiempo_s += delta_tiempo_paradas;  // firmware Arduino ya corrige alineamiento de bit
            }

            acc_pisadas_out = st.acc_pisadas;
            acc_metrica_tiempo_s_out = st.acc_metrica_tiempo_s;
            acc_paradas_count_out = st.acc_paradas_count;
            acc_paradas_tiempo_s_out = st.acc_paradas_tiempo_s;

            if (acc_metrica_tiempo_s_out > 1.0) {
                pisadas_min = acc_pisadas_out / (acc_metrica_tiempo_s_out / 60.0);
            }

            // Guardar tras procesar el mensaje, dentro del mismo mutex que protege
            // el estado: un corte de energía no da oportunidad de cerrar limpio, así
            // que no vale dejarlo solo en SIGTERM.
            unobserved_s = st.acc_unobserved_s;
            persist_state(st, "prensa_hidraulica2", line, shiftNum);
        }

        int factor_pisadas;
        switch (line) {
            case 1: factor_pisadas = L1_PIEZAS_PISADA; break;
            case 2: factor_pisadas = L2_PIEZAS_PISADA; break;
            case 3: factor_pisadas = L3_PIEZAS_PISADA; break;
            case 4: factor_pisadas = L4_PIEZAS_PISADA; break;
            case 5: factor_pisadas = L5_PIEZAS_PISADA; break;
            default: factor_pisadas = 3; break;
        }

        json qual;
        qual["alarms"] = alarms;
        qual["timestamp_device"] = device_timestamp(msg);

        json prod;
        prod["maquina_id"] = 2;
        prod["turno"] = shiftNum;

        prod["cantidadProductos_raw"] = raw_pisadas;
        prod["cantidadProductos_instantaneo"] = pisadas;
        prod["cantidadPisadas_turno"] = acc_pisadas_out;
        prod["cantidadPisadas_min"] = static_cast<uint32_t>(pisadas_min);
        prod["cantidadProductos_turno"] = acc_pisadas_out * factor_pisadas;

        prod["tiempoProduccion_ds_instantaneo"] = metrica_tiempo;
        prod["tiempoProduccion_turno_s"] = static_cast<uint32_t>(acc_metrica_tiempo_s_out);

        prod["paradas_raw"] = raw_paradas;
        prod["paradas_instantaneo"] = paradas_count;
        prod["paradas_turno"] = acc_paradas_count_out;

        prod["tiempoParadas_raw"] = raw_tiempo_paradas;
        prod["tiempoParadas_instantaneo"] = paradas_tiempo;
        prod["tiempoParadas_turno_s"] = acc_paradas_tiempo_s_out;

        prod["timestamp_device"] = device_timestamp(msg);
        add_unobserved_marker(prod, unobserved_s);

        auto t1 = isa95_prefix + std::to_string(line) + "/prensa_hidraulica2/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/prensa_hidraulica2/production";

        return {make_pub(t1, qual), make_pub(t2, prod)};
    }
};

// Static definitions
std::mutex PrensaHidraulica2Processor::mtx_;
std::unordered_map<int, PrensaHidraulica2Processor::PH2State>
    PrensaHidraulica2Processor::states_;

/**
 * EntradaSecadorProcessor - CORRECTED VERSION
 * 
 * Changes from original:
 * 1. Reads NEW semantic field names from LoRaWAN decoder v2
 * 2. ALL registers use bit-15 flag — diff_counter for everything
 * 3. Tracks ALL fields from PLC (not just 2)
 * 4. Output JSON uses correct semantic names
 * 
 * Input fields (from decoder v2):
 *   checksum, timer1Hz, alarms, paradas_cantidad, paradas_tempo_s,
 *   ingreso_elevador_cantidad, ingreso_elevador_tiempo_ds,
 *   bancalino_l1_cantidad, bancalino_l1_tiempo_ds,
 *   bancalino_l2_cantidad, bancalino_l2_tiempo_ds
 * 
 * PLC Register mapping:
 *   D29003 = paradas_cantidad (stop events)
 *   D29004 = paradas_tempo_s (stop time in seconds)
 *   D29005 = ingreso_elevador_cantidad (elevator entry events)
 *   D29006 = ingreso_elevador_tiempo_ds (elevator time in deciseconds)
 *   D29007 = bancalino_l1_cantidad (bancalino L1 movements)
 *   D29008 = bancalino_l1_tiempo_ds (bancalino L1 time in deciseconds)
 *   D29009 = bancalino_l2_cantidad (bancalino L2 movements)
 *   D29010 = bancalino_l2_tiempo_ds (bancalino L2 time in deciseconds)
 */

class EntradaSecadorProcessor : public IMessageProcessor
{
private:
    struct State
    {
        bool initialized = false;
        int shift = 0;

        uint16_t last_accepted_timer1Hz = 0;
        // Época del último mensaje ACEPTADO, para medir el hueco (D2).
        // Semántica y unidad distintas a last_accepted_time (dedup): no reutilizar.
        int64_t last_accepted_epoch_s = 0;

        // All 16-bit counters - last values and accumulators
        uint16_t last_timer1Hz = 0;
        uint8_t  rc_timer1Hz = 0;
        uint32_t acc_timer1Hz = 0;

        uint16_t last_paradas_cantidad = 0;
        uint8_t  rc_paradas_cantidad = 0;
        uint32_t acc_paradas_cantidad = 0;

        uint16_t last_paradas_tempo = 0;
        uint8_t  rc_paradas_tempo = 0;
        uint32_t acc_paradas_tempo_s = 0;

        uint16_t last_ingreso_elevador_cantidad = 0;
        uint8_t  rc_ingreso_elevador_cantidad = 0;
        uint32_t acc_ingreso_elevador_cantidad = 0;

        uint16_t last_ingreso_elevador_tiempo = 0;
        uint8_t  rc_ingreso_elevador_tiempo = 0;
        uint32_t acc_ingreso_elevador_tiempo_ds = 0;

        uint16_t last_bancalino_l1_cantidad = 0;
        uint8_t  rc_bancalino_l1_cantidad = 0;
        uint32_t acc_bancalino_l1_cantidad = 0;

        uint16_t last_bancalino_l1_tiempo = 0;
        uint8_t  rc_bancalino_l1_tiempo = 0;
        uint32_t acc_bancalino_l1_tiempo_ds = 0;

        uint16_t last_bancalino_l2_cantidad = 0;
        uint8_t  rc_bancalino_l2_cantidad = 0;
        uint32_t acc_bancalino_l2_cantidad = 0;

        uint16_t last_bancalino_l2_tiempo = 0;
        uint8_t  rc_bancalino_l2_tiempo = 0;
        uint32_t acc_bancalino_l2_tiempo_ds = 0;

        // Segundos del turno en curso que nadie observó (huecos por reinicio).
        int64_t acc_unobserved_s = 0;
        // Traza de re-siembra ya emitida por la restauración. No se serializa.
        bool suppress_reseed_log = false;

        // Serialización generada a partir de los miembros del struct: se
        // persisten TODOS (acumuladores, tracking de raw, contadores de
        // rechazo, EMA y estado de deduplicación), no solo los acc_*.
        nlohmann::json to_json() const {
            nlohmann::json j;
            j["v"] = celima::kStateSchemaVersion;
            j["acc_unobserved_s"] = acc_unobserved_s;
            j["initialized"] = initialized;
            j["shift"] = shift;
            j["last_accepted_timer1Hz"] = last_accepted_timer1Hz;
            j["last_accepted_epoch_s"] = last_accepted_epoch_s;
            j["last_timer1Hz"] = last_timer1Hz;
            j["rc_timer1Hz"] = rc_timer1Hz;
            j["acc_timer1Hz"] = acc_timer1Hz;
            j["last_paradas_cantidad"] = last_paradas_cantidad;
            j["rc_paradas_cantidad"] = rc_paradas_cantidad;
            j["acc_paradas_cantidad"] = acc_paradas_cantidad;
            j["last_paradas_tempo"] = last_paradas_tempo;
            j["rc_paradas_tempo"] = rc_paradas_tempo;
            j["acc_paradas_tempo_s"] = acc_paradas_tempo_s;
            j["last_ingreso_elevador_cantidad"] = last_ingreso_elevador_cantidad;
            j["rc_ingreso_elevador_cantidad"] = rc_ingreso_elevador_cantidad;
            j["acc_ingreso_elevador_cantidad"] = acc_ingreso_elevador_cantidad;
            j["last_ingreso_elevador_tiempo"] = last_ingreso_elevador_tiempo;
            j["rc_ingreso_elevador_tiempo"] = rc_ingreso_elevador_tiempo;
            j["acc_ingreso_elevador_tiempo_ds"] = acc_ingreso_elevador_tiempo_ds;
            j["last_bancalino_l1_cantidad"] = last_bancalino_l1_cantidad;
            j["rc_bancalino_l1_cantidad"] = rc_bancalino_l1_cantidad;
            j["acc_bancalino_l1_cantidad"] = acc_bancalino_l1_cantidad;
            j["last_bancalino_l1_tiempo"] = last_bancalino_l1_tiempo;
            j["rc_bancalino_l1_tiempo"] = rc_bancalino_l1_tiempo;
            j["acc_bancalino_l1_tiempo_ds"] = acc_bancalino_l1_tiempo_ds;
            j["last_bancalino_l2_cantidad"] = last_bancalino_l2_cantidad;
            j["rc_bancalino_l2_cantidad"] = rc_bancalino_l2_cantidad;
            j["acc_bancalino_l2_cantidad"] = acc_bancalino_l2_cantidad;
            j["last_bancalino_l2_tiempo"] = last_bancalino_l2_tiempo;
            j["rc_bancalino_l2_tiempo"] = rc_bancalino_l2_tiempo;
            j["acc_bancalino_l2_tiempo_ds"] = acc_bancalino_l2_tiempo_ds;
            return j;
        }

        void from_json(const nlohmann::json &j) {
            acc_unobserved_s = j.value("acc_unobserved_s", acc_unobserved_s);
            initialized = j.value("initialized", initialized);
            shift = j.value("shift", shift);
            last_accepted_timer1Hz = j.value("last_accepted_timer1Hz", last_accepted_timer1Hz);
            last_accepted_epoch_s = j.value("last_accepted_epoch_s", last_accepted_epoch_s);
            last_timer1Hz = j.value("last_timer1Hz", last_timer1Hz);
            rc_timer1Hz = j.value("rc_timer1Hz", rc_timer1Hz);
            acc_timer1Hz = j.value("acc_timer1Hz", acc_timer1Hz);
            last_paradas_cantidad = j.value("last_paradas_cantidad", last_paradas_cantidad);
            rc_paradas_cantidad = j.value("rc_paradas_cantidad", rc_paradas_cantidad);
            acc_paradas_cantidad = j.value("acc_paradas_cantidad", acc_paradas_cantidad);
            last_paradas_tempo = j.value("last_paradas_tempo", last_paradas_tempo);
            rc_paradas_tempo = j.value("rc_paradas_tempo", rc_paradas_tempo);
            acc_paradas_tempo_s = j.value("acc_paradas_tempo_s", acc_paradas_tempo_s);
            last_ingreso_elevador_cantidad = j.value("last_ingreso_elevador_cantidad", last_ingreso_elevador_cantidad);
            rc_ingreso_elevador_cantidad = j.value("rc_ingreso_elevador_cantidad", rc_ingreso_elevador_cantidad);
            acc_ingreso_elevador_cantidad = j.value("acc_ingreso_elevador_cantidad", acc_ingreso_elevador_cantidad);
            last_ingreso_elevador_tiempo = j.value("last_ingreso_elevador_tiempo", last_ingreso_elevador_tiempo);
            rc_ingreso_elevador_tiempo = j.value("rc_ingreso_elevador_tiempo", rc_ingreso_elevador_tiempo);
            acc_ingreso_elevador_tiempo_ds = j.value("acc_ingreso_elevador_tiempo_ds", acc_ingreso_elevador_tiempo_ds);
            last_bancalino_l1_cantidad = j.value("last_bancalino_l1_cantidad", last_bancalino_l1_cantidad);
            rc_bancalino_l1_cantidad = j.value("rc_bancalino_l1_cantidad", rc_bancalino_l1_cantidad);
            acc_bancalino_l1_cantidad = j.value("acc_bancalino_l1_cantidad", acc_bancalino_l1_cantidad);
            last_bancalino_l1_tiempo = j.value("last_bancalino_l1_tiempo", last_bancalino_l1_tiempo);
            rc_bancalino_l1_tiempo = j.value("rc_bancalino_l1_tiempo", rc_bancalino_l1_tiempo);
            acc_bancalino_l1_tiempo_ds = j.value("acc_bancalino_l1_tiempo_ds", acc_bancalino_l1_tiempo_ds);
            last_bancalino_l2_cantidad = j.value("last_bancalino_l2_cantidad", last_bancalino_l2_cantidad);
            rc_bancalino_l2_cantidad = j.value("rc_bancalino_l2_cantidad", rc_bancalino_l2_cantidad);
            acc_bancalino_l2_cantidad = j.value("acc_bancalino_l2_cantidad", acc_bancalino_l2_cantidad);
            last_bancalino_l2_tiempo = j.value("last_bancalino_l2_tiempo", last_bancalino_l2_tiempo);
            rc_bancalino_l2_tiempo = j.value("rc_bancalino_l2_tiempo", rc_bancalino_l2_tiempo);
            acc_bancalino_l2_tiempo_ds = j.value("acc_bancalino_l2_tiempo_ds", acc_bancalino_l2_tiempo_ds);
        }
    };

    static std::mutex mtx_;
    static std::unordered_map<int, State> states_;

public:
    static void reset_states() {
        std::lock_guard<std::mutex> lock(mtx_);
        states_.clear();
    }

    std::vector<Publication> process(const json &msg,
                                     const std::string &isa95_prefix,
                                     int shift_mode = 3) override
    {
        auto sh = current_shift_localtime(shift_mode);
        int shiftNum = (sh == Shift::S1 ? 1 : (sh == Shift::S2 ? 2 : 3));

        // === Read header fields ===
        int line = jsonu::get_opt<int>(msg, "lineID").value_or(0);
        const auto dev_epoch = device_epoch_s(msg);
        int64_t unobserved_s = 0;   // segundos del turno sin observar
        int alarms = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int checksum = jsonu::get_opt<int>(msg, "checksum").value_or(0);
        int deviceType = jsonu::get_opt<int>(msg, "deviceType").value_or(0);

        // === Read all 16-bit counters (new semantic names from decoder v2) ===
        uint16_t timer1Hz = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "timer1Hz").value_or(0));

        uint16_t paradas_cantidad = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "paradas_cantidad").value_or(0));

        uint16_t paradas_tempo = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "paradas_tempo_s").value_or(0));

        uint16_t ingreso_elevador_cantidad = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "ingreso_elevador_cantidad").value_or(0));

        uint16_t ingreso_elevador_tiempo = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "ingreso_elevador_tiempo_ds").value_or(0));

        uint16_t bancalino_l1_cantidad = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "bancalino_l1_cantidad").value_or(0));

        uint16_t bancalino_l1_tiempo = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "bancalino_l1_tiempo_ds").value_or(0));

        uint16_t bancalino_l2_cantidad = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "bancalino_l2_cantidad").value_or(0));

        uint16_t bancalino_l2_tiempo = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "bancalino_l2_tiempo_ds").value_or(0));

        // === Output accumulators ===
        uint32_t acc_timer1Hz_out = 0;
        uint32_t acc_paradas_cantidad_out = 0;
        uint32_t acc_paradas_tempo_s_out = 0;
        uint32_t acc_ingreso_elevador_cantidad_out = 0;
        uint32_t acc_ingreso_elevador_tiempo_ds_out = 0;
        uint32_t acc_bancalino_l1_cantidad_out = 0;
        uint32_t acc_bancalino_l1_tiempo_ds_out = 0;
        uint32_t acc_bancalino_l2_cantidad_out = 0;
        uint32_t acc_bancalino_l2_tiempo_ds_out = 0;

        {
            std::lock_guard<std::mutex> lock(mtx_);
            State &st = states_[line];
            // Restauración del estado persistido, una vez por clave y arranque.
            // Va PRIMERO: decide si este mensaje continúa el acumulador o lo pone a
            // cero, y deja last_accepted_epoch_s en su sitio para que el hueco de
            // abajo sea el real y no cero.
            const int64_t restart_gap_s =
                restore_state_if_needed(st, "entrada_secador", line, shiftNum, shift_mode, dev_epoch);
            if (restart_gap_s > 0 && st.initialized)
                st.acc_unobserved_s += restart_gap_s;   // caso 4: hueco dentro del turno

            CounterCtx ctx{};
            ctx.line = line;
            ctx.proc = "entrada_secador";
            ctx.rate_max_per_s = celima::rates().rate_per_s(line, ctx.proc);
            ctx.margin         = celima::rates().margin();
            // Hueco desde el último mensaje aceptado de esta clave. Sin epoch de
            // dispositivo queda en 0 y la cota declara el delta implausible.
            if (dev_epoch && st.last_accepted_epoch_s > 0)
                ctx.elapsed_s = static_cast<double>(*dev_epoch - st.last_accepted_epoch_s);

            if (!st.initialized || st.shift != shiftNum) {
                // reseed: una vez por procesador y línea, antes de pisar el estado.
                // suppress_reseed_log lo pone la restauración cuando ya emitió la
                // traza del cambio de turno a través del reinicio.
                if (!st.suppress_reseed_log)
                    celima::log::state_event("reseed", line, "entrada_secador",
                        st.initialized
                            ? ("reason=shift_change shift_prev=" + std::to_string(st.shift) +
                               " shift_new=" + std::to_string(shiftNum))
                            : ("reason=first_message shift=" + std::to_string(shiftNum)));
                // New shift - reset all accumulators and store initial values
                st = State();
                // Los segundos no observados pertenecen al turno: 0 en un cambio de
                // turno normal, el hueco en un cambio a través de un reinicio (caso 2).
                st.acc_unobserved_s = restart_gap_s;
                st.initialized = true;
                st.shift = shiftNum;

                st.last_accepted_timer1Hz = timer1Hz;
                if (dev_epoch) st.last_accepted_epoch_s = *dev_epoch;
                st.last_timer1Hz = timer1Hz;
                st.last_paradas_cantidad = paradas_cantidad;
                st.last_paradas_tempo = paradas_tempo;
                st.last_ingreso_elevador_cantidad = ingreso_elevador_cantidad;
                st.last_ingreso_elevador_tiempo = ingreso_elevador_tiempo;
                st.last_bancalino_l1_cantidad = bancalino_l1_cantidad;
                st.last_bancalino_l1_tiempo = bancalino_l1_tiempo;
                st.last_bancalino_l2_cantidad = bancalino_l2_cantidad;
                st.last_bancalino_l2_tiempo = bancalino_l2_tiempo;
            }
            else {
                // Duplicate-frame rejection: timer1Hz is a free-running 1 Hz counter;
                // identical value guarantees same LoRa frame was retransmitted.
                if (timer1Hz == st.last_accepted_timer1Hz) {
                    std::cout << "[EntradaSecador] Trama repetida descartada (lineID=" << line
                              << " timer1Hz=" << timer1Hz << ")\n";
                    return {};
                }
                st.last_accepted_timer1Hz = timer1Hz;
                if (dev_epoch) st.last_accepted_epoch_s = *dev_epoch;

                // Accumulate deltas — ALL PLC registers have bit-15 flag, use diff_counter for everything
                st.acc_timer1Hz += diff_counter_safe(timer1Hz, st.last_timer1Hz, st.rc_timer1Hz, ctx.with("timer1Hz"));

                st.acc_paradas_cantidad += diff_counter_safe(paradas_cantidad, st.last_paradas_cantidad, st.rc_paradas_cantidad, ctx.with("paradas_cantidad"));

                st.acc_paradas_tempo_s += diff_counter_safe(paradas_tempo, st.last_paradas_tempo, st.rc_paradas_tempo, ctx.with("paradas_tempo"));

                st.acc_ingreso_elevador_cantidad += diff_counter_safe(ingreso_elevador_cantidad, st.last_ingreso_elevador_cantidad, st.rc_ingreso_elevador_cantidad, ctx.with("ingreso_elevador_cantidad"));

                st.acc_ingreso_elevador_tiempo_ds += diff_counter_safe(ingreso_elevador_tiempo, st.last_ingreso_elevador_tiempo, st.rc_ingreso_elevador_tiempo, ctx.with("ingreso_elevador_tiempo"));

                {
                    uint16_t d = diff_counter_safe(bancalino_l1_cantidad, st.last_bancalino_l1_cantidad, st.rc_bancalino_l1_cantidad, ctx.with("bancalino_l1_cantidad"));
                    st.acc_bancalino_l1_cantidad += (line == 2) ? d / 4 : d;
                }

                st.acc_bancalino_l1_tiempo_ds += diff_counter_safe(bancalino_l1_tiempo, st.last_bancalino_l1_tiempo, st.rc_bancalino_l1_tiempo, ctx.with("bancalino_l1_tiempo"));

                {
                    uint16_t d = diff_counter_safe(bancalino_l2_cantidad, st.last_bancalino_l2_cantidad, st.rc_bancalino_l2_cantidad, ctx.with("bancalino_l2_cantidad"));
                    st.acc_bancalino_l2_cantidad += (line == 2) ? d / 4 : d;
                }

                st.acc_bancalino_l2_tiempo_ds += diff_counter_safe(bancalino_l2_tiempo, st.last_bancalino_l2_tiempo, st.rc_bancalino_l2_tiempo, ctx.with("bancalino_l2_tiempo"));
            }

            // Copy accumulated values to output
            acc_timer1Hz_out = st.acc_timer1Hz;
            acc_paradas_cantidad_out = st.acc_paradas_cantidad;
            acc_paradas_tempo_s_out = st.acc_paradas_tempo_s;
            acc_ingreso_elevador_cantidad_out = st.acc_ingreso_elevador_cantidad;
            acc_ingreso_elevador_tiempo_ds_out = st.acc_ingreso_elevador_tiempo_ds;
            acc_bancalino_l1_cantidad_out = st.acc_bancalino_l1_cantidad;
            acc_bancalino_l1_tiempo_ds_out = st.acc_bancalino_l1_tiempo_ds;
            acc_bancalino_l2_cantidad_out = st.acc_bancalino_l2_cantidad;
            acc_bancalino_l2_tiempo_ds_out = st.acc_bancalino_l2_tiempo_ds;

            // Guardar tras procesar el mensaje, dentro del mismo mutex que protege
            // el estado: un corte de energía no da oportunidad de cerrar limpio, así
            // que no vale dejarlo solo en SIGTERM.
            unobserved_s = st.acc_unobserved_s;
            persist_state(st, "entrada_secador", line, shiftNum);
        }

        // === Build output JSON with CORRECT semantic field names ===
        json prod;
        prod["maquina_id"] = 3;
        prod["turno"] = shiftNum;
        prod["deviceType"] = deviceType;
        prod["lineID"] = line;
        prod["checksum"] = checksum;

        // Timer/validation
        prod["timer1Hz_instantaneo"] = timer1Hz;
        prod["timer1Hz_turno"] = acc_timer1Hz_out;  // firmware Arduino ya corrige alineamiento de bit

        // Paradas (stops) - D29003, D29004
        prod["paradas_instantaneo"] = paradas_cantidad;
        prod["paradas_turno"] = acc_paradas_cantidad_out;
        prod["paradas_tiempo_instantaneo_s"] = paradas_tempo;
        prod["paradas_tiempo_turno_s"] = acc_paradas_tempo_s_out;  // firmware Arduino ya corrige alineamiento de bit

        // Ingreso Elevador - D29005, D29006
        prod["ingreso_elevador_instantaneo"] = ingreso_elevador_cantidad;
        prod["ingreso_elevador_turno"] = acc_ingreso_elevador_cantidad_out;
        prod["ingreso_elevador_tiempo_instantaneo_ds"] = ingreso_elevador_tiempo;
        prod["ingreso_elevador_tiempo_turno_ds"] = acc_ingreso_elevador_tiempo_ds_out;  // CF100: 1 tick = 0.1s = 1ds (validated, no ×2)

        // Bancalino Linea 1 - D29007, D29008
        prod["bancalino_l1_instantaneo"] = bancalino_l1_cantidad;
        prod["bancalino_l1_turno"] = acc_bancalino_l1_cantidad_out;  // firmware Arduino ya corrige alineamiento de bit
        prod["bancalino_l1_tiempo_instantaneo_ds"] = bancalino_l1_tiempo;
        prod["bancalino_l1_tiempo_turno_ds"] = acc_bancalino_l1_tiempo_ds_out;  // CF100: 1 tick = 0.1s = 1ds (validated, no ×2)

        // Bancalino Linea 2 - D29009, D29010
        prod["bancalino_l2_instantaneo"] = bancalino_l2_cantidad;
        prod["bancalino_l2_turno"] = acc_bancalino_l2_cantidad_out;  // firmware Arduino ya corrige alineamiento de bit
        prod["bancalino_l2_tiempo_instantaneo_ds"] = bancalino_l2_tiempo;
        prod["bancalino_l2_tiempo_turno_ds"] = acc_bancalino_l2_tiempo_ds_out;  // CF100: 1 tick = 0.1s = 1ds (validated, no ×2)

        prod["timestamp_device"] = device_timestamp(msg);
        add_unobserved_marker(prod, unobserved_s);

        // Alarms
        json qual;
        qual["alarms"] = alarms;
        qual["timestamp_device"] = device_timestamp(msg);

        auto t1 = isa95_prefix + std::to_string(line) + "/entrada_secador/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/entrada_secador/production";

        return { make_pub(t1, qual), make_pub(t2, prod) };
    }
};

// Static definitions
std::mutex EntradaSecadorProcessor::mtx_;
std::unordered_map<int, EntradaSecadorProcessor::State> EntradaSecadorProcessor::states_;


/**
 * SalidaSecadorProcessor - CORRECTED VERSION
 * 
 * Changes from original:
 * 1. Reads NEW semantic field names from LoRaWAN decoder v2
 * 2. ALL registers use bit-15 flag — diff_counter for everything
 * 3. Uses diff_counter() for ALL fields (bit-15 mask + 15-bit rollover)
 * 4. Output JSON uses correct semantic names
 * 
 * Input fields (from decoder v2):
 *   checksum, timer1Hz, alarms, parada_mds_cantidad, parada_mds_tiempo_s,
 *   metrica_mds_cantidad, metrica_mds_tiempo_ds
 * 
 * PLC Register mapping (Sección6):
 *   D29003 = parada_mds_cantidad (stop events)
 *   D29004 = parada_mds_tiempo_s (stop time in seconds)
 *   D29005 = metrica_mds_cantidad (MDS cycles - NOT products!)
 *   D29006 = metrica_mds_tiempo_ds (MDS cycle time in deciseconds)
 * 
 * MDS = Mesa de Descarga Secador (Dryer Discharge Table)
 */

class SalidaSecadorProcessor : public IMessageProcessor
{
private:
    struct State
    {
        bool initialized = false;
        int shift = 0;

        uint16_t last_accepted_timer1Hz = 0;
        // Época del último mensaje ACEPTADO, para medir el hueco (D2).
        // Semántica y unidad distintas a last_accepted_time (dedup): no reutilizar.
        int64_t last_accepted_epoch_s = 0;

        // All 16-bit counters - last values and accumulators
        uint16_t last_timer1Hz = 0;
        uint8_t  rc_timer1Hz = 0;
        uint32_t acc_timer1Hz = 0;

        uint16_t last_parada_mds_cantidad = 0;
        uint8_t  rc_parada_mds_cantidad = 0;
        uint32_t acc_parada_mds_cantidad = 0;

        uint16_t last_parada_mds_tiempo = 0;
        uint8_t  rc_parada_mds_tiempo = 0;
        uint32_t acc_parada_mds_tiempo_s = 0;

        uint16_t last_metrica_mds_cantidad = 0;
        uint8_t  rc_metrica_mds_cantidad = 0;
        uint32_t acc_metrica_mds_cantidad = 0;

        uint16_t last_metrica_mds_tiempo = 0;
        uint8_t  rc_metrica_mds_tiempo = 0;
        uint32_t acc_metrica_mds_tiempo_ds = 0;

        // Segundos del turno en curso que nadie observó (huecos por reinicio).
        int64_t acc_unobserved_s = 0;
        // Traza de re-siembra ya emitida por la restauración. No se serializa.
        bool suppress_reseed_log = false;

        // Serialización generada a partir de los miembros del struct: se
        // persisten TODOS (acumuladores, tracking de raw, contadores de
        // rechazo, EMA y estado de deduplicación), no solo los acc_*.
        nlohmann::json to_json() const {
            nlohmann::json j;
            j["v"] = celima::kStateSchemaVersion;
            j["acc_unobserved_s"] = acc_unobserved_s;
            j["initialized"] = initialized;
            j["shift"] = shift;
            j["last_accepted_timer1Hz"] = last_accepted_timer1Hz;
            j["last_accepted_epoch_s"] = last_accepted_epoch_s;
            j["last_timer1Hz"] = last_timer1Hz;
            j["rc_timer1Hz"] = rc_timer1Hz;
            j["acc_timer1Hz"] = acc_timer1Hz;
            j["last_parada_mds_cantidad"] = last_parada_mds_cantidad;
            j["rc_parada_mds_cantidad"] = rc_parada_mds_cantidad;
            j["acc_parada_mds_cantidad"] = acc_parada_mds_cantidad;
            j["last_parada_mds_tiempo"] = last_parada_mds_tiempo;
            j["rc_parada_mds_tiempo"] = rc_parada_mds_tiempo;
            j["acc_parada_mds_tiempo_s"] = acc_parada_mds_tiempo_s;
            j["last_metrica_mds_cantidad"] = last_metrica_mds_cantidad;
            j["rc_metrica_mds_cantidad"] = rc_metrica_mds_cantidad;
            j["acc_metrica_mds_cantidad"] = acc_metrica_mds_cantidad;
            j["last_metrica_mds_tiempo"] = last_metrica_mds_tiempo;
            j["rc_metrica_mds_tiempo"] = rc_metrica_mds_tiempo;
            j["acc_metrica_mds_tiempo_ds"] = acc_metrica_mds_tiempo_ds;
            return j;
        }

        void from_json(const nlohmann::json &j) {
            acc_unobserved_s = j.value("acc_unobserved_s", acc_unobserved_s);
            initialized = j.value("initialized", initialized);
            shift = j.value("shift", shift);
            last_accepted_timer1Hz = j.value("last_accepted_timer1Hz", last_accepted_timer1Hz);
            last_accepted_epoch_s = j.value("last_accepted_epoch_s", last_accepted_epoch_s);
            last_timer1Hz = j.value("last_timer1Hz", last_timer1Hz);
            rc_timer1Hz = j.value("rc_timer1Hz", rc_timer1Hz);
            acc_timer1Hz = j.value("acc_timer1Hz", acc_timer1Hz);
            last_parada_mds_cantidad = j.value("last_parada_mds_cantidad", last_parada_mds_cantidad);
            rc_parada_mds_cantidad = j.value("rc_parada_mds_cantidad", rc_parada_mds_cantidad);
            acc_parada_mds_cantidad = j.value("acc_parada_mds_cantidad", acc_parada_mds_cantidad);
            last_parada_mds_tiempo = j.value("last_parada_mds_tiempo", last_parada_mds_tiempo);
            rc_parada_mds_tiempo = j.value("rc_parada_mds_tiempo", rc_parada_mds_tiempo);
            acc_parada_mds_tiempo_s = j.value("acc_parada_mds_tiempo_s", acc_parada_mds_tiempo_s);
            last_metrica_mds_cantidad = j.value("last_metrica_mds_cantidad", last_metrica_mds_cantidad);
            rc_metrica_mds_cantidad = j.value("rc_metrica_mds_cantidad", rc_metrica_mds_cantidad);
            acc_metrica_mds_cantidad = j.value("acc_metrica_mds_cantidad", acc_metrica_mds_cantidad);
            last_metrica_mds_tiempo = j.value("last_metrica_mds_tiempo", last_metrica_mds_tiempo);
            rc_metrica_mds_tiempo = j.value("rc_metrica_mds_tiempo", rc_metrica_mds_tiempo);
            acc_metrica_mds_tiempo_ds = j.value("acc_metrica_mds_tiempo_ds", acc_metrica_mds_tiempo_ds);
        }
    };

    static std::mutex mtx_;
    static std::unordered_map<int, State> states_;

public:
    static void reset_states() {
        std::lock_guard<std::mutex> lock(mtx_);
        states_.clear();
    }

    std::vector<Publication> process(const json &msg,
                                     const std::string &isa95_prefix,
                                     int shift_mode = 3) override
    {
        auto sh = current_shift_localtime(shift_mode);
        int shiftNum = (sh == Shift::S1 ? 1 : (sh == Shift::S2 ? 2 : 3));

        // === Read header fields ===
        int line = jsonu::get_opt<int>(msg, "lineID").value_or(0);
        const auto dev_epoch = device_epoch_s(msg);
        int64_t unobserved_s = 0;   // segundos del turno sin observar
        int alarms = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int checksum = jsonu::get_opt<int>(msg, "checksum").value_or(0);
        int deviceType = jsonu::get_opt<int>(msg, "deviceType").value_or(0);

        // === Read all 16-bit counters (new semantic names from decoder v2) ===
        uint16_t timer1Hz = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "timer1Hz").value_or(0));

        uint16_t parada_mds_cantidad = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "parada_mds_cantidad").value_or(0));

        uint16_t parada_mds_tiempo = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "parada_mds_tiempo_s").value_or(0));

        uint16_t metrica_mds_cantidad = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "metrica_mds_cantidad").value_or(0));

        uint16_t metrica_mds_tiempo = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "metrica_mds_tiempo_ds").value_or(0));

        // === Output accumulators ===
        uint32_t acc_timer1Hz_out = 0;
        uint32_t acc_parada_mds_cantidad_out = 0;
        uint32_t acc_parada_mds_tiempo_s_out = 0;
        uint32_t acc_metrica_mds_cantidad_out = 0;
        uint32_t acc_metrica_mds_tiempo_ds_out = 0;

        {
            std::lock_guard<std::mutex> lock(mtx_);
            State &st = states_[line];
            // Restauración del estado persistido, una vez por clave y arranque.
            // Va PRIMERO: decide si este mensaje continúa el acumulador o lo pone a
            // cero, y deja last_accepted_epoch_s en su sitio para que el hueco de
            // abajo sea el real y no cero.
            const int64_t restart_gap_s =
                restore_state_if_needed(st, "salida_secador", line, shiftNum, shift_mode, dev_epoch);
            if (restart_gap_s > 0 && st.initialized)
                st.acc_unobserved_s += restart_gap_s;   // caso 4: hueco dentro del turno

            CounterCtx ctx{};
            ctx.line = line;
            ctx.proc = "salida_secador";
            ctx.rate_max_per_s = celima::rates().rate_per_s(line, ctx.proc);
            ctx.margin         = celima::rates().margin();
            // Hueco desde el último mensaje aceptado de esta clave. Sin epoch de
            // dispositivo queda en 0 y la cota declara el delta implausible.
            if (dev_epoch && st.last_accepted_epoch_s > 0)
                ctx.elapsed_s = static_cast<double>(*dev_epoch - st.last_accepted_epoch_s);

            if (!st.initialized || st.shift != shiftNum) {
                // reseed: una vez por procesador y línea, antes de pisar el estado.
                // suppress_reseed_log lo pone la restauración cuando ya emitió la
                // traza del cambio de turno a través del reinicio.
                if (!st.suppress_reseed_log)
                    celima::log::state_event("reseed", line, "salida_secador",
                        st.initialized
                            ? ("reason=shift_change shift_prev=" + std::to_string(st.shift) +
                               " shift_new=" + std::to_string(shiftNum))
                            : ("reason=first_message shift=" + std::to_string(shiftNum)));
                // New shift - reset all accumulators and store initial values
                st = State();
                // Los segundos no observados pertenecen al turno: 0 en un cambio de
                // turno normal, el hueco en un cambio a través de un reinicio (caso 2).
                st.acc_unobserved_s = restart_gap_s;
                st.initialized = true;
                st.shift = shiftNum;

                st.last_accepted_timer1Hz = timer1Hz;
                if (dev_epoch) st.last_accepted_epoch_s = *dev_epoch;
                st.last_timer1Hz = timer1Hz;
                st.last_parada_mds_cantidad = parada_mds_cantidad;
                st.last_parada_mds_tiempo = parada_mds_tiempo;
                st.last_metrica_mds_cantidad = metrica_mds_cantidad;
                st.last_metrica_mds_tiempo = metrica_mds_tiempo;
            }
            else {
                // Duplicate-frame rejection
                if (timer1Hz == st.last_accepted_timer1Hz) {
                    std::cout << "[SalidaSecador] Trama repetida descartada (lineID=" << line
                              << " timer1Hz=" << timer1Hz << ")\n";
                    return {};
                }
                st.last_accepted_timer1Hz = timer1Hz;
                if (dev_epoch) st.last_accepted_epoch_s = *dev_epoch;

                // Accumulate deltas — ALL PLC registers have bit-15 flag, use diff_counter for everything
                st.acc_timer1Hz += diff_counter_safe(timer1Hz, st.last_timer1Hz, st.rc_timer1Hz, ctx.with("timer1Hz"));

                st.acc_parada_mds_cantidad += diff_counter_safe(parada_mds_cantidad, st.last_parada_mds_cantidad, st.rc_parada_mds_cantidad, ctx.with("parada_mds_cantidad"));

                st.acc_parada_mds_tiempo_s += diff_counter_safe(parada_mds_tiempo, st.last_parada_mds_tiempo, st.rc_parada_mds_tiempo, ctx.with("parada_mds_tiempo"));

                st.acc_metrica_mds_cantidad += diff_counter_safe(metrica_mds_cantidad, st.last_metrica_mds_cantidad, st.rc_metrica_mds_cantidad, ctx.with("metrica_mds_cantidad"));

                st.acc_metrica_mds_tiempo_ds += diff_counter_safe(metrica_mds_tiempo, st.last_metrica_mds_tiempo, st.rc_metrica_mds_tiempo, ctx.with("metrica_mds_tiempo"));
            }

            // Copy accumulated values to output
            acc_timer1Hz_out = st.acc_timer1Hz;
            acc_parada_mds_cantidad_out = st.acc_parada_mds_cantidad;
            acc_parada_mds_tiempo_s_out = st.acc_parada_mds_tiempo_s;
            acc_metrica_mds_cantidad_out = st.acc_metrica_mds_cantidad;
            acc_metrica_mds_tiempo_ds_out = st.acc_metrica_mds_tiempo_ds;

            // Guardar tras procesar el mensaje, dentro del mismo mutex que protege
            // el estado: un corte de energía no da oportunidad de cerrar limpio, así
            // que no vale dejarlo solo en SIGTERM.
            unobserved_s = st.acc_unobserved_s;
            persist_state(st, "salida_secador", line, shiftNum);
        }

        // === Build output JSON with CORRECT semantic field names ===
        json prod;
        prod["maquina_id"] = 4;
        prod["turno"] = shiftNum;
        prod["deviceType"] = deviceType;
        prod["lineID"] = line;
        prod["checksum"] = checksum;

        // Timer/validation
        prod["timer1Hz_instantaneo"] = timer1Hz;
        prod["timer1Hz_turno"] = acc_timer1Hz_out;  // firmware Arduino ya corrige alineamiento de bit

        // Parada MDS (stops) - D29003, D29004
        prod["parada_mds_instantaneo"] = parada_mds_cantidad;
        prod["parada_mds_turno"] = acc_parada_mds_cantidad_out;
        prod["parada_mds_tiempo_instantaneo_s"] = parada_mds_tiempo;
        prod["parada_mds_tiempo_turno_s"] = acc_parada_mds_tiempo_s_out;  // firmware Arduino ya corrige alineamiento de bit

        // Métrica MDS (cycles) - D29005, D29006
        // NOTE: These are MDS machine CYCLES, NOT product count!
        prod["metrica_mds_instantaneo"] = metrica_mds_cantidad;
        prod["metrica_mds_turno"] = acc_metrica_mds_cantidad_out;  // firmware Arduino ya corrige alineamiento de bit
        prod["metrica_mds_tiempo_instantaneo_ds"] = metrica_mds_tiempo;
        prod["metrica_mds_tiempo_turno_ds"] = acc_metrica_mds_tiempo_ds_out;  // CF100: 1 tick = 0.1s = 1ds (validated, no ×2)
        
        // Convert deciseconds to seconds for convenience
        prod["metrica_mds_tiempo_turno_s"] = static_cast<double>(acc_metrica_mds_tiempo_ds_out) * 0.1;  // CF100: each tick = 0.1s (validated)

        prod["timestamp_device"] = device_timestamp(msg);
        add_unobserved_marker(prod, unobserved_s);

        // Alarms
        json qual;
        qual["alarms"] = alarms;
        qual["timestamp_device"] = device_timestamp(msg);

        auto t1 = isa95_prefix + std::to_string(line) + "/salida_secador/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/salida_secador/production";

        return { make_pub(t1, qual), make_pub(t2, prod) };
    }
};

// Static definitions
std::mutex SalidaSecadorProcessor::mtx_;
std::unordered_map<int, SalidaSecadorProcessor::State> SalidaSecadorProcessor::states_;

/**
 * EsmalteProcessor - CORRECTED VERSION
 * 
 * Changes from original:
 * 1. Reads NEW semantic field names from LoRaWAN decoder v2
 * 2. ALL registers use bit-15 flag — diff_counter for everything
 * 3. Uses diff_counter() for ALL fields (bit-15 mask + 15-bit rollover)
 * 4. Output JSON uses correct semantic names
 * 
 * Input fields (from decoder v2):
 *   checksum, timer1Hz, alarms, parada_esm_cantidad, parada_esm_tiempo_s,
 *   metrica_esm_cantidad, metrica_esm_tiempo_ds
 * 
 * PLC Register mapping (Sección1):
 *   D29003 = parada_esm_cantidad (stop events)
 *   D29004 = parada_esm_tiempo_s (stop time in seconds)
 *   D29005 = metrica_esm_cantidad (ESM cycles - NOT products!)
 *   D29006 = metrica_esm_tiempo_ds (ESM cycle time in deciseconds)
 * 
 * ESM = Esmaltadora (Glazing Machine)
 */

class EsmalteProcessor : public IMessageProcessor
{
private:
    struct State
    {
        bool initialized = false;
        int shift = 0;

        uint16_t last_accepted_timer1Hz = 0;
        // Época del último mensaje ACEPTADO, para medir el hueco (D2).
        // Semántica y unidad distintas a last_accepted_time (dedup): no reutilizar.
        int64_t last_accepted_epoch_s = 0;

        // All 16-bit counters - last values and accumulators
        uint16_t last_timer1Hz = 0;
        uint8_t  rc_timer1Hz = 0;
        uint32_t acc_timer1Hz = 0;

        uint16_t last_parada_esm_cantidad = 0;
        uint8_t  rc_parada_esm_cantidad = 0;
        uint32_t acc_parada_esm_cantidad = 0;

        uint16_t last_parada_esm_tiempo = 0;
        uint8_t  rc_parada_esm_tiempo = 0;
        uint32_t acc_parada_esm_tiempo_s = 0;

        uint16_t last_metrica_esm_cantidad = 0;
        uint8_t  rc_metrica_esm_cantidad = 0;
        uint32_t acc_metrica_esm_cantidad = 0;

        uint16_t last_metrica_esm_tiempo = 0;
        uint8_t  rc_metrica_esm_tiempo = 0;
        uint32_t acc_metrica_esm_tiempo_ds = 0;

        // Segundos del turno en curso que nadie observó (huecos por reinicio).
        int64_t acc_unobserved_s = 0;
        // Traza de re-siembra ya emitida por la restauración. No se serializa.
        bool suppress_reseed_log = false;

        // Serialización generada a partir de los miembros del struct: se
        // persisten TODOS (acumuladores, tracking de raw, contadores de
        // rechazo, EMA y estado de deduplicación), no solo los acc_*.
        nlohmann::json to_json() const {
            nlohmann::json j;
            j["v"] = celima::kStateSchemaVersion;
            j["acc_unobserved_s"] = acc_unobserved_s;
            j["initialized"] = initialized;
            j["shift"] = shift;
            j["last_accepted_timer1Hz"] = last_accepted_timer1Hz;
            j["last_accepted_epoch_s"] = last_accepted_epoch_s;
            j["last_timer1Hz"] = last_timer1Hz;
            j["rc_timer1Hz"] = rc_timer1Hz;
            j["acc_timer1Hz"] = acc_timer1Hz;
            j["last_parada_esm_cantidad"] = last_parada_esm_cantidad;
            j["rc_parada_esm_cantidad"] = rc_parada_esm_cantidad;
            j["acc_parada_esm_cantidad"] = acc_parada_esm_cantidad;
            j["last_parada_esm_tiempo"] = last_parada_esm_tiempo;
            j["rc_parada_esm_tiempo"] = rc_parada_esm_tiempo;
            j["acc_parada_esm_tiempo_s"] = acc_parada_esm_tiempo_s;
            j["last_metrica_esm_cantidad"] = last_metrica_esm_cantidad;
            j["rc_metrica_esm_cantidad"] = rc_metrica_esm_cantidad;
            j["acc_metrica_esm_cantidad"] = acc_metrica_esm_cantidad;
            j["last_metrica_esm_tiempo"] = last_metrica_esm_tiempo;
            j["rc_metrica_esm_tiempo"] = rc_metrica_esm_tiempo;
            j["acc_metrica_esm_tiempo_ds"] = acc_metrica_esm_tiempo_ds;
            return j;
        }

        void from_json(const nlohmann::json &j) {
            acc_unobserved_s = j.value("acc_unobserved_s", acc_unobserved_s);
            initialized = j.value("initialized", initialized);
            shift = j.value("shift", shift);
            last_accepted_timer1Hz = j.value("last_accepted_timer1Hz", last_accepted_timer1Hz);
            last_accepted_epoch_s = j.value("last_accepted_epoch_s", last_accepted_epoch_s);
            last_timer1Hz = j.value("last_timer1Hz", last_timer1Hz);
            rc_timer1Hz = j.value("rc_timer1Hz", rc_timer1Hz);
            acc_timer1Hz = j.value("acc_timer1Hz", acc_timer1Hz);
            last_parada_esm_cantidad = j.value("last_parada_esm_cantidad", last_parada_esm_cantidad);
            rc_parada_esm_cantidad = j.value("rc_parada_esm_cantidad", rc_parada_esm_cantidad);
            acc_parada_esm_cantidad = j.value("acc_parada_esm_cantidad", acc_parada_esm_cantidad);
            last_parada_esm_tiempo = j.value("last_parada_esm_tiempo", last_parada_esm_tiempo);
            rc_parada_esm_tiempo = j.value("rc_parada_esm_tiempo", rc_parada_esm_tiempo);
            acc_parada_esm_tiempo_s = j.value("acc_parada_esm_tiempo_s", acc_parada_esm_tiempo_s);
            last_metrica_esm_cantidad = j.value("last_metrica_esm_cantidad", last_metrica_esm_cantidad);
            rc_metrica_esm_cantidad = j.value("rc_metrica_esm_cantidad", rc_metrica_esm_cantidad);
            acc_metrica_esm_cantidad = j.value("acc_metrica_esm_cantidad", acc_metrica_esm_cantidad);
            last_metrica_esm_tiempo = j.value("last_metrica_esm_tiempo", last_metrica_esm_tiempo);
            rc_metrica_esm_tiempo = j.value("rc_metrica_esm_tiempo", rc_metrica_esm_tiempo);
            acc_metrica_esm_tiempo_ds = j.value("acc_metrica_esm_tiempo_ds", acc_metrica_esm_tiempo_ds);
        }
    };

    static std::mutex mtx_;
    static std::unordered_map<int, State> states_;

public:
    static void reset_states() {
        std::lock_guard<std::mutex> lock(mtx_);
        states_.clear();
    }

    std::vector<Publication> process(const json &msg,
                                     const std::string &isa95_prefix,
                                     int shift_mode = 3) override
    {
        auto sh = current_shift_localtime(shift_mode);
        int shiftNum = (sh == Shift::S1 ? 1 : (sh == Shift::S2 ? 2 : 3));

        // === Read header fields ===
        int line = jsonu::get_opt<int>(msg, "lineID").value_or(0);
        const auto dev_epoch = device_epoch_s(msg);
        int64_t unobserved_s = 0;   // segundos del turno sin observar
        int alarms = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int checksum = jsonu::get_opt<int>(msg, "checksum").value_or(0);
        int deviceType = jsonu::get_opt<int>(msg, "deviceType").value_or(0);

        // === Read all 16-bit counters (new semantic names from decoder v2) ===
        uint16_t timer1Hz = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "timer1Hz").value_or(0));

        uint16_t parada_esm_cantidad = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "parada_esm_cantidad").value_or(0));

        uint16_t parada_esm_tiempo = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "parada_esm_tiempo_s").value_or(0));

        uint16_t metrica_esm_cantidad = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "metrica_esm_cantidad").value_or(0));

        uint16_t metrica_esm_tiempo = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "metrica_esm_tiempo_ds").value_or(0));

        // === Output accumulators ===
        uint32_t acc_timer1Hz_out = 0;
        uint32_t acc_parada_esm_cantidad_out = 0;
        uint32_t acc_parada_esm_tiempo_s_out = 0;
        uint32_t acc_metrica_esm_cantidad_out = 0;
        uint32_t acc_metrica_esm_tiempo_ds_out = 0;

        {
            std::lock_guard<std::mutex> lock(mtx_);
            State &st = states_[line];
            // Restauración del estado persistido, una vez por clave y arranque.
            // Va PRIMERO: decide si este mensaje continúa el acumulador o lo pone a
            // cero, y deja last_accepted_epoch_s en su sitio para que el hueco de
            // abajo sea el real y no cero.
            const int64_t restart_gap_s =
                restore_state_if_needed(st, "esmalte", line, shiftNum, shift_mode, dev_epoch);
            if (restart_gap_s > 0 && st.initialized)
                st.acc_unobserved_s += restart_gap_s;   // caso 4: hueco dentro del turno

            CounterCtx ctx{};
            ctx.line = line;
            ctx.proc = "esmalte";
            ctx.rate_max_per_s = celima::rates().rate_per_s(line, ctx.proc);
            ctx.margin         = celima::rates().margin();
            // Hueco desde el último mensaje aceptado de esta clave. Sin epoch de
            // dispositivo queda en 0 y la cota declara el delta implausible.
            if (dev_epoch && st.last_accepted_epoch_s > 0)
                ctx.elapsed_s = static_cast<double>(*dev_epoch - st.last_accepted_epoch_s);

            if (!st.initialized || st.shift != shiftNum) {
                // reseed: una vez por procesador y línea, antes de pisar el estado.
                // suppress_reseed_log lo pone la restauración cuando ya emitió la
                // traza del cambio de turno a través del reinicio.
                if (!st.suppress_reseed_log)
                    celima::log::state_event("reseed", line, "esmalte",
                        st.initialized
                            ? ("reason=shift_change shift_prev=" + std::to_string(st.shift) +
                               " shift_new=" + std::to_string(shiftNum))
                            : ("reason=first_message shift=" + std::to_string(shiftNum)));
                // New shift - reset all accumulators and store initial values
                st = State();
                // Los segundos no observados pertenecen al turno: 0 en un cambio de
                // turno normal, el hueco en un cambio a través de un reinicio (caso 2).
                st.acc_unobserved_s = restart_gap_s;
                st.initialized = true;
                st.shift = shiftNum;

                st.last_accepted_timer1Hz = timer1Hz;
                if (dev_epoch) st.last_accepted_epoch_s = *dev_epoch;
                st.last_timer1Hz = timer1Hz;
                st.last_parada_esm_cantidad = parada_esm_cantidad;
                st.last_parada_esm_tiempo = parada_esm_tiempo;
                st.last_metrica_esm_cantidad = metrica_esm_cantidad;
                st.last_metrica_esm_tiempo = metrica_esm_tiempo;
            }
            else {
                // Duplicate-frame rejection
                if (timer1Hz == st.last_accepted_timer1Hz) {
                    std::cout << "[Esmalte] Trama repetida descartada (lineID=" << line
                              << " timer1Hz=" << timer1Hz << ")\n";
                    return {};
                }
                st.last_accepted_timer1Hz = timer1Hz;
                if (dev_epoch) st.last_accepted_epoch_s = *dev_epoch;

                // Accumulate deltas — ALL PLC registers have bit-15 flag, use diff_counter for everything
                st.acc_timer1Hz += diff_counter_safe(timer1Hz, st.last_timer1Hz, st.rc_timer1Hz, ctx.with("timer1Hz"));

                st.acc_parada_esm_cantidad += diff_counter_safe(parada_esm_cantidad, st.last_parada_esm_cantidad, st.rc_parada_esm_cantidad, ctx.with("parada_esm_cantidad"));

                st.acc_parada_esm_tiempo_s += diff_counter_safe(parada_esm_tiempo, st.last_parada_esm_tiempo, st.rc_parada_esm_tiempo, ctx.with("parada_esm_tiempo"));

                st.acc_metrica_esm_cantidad += diff_counter_safe(metrica_esm_cantidad, st.last_metrica_esm_cantidad, st.rc_metrica_esm_cantidad, ctx.with("metrica_esm_cantidad"));

                st.acc_metrica_esm_tiempo_ds += diff_counter_safe(metrica_esm_tiempo, st.last_metrica_esm_tiempo, st.rc_metrica_esm_tiempo, ctx.with("metrica_esm_tiempo"));
            }

            // Copy accumulated values to output
            acc_timer1Hz_out = st.acc_timer1Hz;
            acc_parada_esm_cantidad_out = st.acc_parada_esm_cantidad;
            acc_parada_esm_tiempo_s_out = st.acc_parada_esm_tiempo_s;
            acc_metrica_esm_cantidad_out = st.acc_metrica_esm_cantidad;
            acc_metrica_esm_tiempo_ds_out = st.acc_metrica_esm_tiempo_ds;

            // Guardar tras procesar el mensaje, dentro del mismo mutex que protege
            // el estado: un corte de energía no da oportunidad de cerrar limpio, así
            // que no vale dejarlo solo en SIGTERM.
            unobserved_s = st.acc_unobserved_s;
            persist_state(st, "esmalte", line, shiftNum);
        }

        // === Build output JSON with CORRECT semantic field names ===
        json prod;
        prod["maquina_id"] = 5;
        prod["turno"] = shiftNum;
        prod["deviceType"] = deviceType;
        prod["lineID"] = line;
        prod["checksum"] = checksum;

        // Timer/validation (D29001)
        prod["timer1Hz_instantaneo"] = timer1Hz;
        prod["timer1Hz_turno"] = acc_timer1Hz_out;  // firmware Arduino ya corrige alineamiento de bit

        // Parada ESM (stops) - D29003, D29004
        prod["parada_esm_instantaneo"] = parada_esm_cantidad;
        prod["parada_esm_turno"] = acc_parada_esm_cantidad_out;
        prod["parada_esm_tiempo_instantaneo_s"] = parada_esm_tiempo;
        prod["parada_esm_tiempo_turno_s"] = acc_parada_esm_tiempo_s_out;  // firmware Arduino ya corrige alineamiento de bit

        // Métrica ESM (cycles) - D29005, D29006
        // NOTE: These are ESM machine CYCLES, NOT product count!
        prod["metrica_esm_instantaneo"] = metrica_esm_cantidad;
        prod["metrica_esm_turno"] = acc_metrica_esm_cantidad_out;  // firmware Arduino ya corrige alineamiento de bit
        prod["metrica_esm_tiempo_instantaneo_ds"] = metrica_esm_tiempo;
        prod["metrica_esm_tiempo_turno_ds"] = acc_metrica_esm_tiempo_ds_out;  // CF100: 1 tick = 0.1s = 1ds (validated, no ×2)

        // Convert deciseconds to seconds for convenience
        prod["metrica_esm_tiempo_turno_s"] = static_cast<double>(acc_metrica_esm_tiempo_ds_out) * 0.1;  // CF100: each tick = 0.1s (validated)

        // Diagnostic log: confirm delta semantics after Arduino firmware fix.
        // Una línea por mensaje aceptado es demasiado para el camino caliente
        // (~36.000 msg/día), así que queda tras CELIMA_DEBUG_ESM=1. El getenv se
        // resuelve una sola vez.
        static const bool esm_diag = [] {
            const char* v = std::getenv("CELIMA_DEBUG_ESM");
            return v && v[0] == '1';
        }();
        if (esm_diag) {
            std::cerr << "[ESM diag] line=" << line
                      << " metrica_esm_raw=" << metrica_esm_cantidad
                      << " acc_metrica_turno=" << acc_metrica_esm_cantidad_out
                      << " timer1Hz_raw=" << timer1Hz
                      << " acc_timer1Hz_turno=" << acc_timer1Hz_out
                      << " parada_esm_raw=" << parada_esm_cantidad
                      << " acc_parada_turno=" << acc_parada_esm_cantidad_out
                      << "\n";
        }

        prod["timestamp_device"] = device_timestamp(msg);
        add_unobserved_marker(prod, unobserved_s);

        // Alarms
        json qual;
        qual["alarms"] = alarms;
        qual["timestamp_device"] = device_timestamp(msg);

        auto t1 = isa95_prefix + std::to_string(line) + "/esmalte/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/esmalte/production";

        return { make_pub(t1, qual), make_pub(t2, prod) };
    }
};

// Static definitions
std::mutex EsmalteProcessor::mtx_;
std::unordered_map<int, EsmalteProcessor::State> EsmalteProcessor::states_;

/**
 * EntradaHornoProcessor - CORRECTED VERSION v2
 * 
 * CRITICAL FIX: Previous decoder had wrong register mapping!
 * This processor reads from the CORRECTED decoder v2 field names.
 * 
 * Input fields (from decoder v2):
 *   checksum, timer1Hz, alarms, parada_mcf_cantidad, parada_mcf_tiempo_s,
 *   metrica_mcf_cantidad, metrica_mcf_tiempo_ds, numero_grades,
 *   metrica_formador_cantidad, metrica_formador_tiempo_ds,
 *   falha_forno_cantidad, falha_forno_tiempo_s
 * 
 * PLC Register mapping (Sección14):
 *   D29003 = parada_mcf_cantidad (MCF stop count)
 *   D29004 = parada_mcf_tiempo_s (MCF stop time, seconds)
 *   D29005 = metrica_mcf_cantidad (MCF cycle count)
 *   D29006 = metrica_mcf_tiempo_ds (MCF time, deciseconds)
 *   D29007 = numero_grades (PRODUCTION COUNT! BCD from D410)
 *   D29008 = metrica_formador_cantidad (formador cycle count)
 *   D29009 = metrica_formador_tiempo_ds (formador time, deciseconds)
 *   D29013 = falha_forno_cantidad (furnace failure count)
 *   D29014 = falha_forno_tiempo_s (furnace failure time, seconds)
 * 
 * MCF = Mesa de Carga Forno (Kiln Loading Table)
 * FORMADOR = Formador de Grades (Rack Former)
 */

class EntradaHornoProcessor : public IMessageProcessor
{
private:
    struct State
    {
        bool initialized = false;
        int shift = 0;

        uint16_t last_accepted_timer1Hz = 0;
        // Época del último mensaje ACEPTADO, para medir el hueco (D2).
        // Semántica y unidad distintas a last_accepted_time (dedup): no reutilizar.
        int64_t last_accepted_epoch_s = 0;

        // Timer
        uint16_t last_timer1Hz = 0;
        uint8_t  rc_timer1Hz = 0;
        uint32_t acc_timer1Hz = 0;

        // Production: Número de Grades (D29007)
        uint16_t last_numero_grades = 0;
        uint8_t  rc_numero_grades = 0;
        uint32_t acc_numero_grades = 0;

        // Parada MCF - D29003, D29004
        uint16_t last_parada_mcf_cantidad = 0;
        uint8_t  rc_parada_mcf_cantidad = 0;
        uint32_t acc_parada_mcf_cantidad = 0;

        uint16_t last_parada_mcf_tiempo = 0;
        uint8_t  rc_parada_mcf_tiempo = 0;
        uint32_t acc_parada_mcf_tiempo_s = 0;

        // Métrica MCF - D29005, D29006
        uint16_t last_metrica_mcf_cantidad = 0;
        uint8_t  rc_metrica_mcf_cantidad = 0;
        uint32_t acc_metrica_mcf_cantidad = 0;

        uint16_t last_metrica_mcf_tiempo = 0;
        uint8_t  rc_metrica_mcf_tiempo = 0;
        uint32_t acc_metrica_mcf_tiempo_ds = 0;

        // Métrica Formador - D29008, D29009
        uint16_t last_metrica_formador_cantidad = 0;
        uint8_t  rc_metrica_formador_cantidad = 0;
        uint32_t acc_metrica_formador_cantidad = 0;

        uint16_t last_metrica_formador_tiempo = 0;
        uint8_t  rc_metrica_formador_tiempo = 0;
        uint32_t acc_metrica_formador_tiempo_ds = 0;

        // Falha Forno - D29013, D29014
        uint16_t last_falha_forno_cantidad = 0;
        uint8_t  rc_falha_forno_cantidad = 0;
        uint32_t acc_falha_forno_cantidad = 0;

        uint16_t last_falha_forno_tiempo = 0;
        uint8_t  rc_falha_forno_tiempo = 0;
        uint32_t acc_falha_forno_tiempo_s = 0;

        // Void detection: accumulated seconds where no units entered the furnace
        uint32_t acc_sin_entrada_s = 0;

        // Segundos del turno en curso que nadie observó (huecos por reinicio).
        int64_t acc_unobserved_s = 0;
        // Traza de re-siembra ya emitida por la restauración. No se serializa.
        bool suppress_reseed_log = false;

        // Serialización generada a partir de los miembros del struct: se
        // persisten TODOS (acumuladores, tracking de raw, contadores de
        // rechazo, EMA y estado de deduplicación), no solo los acc_*.
        nlohmann::json to_json() const {
            nlohmann::json j;
            j["v"] = celima::kStateSchemaVersion;
            j["acc_unobserved_s"] = acc_unobserved_s;
            j["initialized"] = initialized;
            j["shift"] = shift;
            j["last_accepted_timer1Hz"] = last_accepted_timer1Hz;
            j["last_accepted_epoch_s"] = last_accepted_epoch_s;
            j["last_timer1Hz"] = last_timer1Hz;
            j["rc_timer1Hz"] = rc_timer1Hz;
            j["acc_timer1Hz"] = acc_timer1Hz;
            j["last_numero_grades"] = last_numero_grades;
            j["rc_numero_grades"] = rc_numero_grades;
            j["acc_numero_grades"] = acc_numero_grades;
            j["last_parada_mcf_cantidad"] = last_parada_mcf_cantidad;
            j["rc_parada_mcf_cantidad"] = rc_parada_mcf_cantidad;
            j["acc_parada_mcf_cantidad"] = acc_parada_mcf_cantidad;
            j["last_parada_mcf_tiempo"] = last_parada_mcf_tiempo;
            j["rc_parada_mcf_tiempo"] = rc_parada_mcf_tiempo;
            j["acc_parada_mcf_tiempo_s"] = acc_parada_mcf_tiempo_s;
            j["last_metrica_mcf_cantidad"] = last_metrica_mcf_cantidad;
            j["rc_metrica_mcf_cantidad"] = rc_metrica_mcf_cantidad;
            j["acc_metrica_mcf_cantidad"] = acc_metrica_mcf_cantidad;
            j["last_metrica_mcf_tiempo"] = last_metrica_mcf_tiempo;
            j["rc_metrica_mcf_tiempo"] = rc_metrica_mcf_tiempo;
            j["acc_metrica_mcf_tiempo_ds"] = acc_metrica_mcf_tiempo_ds;
            j["last_metrica_formador_cantidad"] = last_metrica_formador_cantidad;
            j["rc_metrica_formador_cantidad"] = rc_metrica_formador_cantidad;
            j["acc_metrica_formador_cantidad"] = acc_metrica_formador_cantidad;
            j["last_metrica_formador_tiempo"] = last_metrica_formador_tiempo;
            j["rc_metrica_formador_tiempo"] = rc_metrica_formador_tiempo;
            j["acc_metrica_formador_tiempo_ds"] = acc_metrica_formador_tiempo_ds;
            j["last_falha_forno_cantidad"] = last_falha_forno_cantidad;
            j["rc_falha_forno_cantidad"] = rc_falha_forno_cantidad;
            j["acc_falha_forno_cantidad"] = acc_falha_forno_cantidad;
            j["last_falha_forno_tiempo"] = last_falha_forno_tiempo;
            j["rc_falha_forno_tiempo"] = rc_falha_forno_tiempo;
            j["acc_falha_forno_tiempo_s"] = acc_falha_forno_tiempo_s;
            j["acc_sin_entrada_s"] = acc_sin_entrada_s;
            return j;
        }

        void from_json(const nlohmann::json &j) {
            acc_unobserved_s = j.value("acc_unobserved_s", acc_unobserved_s);
            initialized = j.value("initialized", initialized);
            shift = j.value("shift", shift);
            last_accepted_timer1Hz = j.value("last_accepted_timer1Hz", last_accepted_timer1Hz);
            last_accepted_epoch_s = j.value("last_accepted_epoch_s", last_accepted_epoch_s);
            last_timer1Hz = j.value("last_timer1Hz", last_timer1Hz);
            rc_timer1Hz = j.value("rc_timer1Hz", rc_timer1Hz);
            acc_timer1Hz = j.value("acc_timer1Hz", acc_timer1Hz);
            last_numero_grades = j.value("last_numero_grades", last_numero_grades);
            rc_numero_grades = j.value("rc_numero_grades", rc_numero_grades);
            acc_numero_grades = j.value("acc_numero_grades", acc_numero_grades);
            last_parada_mcf_cantidad = j.value("last_parada_mcf_cantidad", last_parada_mcf_cantidad);
            rc_parada_mcf_cantidad = j.value("rc_parada_mcf_cantidad", rc_parada_mcf_cantidad);
            acc_parada_mcf_cantidad = j.value("acc_parada_mcf_cantidad", acc_parada_mcf_cantidad);
            last_parada_mcf_tiempo = j.value("last_parada_mcf_tiempo", last_parada_mcf_tiempo);
            rc_parada_mcf_tiempo = j.value("rc_parada_mcf_tiempo", rc_parada_mcf_tiempo);
            acc_parada_mcf_tiempo_s = j.value("acc_parada_mcf_tiempo_s", acc_parada_mcf_tiempo_s);
            last_metrica_mcf_cantidad = j.value("last_metrica_mcf_cantidad", last_metrica_mcf_cantidad);
            rc_metrica_mcf_cantidad = j.value("rc_metrica_mcf_cantidad", rc_metrica_mcf_cantidad);
            acc_metrica_mcf_cantidad = j.value("acc_metrica_mcf_cantidad", acc_metrica_mcf_cantidad);
            last_metrica_mcf_tiempo = j.value("last_metrica_mcf_tiempo", last_metrica_mcf_tiempo);
            rc_metrica_mcf_tiempo = j.value("rc_metrica_mcf_tiempo", rc_metrica_mcf_tiempo);
            acc_metrica_mcf_tiempo_ds = j.value("acc_metrica_mcf_tiempo_ds", acc_metrica_mcf_tiempo_ds);
            last_metrica_formador_cantidad = j.value("last_metrica_formador_cantidad", last_metrica_formador_cantidad);
            rc_metrica_formador_cantidad = j.value("rc_metrica_formador_cantidad", rc_metrica_formador_cantidad);
            acc_metrica_formador_cantidad = j.value("acc_metrica_formador_cantidad", acc_metrica_formador_cantidad);
            last_metrica_formador_tiempo = j.value("last_metrica_formador_tiempo", last_metrica_formador_tiempo);
            rc_metrica_formador_tiempo = j.value("rc_metrica_formador_tiempo", rc_metrica_formador_tiempo);
            acc_metrica_formador_tiempo_ds = j.value("acc_metrica_formador_tiempo_ds", acc_metrica_formador_tiempo_ds);
            last_falha_forno_cantidad = j.value("last_falha_forno_cantidad", last_falha_forno_cantidad);
            rc_falha_forno_cantidad = j.value("rc_falha_forno_cantidad", rc_falha_forno_cantidad);
            acc_falha_forno_cantidad = j.value("acc_falha_forno_cantidad", acc_falha_forno_cantidad);
            last_falha_forno_tiempo = j.value("last_falha_forno_tiempo", last_falha_forno_tiempo);
            rc_falha_forno_tiempo = j.value("rc_falha_forno_tiempo", rc_falha_forno_tiempo);
            acc_falha_forno_tiempo_s = j.value("acc_falha_forno_tiempo_s", acc_falha_forno_tiempo_s);
            acc_sin_entrada_s = j.value("acc_sin_entrada_s", acc_sin_entrada_s);
        }
    };

    static std::mutex mtx_;
    static std::unordered_map<int, State> states_;

public:
    static void reset_states() {
        std::lock_guard<std::mutex> lock(mtx_);
        states_.clear();
    }

    std::vector<Publication> process(const json &msg,
                                     const std::string &isa95_prefix,
                                     int shift_mode = 3) override
    {
        auto sh = current_shift_localtime(shift_mode);
        int shiftNum = (sh == Shift::S1 ? 1 : (sh == Shift::S2 ? 2 : 3));

        // === Read header fields ===
        int line = jsonu::get_opt<int>(msg, "lineID").value_or(0);
        const auto dev_epoch = device_epoch_s(msg);
        int64_t unobserved_s = 0;   // segundos del turno sin observar
        int alarms = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int checksum = jsonu::get_opt<int>(msg, "checksum").value_or(0);
        int deviceType = jsonu::get_opt<int>(msg, "deviceType").value_or(0);

        // === Read all 16-bit counters (CORRECTED field names from decoder v2) ===
        
        uint16_t timer1Hz = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "timer1Hz").value_or(0));

        // PRODUCTION COUNT - D29007 (BCD converted from D410)
        uint16_t numero_grades = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "numero_grades").value_or(0));

        // Parada MCF - D29003, D29004
        uint16_t parada_mcf_cantidad = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "parada_mcf_cantidad").value_or(0));
        uint16_t parada_mcf_tiempo = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "parada_mcf_tiempo_s").value_or(0));

        // Métrica MCF - D29005, D29006
        uint16_t metrica_mcf_cantidad = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "metrica_mcf_cantidad").value_or(0));
        uint16_t metrica_mcf_tiempo = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "metrica_mcf_tiempo_ds").value_or(0));

        // Métrica Formador - D29008, D29009
        uint16_t metrica_formador_cantidad = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "metrica_formador_cantidad").value_or(0));
        uint16_t metrica_formador_tiempo = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "metrica_formador_tiempo_ds").value_or(0));

        // Falha Forno - D29013, D29014
        uint16_t falha_forno_cantidad = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "falha_forno_cantidad").value_or(0));
        uint16_t falha_forno_tiempo = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "falha_forno_tiempo_s").value_or(0));

        // === Output accumulators ===
        uint32_t acc_timer1Hz_out = 0;
        uint32_t acc_numero_grades_out = 0;
        uint32_t acc_parada_mcf_cantidad_out = 0;
        uint32_t acc_parada_mcf_tiempo_s_out = 0;
        uint32_t acc_metrica_mcf_cantidad_out = 0;
        uint32_t acc_metrica_mcf_tiempo_ds_out = 0;
        uint32_t acc_metrica_formador_cantidad_out = 0;
        uint32_t acc_metrica_formador_tiempo_ds_out = 0;
        uint32_t acc_falha_forno_cantidad_out = 0;
        uint32_t acc_falha_forno_tiempo_s_out = 0;
        uint32_t acc_sin_entrada_s_out = 0;

        {
            std::lock_guard<std::mutex> lock(mtx_);
            State &st = states_[line];
            // Restauración del estado persistido, una vez por clave y arranque.
            // Va PRIMERO: decide si este mensaje continúa el acumulador o lo pone a
            // cero, y deja last_accepted_epoch_s en su sitio para que el hueco de
            // abajo sea el real y no cero.
            const int64_t restart_gap_s =
                restore_state_if_needed(st, "entrada_horno", line, shiftNum, shift_mode, dev_epoch);
            if (restart_gap_s > 0 && st.initialized)
                st.acc_unobserved_s += restart_gap_s;   // caso 4: hueco dentro del turno

            CounterCtx ctx{};
            ctx.line = line;
            ctx.proc = "entrada_horno";
            ctx.rate_max_per_s = celima::rates().rate_per_s(line, ctx.proc);
            ctx.margin         = celima::rates().margin();
            // Hueco desde el último mensaje aceptado de esta clave. Sin epoch de
            // dispositivo queda en 0 y la cota declara el delta implausible.
            if (dev_epoch && st.last_accepted_epoch_s > 0)
                ctx.elapsed_s = static_cast<double>(*dev_epoch - st.last_accepted_epoch_s);

            if (!st.initialized || st.shift != shiftNum) {
                // reseed: una vez por procesador y línea, antes de pisar el estado.
                // suppress_reseed_log lo pone la restauración cuando ya emitió la
                // traza del cambio de turno a través del reinicio.
                if (!st.suppress_reseed_log)
                    celima::log::state_event("reseed", line, "entrada_horno",
                        st.initialized
                            ? ("reason=shift_change shift_prev=" + std::to_string(st.shift) +
                               " shift_new=" + std::to_string(shiftNum))
                            : ("reason=first_message shift=" + std::to_string(shiftNum)));
                // New shift - reset all accumulators and store initial values
                st = State();
                // Los segundos no observados pertenecen al turno: 0 en un cambio de
                // turno normal, el hueco en un cambio a través de un reinicio (caso 2).
                st.acc_unobserved_s = restart_gap_s;
                st.initialized = true;
                st.shift = shiftNum;

                st.last_accepted_timer1Hz = timer1Hz;
                if (dev_epoch) st.last_accepted_epoch_s = *dev_epoch;
                st.last_timer1Hz = timer1Hz;
                st.last_numero_grades = numero_grades;
                st.last_parada_mcf_cantidad = parada_mcf_cantidad;
                st.last_parada_mcf_tiempo = parada_mcf_tiempo;
                st.last_metrica_mcf_cantidad = metrica_mcf_cantidad;
                st.last_metrica_mcf_tiempo = metrica_mcf_tiempo;
                st.last_metrica_formador_cantidad = metrica_formador_cantidad;
                st.last_metrica_formador_tiempo = metrica_formador_tiempo;
                st.last_falha_forno_cantidad = falha_forno_cantidad;
                st.last_falha_forno_tiempo = falha_forno_tiempo;
            }
            else {
                // Duplicate-frame rejection
                if (timer1Hz == st.last_accepted_timer1Hz) {
                    std::cout << "[EntradaHorno] Trama repetida descartada (lineID=" << line
                              << " timer1Hz=" << timer1Hz << ")\n";
                    return {};
                }
                st.last_accepted_timer1Hz = timer1Hz;
                if (dev_epoch) st.last_accepted_epoch_s = *dev_epoch;

                // Accumulate deltas — ALL PLC registers have bit-15 flag, use diff_counter for everything
                uint32_t delta_timer = diff_counter_safe(timer1Hz, st.last_timer1Hz, st.rc_timer1Hz, ctx.with("timer1Hz"));
                st.acc_timer1Hz += delta_timer;

                st.acc_numero_grades += diff_counter_safe(numero_grades, st.last_numero_grades, st.rc_numero_grades, ctx.with("numero_grades"));

                st.acc_parada_mcf_cantidad += diff_counter_safe(parada_mcf_cantidad, st.last_parada_mcf_cantidad, st.rc_parada_mcf_cantidad, ctx.with("parada_mcf_cantidad"));

                st.acc_parada_mcf_tiempo_s += diff_counter_safe(parada_mcf_tiempo, st.last_parada_mcf_tiempo, st.rc_parada_mcf_tiempo, ctx.with("parada_mcf_tiempo"));

                uint32_t delta_mcf = diff_counter_safe(metrica_mcf_cantidad, st.last_metrica_mcf_cantidad, st.rc_metrica_mcf_cantidad, ctx.with("metrica_mcf_cantidad"));
                st.acc_metrica_mcf_cantidad += delta_mcf;

                st.acc_metrica_mcf_tiempo_ds += diff_counter_safe(metrica_mcf_tiempo, st.last_metrica_mcf_tiempo, st.rc_metrica_mcf_tiempo, ctx.with("metrica_mcf_tiempo"));

                st.acc_metrica_formador_cantidad += diff_counter_safe(metrica_formador_cantidad, st.last_metrica_formador_cantidad, st.rc_metrica_formador_cantidad, ctx.with("metrica_formador_cantidad"));

                st.acc_metrica_formador_tiempo_ds += diff_counter_safe(metrica_formador_tiempo, st.last_metrica_formador_tiempo, st.rc_metrica_formador_tiempo, ctx.with("metrica_formador_tiempo"));

                st.acc_falha_forno_cantidad += diff_counter_safe(falha_forno_cantidad, st.last_falha_forno_cantidad, st.rc_falha_forno_cantidad, ctx.with("falha_forno_cantidad"));

                st.acc_falha_forno_tiempo_s += diff_counter_safe(falha_forno_tiempo, st.last_falha_forno_tiempo, st.rc_falha_forno_tiempo, ctx.with("falha_forno_tiempo"));

                // Void detection: if no units entered this interval, count elapsed time as void
                // delta_mcf == 0 means sensor saw zero new pieces since last message
                if (delta_mcf == 0 && delta_timer > 0) {
                    st.acc_sin_entrada_s += delta_timer;  // firmware Arduino ya corrige alineamiento de bit
                }
            }

            // Copy accumulated values to output
            acc_timer1Hz_out = st.acc_timer1Hz;
            acc_numero_grades_out = st.acc_numero_grades;
            acc_parada_mcf_cantidad_out = st.acc_parada_mcf_cantidad;
            acc_parada_mcf_tiempo_s_out = st.acc_parada_mcf_tiempo_s;
            acc_metrica_mcf_cantidad_out = st.acc_metrica_mcf_cantidad;
            acc_metrica_mcf_tiempo_ds_out = st.acc_metrica_mcf_tiempo_ds;
            acc_metrica_formador_cantidad_out = st.acc_metrica_formador_cantidad;
            acc_metrica_formador_tiempo_ds_out = st.acc_metrica_formador_tiempo_ds;
            acc_falha_forno_cantidad_out = st.acc_falha_forno_cantidad;
            acc_falha_forno_tiempo_s_out = st.acc_falha_forno_tiempo_s;
            acc_sin_entrada_s_out = st.acc_sin_entrada_s;

            // Guardar tras procesar el mensaje, dentro del mismo mutex que protege
            // el estado: un corte de energía no da oportunidad de cerrar limpio, así
            // que no vale dejarlo solo en SIGTERM.
            unobserved_s = st.acc_unobserved_s;
            persist_state(st, "entrada_horno", line, shiftNum);
        }

        // === Build output JSON with CORRECT semantic field names ===
        json prod;
        prod["maquina_id"] = 6;
        prod["turno"] = shiftNum;
        prod["deviceType"] = deviceType;
        prod["lineID"] = line;
        prod["checksum"] = checksum;

        // Timer/validation
        prod["timer1Hz_instantaneo"] = timer1Hz;
        prod["timer1Hz_turno"] = acc_timer1Hz_out;  // firmware Arduino ya corrige alineamiento de bit

        // PRODUCTION COUNT - numero_grades (D29007)
        prod["numero_grades_instantaneo"] = numero_grades;
        prod["numero_grades_turno"] = acc_numero_grades_out;

        // Parada MCF - D29003, D29004
        prod["parada_mcf_instantaneo"] = parada_mcf_cantidad;
        prod["parada_mcf_turno"] = acc_parada_mcf_cantidad_out;
        prod["parada_mcf_tiempo_instantaneo_s"] = parada_mcf_tiempo;
        prod["parada_mcf_tiempo_turno_s"] = acc_parada_mcf_tiempo_s_out;  // firmware Arduino ya corrige alineamiento de bit

        // Métrica MCF - D29005, D29006
        prod["metrica_mcf_instantaneo"] = metrica_mcf_cantidad;
        prod["metrica_mcf_turno"] = acc_metrica_mcf_cantidad_out;  // firmware Arduino ya corrige alineamiento de bit
        prod["metrica_mcf_tiempo_instantaneo_ds"] = metrica_mcf_tiempo;
        prod["metrica_mcf_tiempo_turno_ds"] = acc_metrica_mcf_tiempo_ds_out;  // CF100: 1 tick = 0.1s = 1ds (validated, no ×2)
        prod["metrica_mcf_tiempo_turno_s"] = static_cast<double>(acc_metrica_mcf_tiempo_ds_out) * 0.1;  // CF100: each tick = 0.1s (validated)

        // Métrica Formador - D29008, D29009
        prod["metrica_formador_instantaneo"] = metrica_formador_cantidad;
        prod["metrica_formador_turno"] = acc_metrica_formador_cantidad_out;  // firmware Arduino ya corrige alineamiento de bit
        prod["metrica_formador_tiempo_instantaneo_ds"] = metrica_formador_tiempo;
        prod["metrica_formador_tiempo_turno_ds"] = acc_metrica_formador_tiempo_ds_out;  // CF100: 1 tick = 0.1s = 1ds (validated, no ×2)
        prod["metrica_formador_tiempo_turno_s"] = static_cast<double>(acc_metrica_formador_tiempo_ds_out) * 0.1;  // CF100: each tick = 0.1s (validated)

        // Falha Forno - D29013, D29014
        prod["falha_forno_instantaneo"] = falha_forno_cantidad;
        prod["falha_forno_turno"] = acc_falha_forno_cantidad_out;
        prod["falha_forno_tiempo_instantaneo_s"] = falha_forno_tiempo;
        prod["falha_forno_tiempo_turno_s"] = acc_falha_forno_tiempo_s_out;  // firmware Arduino ya corrige alineamiento de bit

        // Void time: accumulated seconds where metrica_mcf_cantidad delta was 0
        // (no new units detected entering the furnace during that message interval)
        // Resets on shift change along with all other accumulators
        prod["sin_entrada_turno_s"] = acc_sin_entrada_s_out;

        prod["timestamp_device"] = device_timestamp(msg);
        add_unobserved_marker(prod, unobserved_s);

        // Alarms
        json qual;
        qual["alarms"] = alarms;
        qual["timestamp_device"] = device_timestamp(msg);

        auto t1 = isa95_prefix + std::to_string(line) + "/entrada_horno/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/entrada_horno/production";

        return { make_pub(t1, qual), make_pub(t2, prod) };
    }
};

// Static definitions
std::mutex EntradaHornoProcessor::mtx_;
std::unordered_map<int, EntradaHornoProcessor::State> EntradaHornoProcessor::states_;

/**
 * SalidaHornoProcessor - CORRECTED VERSION
 * 
 * Changes from original:
 * 1. Reads NEW semantic field names from LoRaWAN decoder
 * 2. ALL registers use bit-15 flag — diff_counter for everything
 * 3. Outputs OLD field names in JSON for backward compatibility
 * 
 * Input fields (from decoder v3):
 *   checksum, timer1Hz, alarms, paradas_cantidad, paradas_tempo,
 *   metrica_mdf_ciclos, metrica_mdf_tiempo, bancalinos_q301, bancalinos_q300,
 *   bancalinos_comb1, parada_escolha_cantidad, parada_escolha_tempo,
 *   sentido_escolha_cantidad, sentido_escolha_tiempo, barreira1_cantidad,
 *   barreira1_tiempo, bancalinos_comb2, bancalinos_total
 * 
 * Output fields (backward compatible):
 *   cantidad_*, bancalinos0_*, bancalinos1_*, paradas_1_*, paradas_2_*, etc.
 */

class SalidaHornoProcessor : public IMessageProcessor
{
private:
    struct State
    {
        bool initialized = false;
        int shift = 0;

        uint16_t last_accepted_timer1Hz = 0;
        // Época del último mensaje ACEPTADO, para medir el hueco (D2).
        // Semántica y unidad distintas a last_accepted_time (dedup): no reutilizar.
        int64_t last_accepted_epoch_s = 0;

        // All 16-bit counters - last values and accumulators
        uint16_t last_timer1Hz = 0;
        uint8_t  rc_timer1Hz = 0;
        uint32_t acc_timer1Hz = 0;
        uint32_t acc_tiempo_operacion_s = 0;

        uint16_t last_paradas_cantidad = 0;
        uint8_t  rc_paradas_cantidad = 0;
        uint32_t acc_paradas_cantidad = 0;

        uint16_t last_paradas_tempo = 0;
        uint8_t  rc_paradas_tempo = 0;
        uint32_t acc_paradas_tempo = 0;

        uint16_t last_metrica_ciclos = 0;
        uint8_t  rc_metrica_ciclos = 0;
        uint32_t acc_metrica_ciclos = 0;

        uint16_t last_metrica_tiempo = 0;
        uint8_t  rc_metrica_tiempo = 0;
        uint32_t acc_metrica_tiempo = 0;

        uint16_t last_bancalinos_q301 = 0;
        uint8_t  rc_bancalinos_q301 = 0;
        uint32_t acc_bancalinos_q301 = 0;

        uint16_t last_bancalinos_q300 = 0;
        uint8_t  rc_bancalinos_q300 = 0;
        uint32_t acc_bancalinos_q300 = 0;

        uint16_t last_bancalinos_comb1 = 0;
        uint8_t  rc_bancalinos_comb1 = 0;
        uint32_t acc_bancalinos_comb1 = 0;

        uint16_t last_bancalinos_comb2 = 0;
        uint8_t  rc_bancalinos_comb2 = 0;
        uint32_t acc_bancalinos_comb2 = 0;

        uint16_t last_bancalinos_total = 0;
        uint8_t  rc_bancalinos_total = 0;
        uint32_t acc_bancalinos_total = 0;

        uint16_t last_parada_escolha_cantidad = 0;
        uint8_t  rc_parada_escolha_cantidad = 0;
        uint32_t acc_parada_escolha_cantidad = 0;

        uint16_t last_parada_escolha_tempo = 0;
        uint8_t  rc_parada_escolha_tempo = 0;
        uint32_t acc_parada_escolha_tempo = 0;

        uint16_t last_sentido_escolha_cantidad = 0;
        uint8_t  rc_sentido_escolha_cantidad = 0;
        uint32_t acc_sentido_escolha_cantidad = 0;

        uint16_t last_sentido_escolha_tiempo = 0;
        uint8_t  rc_sentido_escolha_tiempo = 0;
        uint32_t acc_sentido_escolha_tiempo = 0;

        uint16_t last_barreira1_cantidad = 0;
        uint8_t  rc_barreira1_cantidad = 0;
        uint32_t acc_barreira1_cantidad = 0;

        uint16_t last_barreira1_tiempo = 0;
        uint8_t  rc_barreira1_tiempo = 0;
        uint32_t acc_barreira1_tiempo = 0;

        // EMA of per-field deltas for corrupt-frame detection.
        // Only counter fields are used as anchors: their normal delta (~28/interval)
        // is well below the floor (300), so false positives from data gaps are impossible.
        // Time fields (~1869/interval) exceed any useful floor and cannot be used.
        // -1.0f = not yet initialized (uses floor threshold until first valid sample).
        float ema_metrica_ciclos     = -1.0f;
        float ema_barreira1_cantidad = -1.0f;

        // Segundos del turno en curso que nadie observó (huecos por reinicio).
        int64_t acc_unobserved_s = 0;
        // Traza de re-siembra ya emitida por la restauración. No se serializa.
        bool suppress_reseed_log = false;

        // Serialización generada a partir de los miembros del struct: se
        // persisten TODOS (acumuladores, tracking de raw, contadores de
        // rechazo, EMA y estado de deduplicación), no solo los acc_*.
        nlohmann::json to_json() const {
            nlohmann::json j;
            j["v"] = celima::kStateSchemaVersion;
            j["acc_unobserved_s"] = acc_unobserved_s;
            j["initialized"] = initialized;
            j["shift"] = shift;
            j["last_accepted_timer1Hz"] = last_accepted_timer1Hz;
            j["last_accepted_epoch_s"] = last_accepted_epoch_s;
            j["last_timer1Hz"] = last_timer1Hz;
            j["rc_timer1Hz"] = rc_timer1Hz;
            j["acc_timer1Hz"] = acc_timer1Hz;
            j["acc_tiempo_operacion_s"] = acc_tiempo_operacion_s;
            j["last_paradas_cantidad"] = last_paradas_cantidad;
            j["rc_paradas_cantidad"] = rc_paradas_cantidad;
            j["acc_paradas_cantidad"] = acc_paradas_cantidad;
            j["last_paradas_tempo"] = last_paradas_tempo;
            j["rc_paradas_tempo"] = rc_paradas_tempo;
            j["acc_paradas_tempo"] = acc_paradas_tempo;
            j["last_metrica_ciclos"] = last_metrica_ciclos;
            j["rc_metrica_ciclos"] = rc_metrica_ciclos;
            j["acc_metrica_ciclos"] = acc_metrica_ciclos;
            j["last_metrica_tiempo"] = last_metrica_tiempo;
            j["rc_metrica_tiempo"] = rc_metrica_tiempo;
            j["acc_metrica_tiempo"] = acc_metrica_tiempo;
            j["last_bancalinos_q301"] = last_bancalinos_q301;
            j["rc_bancalinos_q301"] = rc_bancalinos_q301;
            j["acc_bancalinos_q301"] = acc_bancalinos_q301;
            j["last_bancalinos_q300"] = last_bancalinos_q300;
            j["rc_bancalinos_q300"] = rc_bancalinos_q300;
            j["acc_bancalinos_q300"] = acc_bancalinos_q300;
            j["last_bancalinos_comb1"] = last_bancalinos_comb1;
            j["rc_bancalinos_comb1"] = rc_bancalinos_comb1;
            j["acc_bancalinos_comb1"] = acc_bancalinos_comb1;
            j["last_bancalinos_comb2"] = last_bancalinos_comb2;
            j["rc_bancalinos_comb2"] = rc_bancalinos_comb2;
            j["acc_bancalinos_comb2"] = acc_bancalinos_comb2;
            j["last_bancalinos_total"] = last_bancalinos_total;
            j["rc_bancalinos_total"] = rc_bancalinos_total;
            j["acc_bancalinos_total"] = acc_bancalinos_total;
            j["last_parada_escolha_cantidad"] = last_parada_escolha_cantidad;
            j["rc_parada_escolha_cantidad"] = rc_parada_escolha_cantidad;
            j["acc_parada_escolha_cantidad"] = acc_parada_escolha_cantidad;
            j["last_parada_escolha_tempo"] = last_parada_escolha_tempo;
            j["rc_parada_escolha_tempo"] = rc_parada_escolha_tempo;
            j["acc_parada_escolha_tempo"] = acc_parada_escolha_tempo;
            j["last_sentido_escolha_cantidad"] = last_sentido_escolha_cantidad;
            j["rc_sentido_escolha_cantidad"] = rc_sentido_escolha_cantidad;
            j["acc_sentido_escolha_cantidad"] = acc_sentido_escolha_cantidad;
            j["last_sentido_escolha_tiempo"] = last_sentido_escolha_tiempo;
            j["rc_sentido_escolha_tiempo"] = rc_sentido_escolha_tiempo;
            j["acc_sentido_escolha_tiempo"] = acc_sentido_escolha_tiempo;
            j["last_barreira1_cantidad"] = last_barreira1_cantidad;
            j["rc_barreira1_cantidad"] = rc_barreira1_cantidad;
            j["acc_barreira1_cantidad"] = acc_barreira1_cantidad;
            j["last_barreira1_tiempo"] = last_barreira1_tiempo;
            j["rc_barreira1_tiempo"] = rc_barreira1_tiempo;
            j["acc_barreira1_tiempo"] = acc_barreira1_tiempo;
            j["ema_metrica_ciclos"] = ema_metrica_ciclos;
            j["ema_barreira1_cantidad"] = ema_barreira1_cantidad;
            return j;
        }

        void from_json(const nlohmann::json &j) {
            acc_unobserved_s = j.value("acc_unobserved_s", acc_unobserved_s);
            initialized = j.value("initialized", initialized);
            shift = j.value("shift", shift);
            last_accepted_timer1Hz = j.value("last_accepted_timer1Hz", last_accepted_timer1Hz);
            last_accepted_epoch_s = j.value("last_accepted_epoch_s", last_accepted_epoch_s);
            last_timer1Hz = j.value("last_timer1Hz", last_timer1Hz);
            rc_timer1Hz = j.value("rc_timer1Hz", rc_timer1Hz);
            acc_timer1Hz = j.value("acc_timer1Hz", acc_timer1Hz);
            acc_tiempo_operacion_s = j.value("acc_tiempo_operacion_s", acc_tiempo_operacion_s);
            last_paradas_cantidad = j.value("last_paradas_cantidad", last_paradas_cantidad);
            rc_paradas_cantidad = j.value("rc_paradas_cantidad", rc_paradas_cantidad);
            acc_paradas_cantidad = j.value("acc_paradas_cantidad", acc_paradas_cantidad);
            last_paradas_tempo = j.value("last_paradas_tempo", last_paradas_tempo);
            rc_paradas_tempo = j.value("rc_paradas_tempo", rc_paradas_tempo);
            acc_paradas_tempo = j.value("acc_paradas_tempo", acc_paradas_tempo);
            last_metrica_ciclos = j.value("last_metrica_ciclos", last_metrica_ciclos);
            rc_metrica_ciclos = j.value("rc_metrica_ciclos", rc_metrica_ciclos);
            acc_metrica_ciclos = j.value("acc_metrica_ciclos", acc_metrica_ciclos);
            last_metrica_tiempo = j.value("last_metrica_tiempo", last_metrica_tiempo);
            rc_metrica_tiempo = j.value("rc_metrica_tiempo", rc_metrica_tiempo);
            acc_metrica_tiempo = j.value("acc_metrica_tiempo", acc_metrica_tiempo);
            last_bancalinos_q301 = j.value("last_bancalinos_q301", last_bancalinos_q301);
            rc_bancalinos_q301 = j.value("rc_bancalinos_q301", rc_bancalinos_q301);
            acc_bancalinos_q301 = j.value("acc_bancalinos_q301", acc_bancalinos_q301);
            last_bancalinos_q300 = j.value("last_bancalinos_q300", last_bancalinos_q300);
            rc_bancalinos_q300 = j.value("rc_bancalinos_q300", rc_bancalinos_q300);
            acc_bancalinos_q300 = j.value("acc_bancalinos_q300", acc_bancalinos_q300);
            last_bancalinos_comb1 = j.value("last_bancalinos_comb1", last_bancalinos_comb1);
            rc_bancalinos_comb1 = j.value("rc_bancalinos_comb1", rc_bancalinos_comb1);
            acc_bancalinos_comb1 = j.value("acc_bancalinos_comb1", acc_bancalinos_comb1);
            last_bancalinos_comb2 = j.value("last_bancalinos_comb2", last_bancalinos_comb2);
            rc_bancalinos_comb2 = j.value("rc_bancalinos_comb2", rc_bancalinos_comb2);
            acc_bancalinos_comb2 = j.value("acc_bancalinos_comb2", acc_bancalinos_comb2);
            last_bancalinos_total = j.value("last_bancalinos_total", last_bancalinos_total);
            rc_bancalinos_total = j.value("rc_bancalinos_total", rc_bancalinos_total);
            acc_bancalinos_total = j.value("acc_bancalinos_total", acc_bancalinos_total);
            last_parada_escolha_cantidad = j.value("last_parada_escolha_cantidad", last_parada_escolha_cantidad);
            rc_parada_escolha_cantidad = j.value("rc_parada_escolha_cantidad", rc_parada_escolha_cantidad);
            acc_parada_escolha_cantidad = j.value("acc_parada_escolha_cantidad", acc_parada_escolha_cantidad);
            last_parada_escolha_tempo = j.value("last_parada_escolha_tempo", last_parada_escolha_tempo);
            rc_parada_escolha_tempo = j.value("rc_parada_escolha_tempo", rc_parada_escolha_tempo);
            acc_parada_escolha_tempo = j.value("acc_parada_escolha_tempo", acc_parada_escolha_tempo);
            last_sentido_escolha_cantidad = j.value("last_sentido_escolha_cantidad", last_sentido_escolha_cantidad);
            rc_sentido_escolha_cantidad = j.value("rc_sentido_escolha_cantidad", rc_sentido_escolha_cantidad);
            acc_sentido_escolha_cantidad = j.value("acc_sentido_escolha_cantidad", acc_sentido_escolha_cantidad);
            last_sentido_escolha_tiempo = j.value("last_sentido_escolha_tiempo", last_sentido_escolha_tiempo);
            rc_sentido_escolha_tiempo = j.value("rc_sentido_escolha_tiempo", rc_sentido_escolha_tiempo);
            acc_sentido_escolha_tiempo = j.value("acc_sentido_escolha_tiempo", acc_sentido_escolha_tiempo);
            last_barreira1_cantidad = j.value("last_barreira1_cantidad", last_barreira1_cantidad);
            rc_barreira1_cantidad = j.value("rc_barreira1_cantidad", rc_barreira1_cantidad);
            acc_barreira1_cantidad = j.value("acc_barreira1_cantidad", acc_barreira1_cantidad);
            last_barreira1_tiempo = j.value("last_barreira1_tiempo", last_barreira1_tiempo);
            rc_barreira1_tiempo = j.value("rc_barreira1_tiempo", rc_barreira1_tiempo);
            acc_barreira1_tiempo = j.value("acc_barreira1_tiempo", acc_barreira1_tiempo);
            ema_metrica_ciclos = j.value("ema_metrica_ciclos", ema_metrica_ciclos);
            ema_barreira1_cantidad = j.value("ema_barreira1_cantidad", ema_barreira1_cantidad);
        }
    };

    static std::mutex mtx_;
    static std::unordered_map<int, State> states_;

public:
    static void reset_states() {
        std::lock_guard<std::mutex> lock(mtx_);
        states_.clear();
    }

    std::vector<Publication> process(const json &msg,
                                     const std::string &isa95_prefix,
                                     int shift_mode = 3) override
    {
        auto sh = current_shift_localtime(shift_mode);
        int shiftNum = (sh == Shift::S1 ? 1 : (sh == Shift::S2 ? 2 : 3));

        // === Read header fields ===
        int line = jsonu::get_opt<int>(msg, "lineID").value_or(0);
        const auto dev_epoch = device_epoch_s(msg);
        int64_t unobserved_s = 0;   // segundos del turno sin observar
        int alarms = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int checksum = jsonu::get_opt<int>(msg, "checksum").value_or(0);
        int deviceType = jsonu::get_opt<int>(msg, "deviceType").value_or(0);

        // === Read all 16-bit counters (new semantic names from decoder v3) ===
        uint16_t timer1Hz = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "timer1Hz").value_or(0));
        
        uint16_t paradas_cantidad = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "paradas_cantidad").value_or(0));
        
        uint16_t paradas_tempo = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "paradas_tempo").value_or(0));
        
        uint16_t metrica_ciclos = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "metrica_mdf_ciclos").value_or(0));
        
        uint16_t metrica_tiempo = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "metrica_mdf_tiempo").value_or(0));
        
        uint16_t bancalinos_q301 = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "bancalinos_q301").value_or(0));
        
        uint16_t bancalinos_q300 = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "bancalinos_q300").value_or(0));
        
        uint16_t bancalinos_comb1 = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "bancalinos_comb1").value_or(0));
        
        uint16_t bancalinos_comb2 = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "bancalinos_comb2").value_or(0));
        
        uint16_t bancalinos_total = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "bancalinos_total").value_or(0));
        
        uint16_t parada_escolha_cantidad = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "parada_escolha_cantidad").value_or(0));
        
        uint16_t parada_escolha_tempo = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "parada_escolha_tempo").value_or(0));
        
        uint16_t sentido_escolha_cantidad = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "sentido_escolha_cantidad").value_or(0));
        
        uint16_t sentido_escolha_tiempo = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "sentido_escolha_tiempo").value_or(0));
        
        uint16_t barreira1_cantidad = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "barreira1_cantidad").value_or(0));
        
        uint16_t barreira1_tiempo = static_cast<uint16_t>(
            jsonu::get_opt<int>(msg, "barreira1_tiempo").value_or(0));

        // === Output accumulators ===
        uint32_t acc_timer1Hz_out = 0;
        uint32_t acc_tiempo_operacion_s_out = 0;
        uint32_t acc_paradas_cantidad_out = 0;
        uint32_t acc_paradas_tempo_out = 0;
        uint32_t acc_metrica_ciclos_out = 0;
        uint32_t acc_metrica_tiempo_out = 0;
        uint32_t acc_bancalinos_q301_out = 0;
        uint32_t acc_bancalinos_q300_out = 0;
        uint32_t acc_bancalinos_comb1_out = 0;
        uint32_t acc_bancalinos_comb2_out = 0;
        uint32_t acc_bancalinos_total_out = 0;
        uint32_t acc_parada_escolha_cantidad_out = 0;
        uint32_t acc_parada_escolha_tempo_out = 0;
        uint32_t acc_sentido_escolha_cantidad_out = 0;
        uint32_t acc_sentido_escolha_tiempo_out = 0;
        uint32_t acc_barreira1_cantidad_out = 0;
        uint32_t acc_barreira1_tiempo_out = 0;

        {
            std::lock_guard<std::mutex> lock(mtx_);
            State &st = states_[line];
            // Restauración del estado persistido, una vez por clave y arranque.
            // Va PRIMERO: decide si este mensaje continúa el acumulador o lo pone a
            // cero, y deja last_accepted_epoch_s en su sitio para que el hueco de
            // abajo sea el real y no cero.
            const int64_t restart_gap_s =
                restore_state_if_needed(st, "salida_horno", line, shiftNum, shift_mode, dev_epoch);
            if (restart_gap_s > 0 && st.initialized)
                st.acc_unobserved_s += restart_gap_s;   // caso 4: hueco dentro del turno

            CounterCtx ctx{};
            ctx.line = line;
            ctx.proc = "salida_horno";
            ctx.rate_max_per_s = celima::rates().rate_per_s(line, ctx.proc);
            ctx.margin         = celima::rates().margin();
            // Hueco desde el último mensaje aceptado de esta clave. Sin epoch de
            // dispositivo queda en 0 y la cota declara el delta implausible.
            if (dev_epoch && st.last_accepted_epoch_s > 0)
                ctx.elapsed_s = static_cast<double>(*dev_epoch - st.last_accepted_epoch_s);

            if (!st.initialized || st.shift != shiftNum) {
                // reseed: una vez por procesador y línea, antes de pisar el estado.
                // suppress_reseed_log lo pone la restauración cuando ya emitió la
                // traza del cambio de turno a través del reinicio.
                if (!st.suppress_reseed_log)
                    celima::log::state_event("reseed", line, "salida_horno",
                        st.initialized
                            ? ("reason=shift_change shift_prev=" + std::to_string(st.shift) +
                               " shift_new=" + std::to_string(shiftNum))
                            : ("reason=first_message shift=" + std::to_string(shiftNum)));
                // New shift - reset all accumulators and store initial values
                st = State();
                // Los segundos no observados pertenecen al turno: 0 en un cambio de
                // turno normal, el hueco en un cambio a través de un reinicio (caso 2).
                st.acc_unobserved_s = restart_gap_s;
                st.initialized = true;
                st.shift = shiftNum;

                st.last_accepted_timer1Hz = timer1Hz;
                if (dev_epoch) st.last_accepted_epoch_s = *dev_epoch;
                st.last_timer1Hz = timer1Hz;
                st.last_paradas_cantidad = paradas_cantidad;
                st.last_paradas_tempo = paradas_tempo;
                st.last_metrica_ciclos = metrica_ciclos;
                st.last_metrica_tiempo = metrica_tiempo;
                st.last_bancalinos_q301 = bancalinos_q301;
                st.last_bancalinos_q300 = bancalinos_q300;
                st.last_bancalinos_comb1 = bancalinos_comb1;
                st.last_bancalinos_comb2 = bancalinos_comb2;
                st.last_bancalinos_total = bancalinos_total;
                st.last_parada_escolha_cantidad = parada_escolha_cantidad;
                st.last_parada_escolha_tempo = parada_escolha_tempo;
                st.last_sentido_escolha_cantidad = sentido_escolha_cantidad;
                st.last_sentido_escolha_tiempo = sentido_escolha_tiempo;
                st.last_barreira1_cantidad = barreira1_cantidad;
                st.last_barreira1_tiempo = barreira1_tiempo;
            }
            else {
                // Duplicate-frame rejection
                if (timer1Hz == st.last_accepted_timer1Hz) {
                    std::cout << "[SalidaHorno] Trama repetida descartada (lineID=" << line
                              << " timer1Hz=" << timer1Hz << ")\n";
                    return {};
                }
                st.last_accepted_timer1Hz = timer1Hz;
                if (dev_epoch) st.last_accepted_epoch_s = *dev_epoch;

                // Corrupt-frame detection using counter fields only.
                // Time fields (~1869/interval) exceed any useful detection floor and
                // cannot distinguish corruption from legitimate data gaps — excluded.
                // Counter fields (metrica_ciclos, barreira1_cantidad): normal delta ~28/interval,
                // well below floor=300. Corruption creates deltas of 10000-50000: unambiguous.
                uint16_t rd_mc  = static_cast<uint16_t>(metrica_ciclos    - st.last_metrica_ciclos);
                uint16_t rd_b1c = static_cast<uint16_t>(barreira1_cantidad - st.last_barreira1_cantidad);

                int n_spikes = 0;
                if (spike_detected(rd_mc,  st.ema_metrica_ciclos,     300, 10.0f)) n_spikes++;
                if (spike_detected(rd_b1c, st.ema_barreira1_cantidad, 300, 10.0f)) n_spikes++;

                if (n_spikes >= 2) {
                    std::cout << "[SalidaHorno] Frame corrupto descartado (lineID=" << line
                              << " n_spikes=" << n_spikes
                              << " rd_mc=" << rd_mc << " rd_b1c=" << rd_b1c << ")\n";
                    // Fall through to output copy — publish last known good state unchanged.
                } else {
                    // Update EMA only on clean frames (corrupt deltas must not poison the average).
                    spike_ema_update(st.ema_metrica_ciclos,     rd_mc);
                    spike_ema_update(st.ema_barreira1_cantidad, rd_b1c);

                    // Accumulate deltas — ALL PLC registers have bit-15 flag, use diff_counter for everything
                    uint16_t delta_timer = diff_counter_safe(timer1Hz, st.last_timer1Hz, st.rc_timer1Hz, ctx.with("timer1Hz"));
                    st.acc_timer1Hz += delta_timer;
                    st.acc_tiempo_operacion_s += delta_timer;

                    st.acc_paradas_cantidad += diff_counter_safe(paradas_cantidad, st.last_paradas_cantidad, st.rc_paradas_cantidad, ctx.with("paradas_cantidad"));

                    st.acc_paradas_tempo += diff_counter_safe(paradas_tempo, st.last_paradas_tempo, st.rc_paradas_tempo, ctx.with("paradas_tempo"));

                    st.acc_metrica_ciclos += diff_counter_safe(metrica_ciclos, st.last_metrica_ciclos, st.rc_metrica_ciclos, ctx.with("metrica_ciclos"));

                    st.acc_metrica_tiempo += diff_counter_safe(metrica_tiempo, st.last_metrica_tiempo, st.rc_metrica_tiempo, ctx.with("metrica_tiempo"));

                    st.acc_bancalinos_q301 += diff_counter_safe(bancalinos_q301, st.last_bancalinos_q301, st.rc_bancalinos_q301, ctx.with("bancalinos_q301"));

                    st.acc_bancalinos_q300 += diff_counter_safe(bancalinos_q300, st.last_bancalinos_q300, st.rc_bancalinos_q300, ctx.with("bancalinos_q300"));

                    st.acc_bancalinos_comb1 += diff_counter_safe(bancalinos_comb1, st.last_bancalinos_comb1, st.rc_bancalinos_comb1, ctx.with("bancalinos_comb1"));

                    st.acc_bancalinos_comb2 += diff_counter_safe(bancalinos_comb2, st.last_bancalinos_comb2, st.rc_bancalinos_comb2, ctx.with("bancalinos_comb2"));

                    st.acc_bancalinos_total += diff_counter_safe(bancalinos_total, st.last_bancalinos_total, st.rc_bancalinos_total, ctx.with("bancalinos_total"));

                    st.acc_parada_escolha_cantidad += diff_counter_safe(parada_escolha_cantidad, st.last_parada_escolha_cantidad, st.rc_parada_escolha_cantidad, ctx.with("parada_escolha_cantidad"));

                    st.acc_parada_escolha_tempo += diff_counter_safe(parada_escolha_tempo, st.last_parada_escolha_tempo, st.rc_parada_escolha_tempo, ctx.with("parada_escolha_tempo"));

                    st.acc_sentido_escolha_cantidad += diff_counter_safe(sentido_escolha_cantidad, st.last_sentido_escolha_cantidad, st.rc_sentido_escolha_cantidad, ctx.with("sentido_escolha_cantidad"));

                    st.acc_sentido_escolha_tiempo += diff_counter_safe(sentido_escolha_tiempo, st.last_sentido_escolha_tiempo, st.rc_sentido_escolha_tiempo, ctx.with("sentido_escolha_tiempo"));

                    st.acc_barreira1_cantidad += diff_counter_safe(barreira1_cantidad, st.last_barreira1_cantidad, st.rc_barreira1_cantidad, ctx.with("barreira1_cantidad"));

                    st.acc_barreira1_tiempo += diff_counter_safe(barreira1_tiempo, st.last_barreira1_tiempo, st.rc_barreira1_tiempo, ctx.with("barreira1_tiempo"));
                }
            }

            // Copy accumulated values to output
            acc_timer1Hz_out = st.acc_timer1Hz;
            acc_tiempo_operacion_s_out = st.acc_tiempo_operacion_s;
            acc_paradas_cantidad_out = st.acc_paradas_cantidad;
            acc_paradas_tempo_out = st.acc_paradas_tempo;
            acc_metrica_ciclos_out = st.acc_metrica_ciclos;
            acc_metrica_tiempo_out = st.acc_metrica_tiempo;
            acc_bancalinos_q301_out = st.acc_bancalinos_q301;
            acc_bancalinos_q300_out = st.acc_bancalinos_q300;
            acc_bancalinos_comb1_out = st.acc_bancalinos_comb1;
            acc_bancalinos_comb2_out = st.acc_bancalinos_comb2;
            acc_bancalinos_total_out = st.acc_bancalinos_total;
            acc_parada_escolha_cantidad_out = st.acc_parada_escolha_cantidad;
            acc_parada_escolha_tempo_out = st.acc_parada_escolha_tempo;
            acc_sentido_escolha_cantidad_out = st.acc_sentido_escolha_cantidad;
            acc_sentido_escolha_tiempo_out = st.acc_sentido_escolha_tiempo;
            acc_barreira1_cantidad_out = st.acc_barreira1_cantidad;
            acc_barreira1_tiempo_out = st.acc_barreira1_tiempo;

            // Guardar tras procesar el mensaje, dentro del mismo mutex que protege
            // el estado: un corte de energía no da oportunidad de cerrar limpio, así
            // que no vale dejarlo solo en SIGTERM.
            unobserved_s = st.acc_unobserved_s;
            persist_state(st, "salida_horno", line, shiftNum);
        }

        // === Build output JSON with BACKWARD COMPATIBLE field names ===
        json prod;
        prod["maquina_id"] = 7;
        prod["turno"] = shiftNum;
        prod["deviceType"] = deviceType;
        prod["lineID"] = line;
        prod["checksum"] = checksum;

        // Timer/operation time
        prod["timer1Hz_instantaneo"] = timer1Hz;
        prod["tiempo_operacion_turno_s"] = acc_tiempo_operacion_s_out;  // firmware Arduino ya corrige alineamiento de bit

        // Main production counter (D25005 - metrica MDF ciclos)
        // OLD: cantidad → NEW: metrica_mdf_ciclos
        prod["cantidad_instantanea"] = metrica_ciclos;
        prod["cantidad_produccion_turno"] = acc_metrica_ciclos_out;  // firmware Arduino ya corrige alineamiento de bit

        // Cycle time accumulator (D25006 - metrica MDF tiempo)
        // OLD: cantidad_total → NEW: metrica_mdf_tiempo
        prod["cantidad_total_instantanea"] = metrica_tiempo;
        prod["cantidad_total_turno"] = acc_metrica_tiempo_out;  // CF100: 1 tick = 0.1s = 1ds (validated, no ×2)

        // Paradas cantidad (D25003)
        // OLD: paradas_1 → NEW: paradas_cantidad
        prod["paradas_1_instantaneo"] = paradas_cantidad;
        prod["paradas_1_turno"] = acc_paradas_cantidad_out;

        // Paradas tempo (D25004)
        // OLD: paradas_2 → NEW: paradas_tempo
        prod["paradas_2_instantaneo"] = paradas_tempo;
        prod["paradas_2_turno"] = acc_paradas_tempo_out;  // firmware Arduino ya corrige alineamiento de bit

        // Bancalinos Q:3.01 sin sensor (D25007) — activaciones Q:3.01 sin confirmar presencia de lozeta
        prod["bancalinos0_nosensor_instantaneo"] = bancalinos_q301;
        prod["bancalinos0_nosensor_turno"] = acc_bancalinos_q301_out;

        // Bancalinos Q:3.00 (D25008)
        // OLD: bancalinos1 → NEW: bancalinos_q300
        prod["bancalinos1_instantaneo"] = bancalinos_q300;
        prod["bancalinos1_turno"] = acc_bancalinos_q300_out;  // firmware Arduino ya corrige alineamiento de bit

        // Bancalinos Comb1: Q:3.01 AND I:1.09 (D25009) — activaciones con lozeta presente (sustituye bancalinos0)
        prod["bancalinos0_instantaneo"] = bancalinos_comb1;
        prod["bancalinos0_turno"] = acc_bancalinos_comb1_out;

        // Bancalinos Comb2: Q:3.01 AND Q:2.10 (D25016) - NOW INCLUDED!
        prod["bancalinosComb2_instantaneo"] = bancalinos_comb2;
        prod["bancalinosComb2_turno"] = acc_bancalinos_comb2_out;  // firmware Arduino ya corrige alineamiento de bit

        // Bancalinos Total: Q:3.00 AND Q:2.10 (D25017) - NOW INCLUDED!
        prod["bancalinosTotal_instantaneo"] = bancalinos_total;
        prod["bancalinosTotal_turno"] = acc_bancalinos_total_out;  // firmware Arduino ya corrige alineamiento de bit

        // Sentido Escolha (D25012, D25013)
        // OLD: cambioSentido → NEW: sentido_escolha
        prod["cambioSentido_instantaneo"] = sentido_escolha_cantidad;
        prod["cambioSentido_turno"] = acc_sentido_escolha_cantidad_out;
        prod["cambioSentidoTotal_instantaneo"] = sentido_escolha_tiempo;
        prod["cambioSentidoTotal_turno"] = acc_sentido_escolha_tiempo_out;  // CF100: 1 tick = 0.1s = 1ds (validated, no ×2)

        // Barreira 1 (D25014, D25015)
        // OLD: cambioBarrera → NEW: barreira1
        prod["cambioBarrera_instantaneo"] = barreira1_cantidad;
        prod["cambioBarrera_turno"] = acc_barreira1_cantidad_out;  // firmware Arduino ya corrige alineamiento de bit
        prod["cambioBarreraTotal_instantaneo"] = barreira1_tiempo;
        prod["cambioBarreraTotal_turno"] = acc_barreira1_tiempo_out;  // CF100: 1 tick = 0.1s = 1ds (validated, no ×2)

        // Parada Escolha (D25010, D25011) - NEW FIELDS
        prod["paradaEscolha_instantaneo"] = parada_escolha_cantidad;
        prod["paradaEscolha_turno"] = acc_parada_escolha_cantidad_out;
        prod["paradaEscolhaTempo_instantaneo"] = parada_escolha_tempo;
        prod["paradaEscolhaTempo_turno"] = acc_parada_escolha_tempo_out;  // firmware Arduino ya corrige alineamiento de bit

        prod["timestamp_device"] = device_timestamp(msg);
        add_unobserved_marker(prod, unobserved_s);

        // Alarms
        json qual;
        qual["alarms"] = alarms;
        qual["timestamp_device"] = device_timestamp(msg);

        auto t1 = isa95_prefix + std::to_string(line) + "/salida_horno/alarms";
        auto t2 = isa95_prefix + std::to_string(line) + "/salida_horno/production";

        return { make_pub(t1, qual), make_pub(t2, prod) };
    }
};

// Static definitions
std::mutex SalidaHornoProcessor::mtx_;
std::unordered_map<int, SalidaHornoProcessor::State> SalidaHornoProcessor::states_;

// ============================================================================
// Factory functions
// ============================================================================

std::unique_ptr<IMessageProcessor> createDefaultProcessor()
{
    return std::make_unique<DefaultProcessor>();
}

std::unique_ptr<IMessageProcessor> createProcessor(DeviceType dt)
{
    switch (dt)
    {
    case DeviceType::PH_1:
        return std::make_unique<PrensaHidraulica1Processor>();
    case DeviceType::PH_2:
        return std::make_unique<PrensaHidraulica2Processor>();
    case DeviceType::Calidad:
        return std::make_unique<CalidadProcessor>();
    case DeviceType::Entrada_secador:
        return std::make_unique<EntradaSecadorProcessor>();
    case DeviceType::Salida_secador:
        return std::make_unique<SalidaSecadorProcessor>();
    case DeviceType::Esmalte:
        return std::make_unique<EsmalteProcessor>();
    case DeviceType::Entrada_horno:
        return std::make_unique<EntradaHornoProcessor>();
    case DeviceType::Salida_horno:
        return std::make_unique<SalidaHornoProcessor>();
    default:
        return std::make_unique<DefaultProcessor>();
    }
}

void reset_all_processor_states()
{
    PrensaHidraulica1Processor::reset_states();
    PrensaHidraulica2Processor::reset_states();
    SalidaSecadorProcessor::reset_states();
    EntradaSecadorProcessor::reset_states();
    EsmalteProcessor::reset_states();
    EntradaHornoProcessor::reset_states();
    SalidaHornoProcessor::reset_states();
    CalidadProcessor::reset_states();
}