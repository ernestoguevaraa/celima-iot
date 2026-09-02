#pragma once
#include <string>
#include <vector>
#include <utility>
#include <optional>
#include <nlohmann/json.hpp>
#include "DeviceTypes.hpp"

const int L1_PIEZAS_PISADA = 3;
const int L2_PIEZAS_PISADA = 3;
const int L3_PIEZAS_PISADA = 2;
const int L4_PIEZAS_PISADA = 4;
const int L5_PIEZAS_PISADA = 2;
/**
 * Each processor returns a set of (topic, payload) publications.
 * All publications are QoS 1 (the app enforces it).
 */
struct Publication {
    std::string topic;
    std::string payload; // JSON string
};

class IMessageProcessor {
public:
    virtual ~IMessageProcessor() = default;
    virtual std::vector<Publication> process(const nlohmann::json& msg,
                                             const std::string& isa95_prefix,
                                             int shift_mode = 3) = 0;
};

/**
 * Factory to get a processor for a given DeviceType.
 * Unknown types fallback to DefaultProcessor (pass-through summarized).
 */
std::unique_ptr<IMessageProcessor> createProcessor(DeviceType dt);

/**
 * Default processor for unknown/unspecified device types.
 */
std::unique_ptr<IMessageProcessor> createDefaultProcessor();

bool detect_global_shift_change(int currentShift);
void reset_all_processor_states();

// Devuelve el turno global al estado de "aún no se ha visto ningún mensaje".
// Sirve para simular un arranque en frío en las pruebas; en producción solo
// ocurre una vez, al iniciar el proceso.
void reset_global_shift_state();

// Marcador de turno incompleto (apartado G). Sujeto a una decisión abierta con
// el equipo del edge processor, así que por defecto NO se publica: se activa con
// CELIMA_INCOMPLETE_SHIFT_MARKER=1. Estas dos funciones son para las pruebas.
void set_incomplete_shift_marker_for_tests(bool on);
void clear_incomplete_shift_marker_override();

/**
 * Contexto de un contador, para la aritmética de deltas y para etiquetar los
 * eventos [STATE]. Un argumento en lugar de siete: los procesadores lo
 * construyen una vez por mensaje y lo reutilizan con with(<campo>).
 *
 * line/proc/field solo etiquetan; elapsed_s y rate_max_per_s son los que
 * determinan la cota. Los valores por defecto conservan el fallback
 * "line=-1 proc=? field=?" que delata una llamada sin contexto.
 */
struct CounterCtx {
    int         line           = -1;
    const char* proc           = "?";
    const char* field          = "?";
    double      elapsed_s      = 0.0;   // desde el último mensaje ACEPTADO de esta clave
    double      rate_max_per_s = 0.0;   // de RateConfig
    double      margin         = 1.5;   // de RateConfig
    uint16_t    max_valid      = 5000;  // techo mínimo: la cota escalada nunca es más estricta
    uint8_t     max_rejects    = 3;

    CounterCtx with(const char* f) const { CounterCtx c = *this; c.field = f; return c; }
    CounterCtx with(const char* f, uint16_t mv) const
    {
        CounterCtx c = *this; c.field = f; c.max_valid = mv; return c;
    }
};

/**
 * Resultado de un delta acotado. plausible == false significa re-sembrar y
 * sumar 0; `reason` es lo que se emite en el evento [STATE] delta_rejected.
 */
struct DeltaResult {
    uint16_t    value         = 0;
    bool        plausible     = false;
    double      max_plausible = 0.0;
    const char* reason        = "";
};

DeltaResult diff_counter_scaled(uint16_t curr, uint16_t prev, const CounterCtx &ctx);

/**
 * Delta seguro para contadores de 16 bits provenientes de PLCs.
 *
 * Corrige el rollover y descarta el salto anómalo, igual que antes, pero la
 * cota ya no es un techo fijo: sale de diff_counter_scaled(), así que escala
 * con el hueco. ctx.max_valid se conserva como techo mínimo, de modo que el
 * comportamiento en intervalos normales es el mismo de siempre.
 *
 * Lo usa CalidadProcessor, que no tiene mecanismo de re-anclaje por rechazos
 * consecutivos: un delta implausible se descarta y se registra, sin más.
 */
uint32_t safe_delta_u16(uint16_t prev, uint16_t curr, const CounterCtx &ctx);
