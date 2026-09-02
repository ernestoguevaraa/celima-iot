#pragma once
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <nlohmann/json.hpp>

namespace celima {

// Versión del esquema del estado serializado. Si un registro trae una versión
// desconocida o mayor, se ignora y se re-siembra: nunca se interpreta un
// formato que no se conoce.
inline constexpr int kStateSchemaVersion = 1;

/**
 * Estado persistido de un (procesador, línea).
 *
 * shift y updated_at viajan fuera del JSON porque la restauración los necesita
 * antes de deserializar el resto: con ellos decide si el turno cambió y de qué
 * tamaño es el hueco.
 */
struct StoredState {
    int            shift      = -1;
    int64_t        updated_at = 0;    // época en segundos, UTC
    nlohmann::json state;
};

class IStateStore {
public:
    virtual ~IStateStore() = default;

    // false = no hay estado usable para esa clave (ausente, corrupto o de una
    // versión desconocida). Nunca lanza.
    virtual bool load(const std::string& proc, int line, StoredState& out) = 0;

    virtual bool save(const std::string& proc, int line,
                      int shift, int64_t updated_at, const nlohmann::json& st) = 0;
};

/** Doble en memoria, para las pruebas. */
class MemoryStateStore : public IStateStore {
public:
    bool load(const std::string& proc, int line, StoredState& out) override;
    bool save(const std::string& proc, int line,
              int shift, int64_t updated_at, const nlohmann::json& st) override;

    size_t size() const;
    void clear();
    int  save_count() const { return saves_; }

private:
    mutable std::mutex mtx_;
    std::unordered_map<std::string, StoredState> rows_;
    int saves_ = 0;
};

/**
 * Store SQLite. Nunca es causa de caída: si no se puede abrir la base, o un
 * save falla, se registra (con supresión de repetidos) y el servicio sigue
 * funcionando solo en memoria, que es el comportamiento de hoy.
 */
std::unique_ptr<IStateStore> make_sqlite_state_store(const std::string& path);

// ---------------------------------------------------------------------------
// Acceso global. nullptr = sin persistencia (comportamiento previo a PR 2).
IStateStore* state_store();

// Toma posesión del store. Pasar nullptr desactiva la persistencia.
void set_state_store(std::unique_ptr<IStateStore> store);

/**
 * Configura el store desde el entorno:
 *   CELIMA_STATE_PERSISTENCE=0  → desactivada (interruptor de emergencia:
 *                                 revertir en planta sin recompilar)
 *   CELIMA_STATE_DB=<ruta>      → por defecto /var/lib/iot-celima-mqtt/state.db
 */
void init_state_store_from_env();

// Ventana por debajo de la cual un hueco se considera corto y la restauración
// continúa sin más (caso 3). Por encima se registra el hueco (caso 4).
// 5x el intervalo normal de 180 s. Valor a revisar con datos de planta.
int gap_short_seconds();

} // namespace celima
