#pragma once
#include <mutex>
#include <string>
#include <unordered_map>

namespace celima {

/**
 * Tasas máximas por línea y máquina, para la cota de plausibilidad escalada.
 *
 * Se carga una vez al arrancar desde $CELIMA_RATES_CONFIG (por defecto
 * /etc/iot-celima-mqtt/rates.json). Si el archivo falta o no parsea, el
 * servicio NO aborta: registra el motivo una vez y sigue con
 * default_rate_per_h, que es deliberadamente bajo — tasa baja, cota baja, más
 * descartes, total subcontado, que es el lado seguro del error.
 *
 * Formato:
 *   {
 *     "default_rate_per_h": 600,
 *     "margin": 1.5,
 *     "lines": { "1": { "prensa_hidraulica1": 1500 } }
 *   }
 *
 * Resolución: lines[<lineID>][<maquina>] → default_rate_per_h si falta.
 */
class RateConfig {
public:
    static RateConfig& instance();

    // Carga desde $CELIMA_RATES_CONFIG o la ruta por defecto. Idempotente:
    // solo la primera llamada registra el resultado.
    void load_from_env();

    // Carga explícita (tests). Devuelve false si no se pudo usar el archivo;
    // en ese caso quedan los valores por defecto.
    bool load_file(const std::string& path);

    // Solo para tests: fija los valores sin pasar por un archivo.
    void set_for_tests(double default_rate_per_h, double margin);
    void set_rate_for_tests(int line, const std::string& proc, double rate_per_h);
    void reset_for_tests();

    // Tasa máxima en unidades por segundo para (línea, máquina).
    double rate_per_s(int line, const std::string& proc) const;
    double margin() const;
    double default_rate_per_h() const;

private:
    RateConfig() = default;
    bool apply_json(const std::string& text, std::string& err);

    mutable std::mutex mtx_;
    double default_rate_per_h_ = 600.0;   // conservador a propósito
    double margin_ = 1.5;
    // clave: "<line>/<proc>"
    std::unordered_map<std::string, double> rates_per_h_;
    bool loaded_ = false;
};

// Atajo de lectura.
inline RateConfig& rates() { return RateConfig::instance(); }

} // namespace celima
