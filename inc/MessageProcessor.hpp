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
                                             const std::string& isa95_prefix) = 0;
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

// Delta seguro para contadores de 16 bits provenientes de PLCs
// Evita saltos absurdos (> max_reasonable), corrige rollover, descarta ruido.
inline uint32_t safe_delta_u16(uint16_t prev, uint16_t curr, int max_reasonable = 200)
{
    int delta = static_cast<int>(curr) - static_cast<int>(prev);

    // Caso normal pequeño
    if (delta >= 0 && delta <= max_reasonable)
        return static_cast<uint32_t>(delta);

    // Caso rollover
    if (delta < 0) {
        int rolled = delta + 65536;
        if (rolled >= 0 && rolled <= max_reasonable)
            return static_cast<uint32_t>(rolled);
    }

    // Salto anómalo → ruido → ignorar
    return 0;
}
