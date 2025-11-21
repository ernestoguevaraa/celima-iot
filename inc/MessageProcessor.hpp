#pragma once
#include <string>
#include <vector>
#include <utility>
#include <optional>
#include <nlohmann/json.hpp>
#include "DeviceTypes.hpp"

const int PIEZAS_PISADA = 6;
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
