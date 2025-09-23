#include "MessageProcessor.hpp"
#include "JsonUtils.hpp"
#include <memory>
#include <sstream>
using json = nlohmann::json;

/**
 * Two ISA-95-style example topics. Adjust/extend later:
 *  1) <prefix>/production/line/quantity
 *  2) <prefix>/quality/alarms
 * Where <prefix> can be like: enterprise/site/area/line1
 */

static Publication make_pub(const std::string& topic, const json& j) {
    return Publication{topic, j.dump()};
}

/** Default processor: lightly normalize and forward a summary. */
class DefaultProcessor : public IMessageProcessor {
public:
    std::vector<Publication> process(const json& msg, const std::string& isa95_prefix) override {
        json out;
        out["source"] = "celima/data";
        out["observed"] = msg;

        // Put some commonly useful fields if present
        if (auto dev = jsonu::get_opt<std::string>(msg, "devEUI")) out["devEUI"] = *dev;
        if (auto dn  = jsonu::get_opt<std::string>(msg, "deviceName")) out["deviceName"] = *dn;
        if (auto dt  = jsonu::get_opt<int>(msg, "deviceType")) out["deviceType"] = *dt;

        // Example: publish to a “production” topic
        auto t1 = isa95_prefix + "/production/line/quantity";
        json p1;
        p1["quantity"] = jsonu::get_opt<int>(msg, "cantidad").value_or(0);
        p1["ts"] = std::time(nullptr);

        // Example: publish to a “quality/alarms” topic
        auto t2 = isa95_prefix + "/quality/alarms";
        json p2;
        p2["alarms"] = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        p2["ts"] = std::time(nullptr);

        return { make_pub(t1, p1), make_pub(t2, p2) };
    }
};

/** Example specialized processor: Calidad (deviceType 8) */
class CalidadProcessor : public IMessageProcessor {
public:
    std::vector<Publication> process(const json& msg, const std::string& isa95_prefix) override {
        // Interpret some likely quality-related signals
        int alarms = jsonu::get_opt<int>(msg, "alarms").value_or(0);
        int qty    = jsonu::get_opt<int>(msg, "cantidad").value_or(0);

        // Example outputs:
        json qual;
        qual["alarms"] = alarms;
        if (auto defects = jsonu::get_opt<int>(msg, "defects")) qual["defects"] = *defects;
        qual["ts"] = std::time(nullptr);

        json prod;
        prod["good_count"] = qty;
        if (auto rejects = jsonu::get_opt<int>(msg, "rejects")) prod["rejects"] = *rejects;
        prod["ts"] = std::time(nullptr);

        auto t1 = isa95_prefix + "/quality/alarms";
        auto t2 = isa95_prefix + "/production/line/quantity";

        return { make_pub(t1, qual), make_pub(t2, prod) };
    }
};

std::unique_ptr<IMessageProcessor> createDefaultProcessor() {
    return std::make_unique<DefaultProcessor>();
}

std::unique_ptr<IMessageProcessor> createProcessor(DeviceType dt) {
    switch (dt) {
        case DeviceType::Calidad:
            return std::make_unique<CalidadProcessor>();
        // Add more specialized processors here as needed:
        // case DeviceType::PH_1: return std::make_unique<PH1Processor>();
        default:
            return std::make_unique<DefaultProcessor>();
    }
}
