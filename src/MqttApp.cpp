#include "MqttApp.hpp"
#include "JsonUtils.hpp"
#include "MessageProcessor.hpp"
#include "DeviceTypes.hpp"
#include <iostream>
#include <thread>
#include <chrono>
#include <mqtt/async_client.h>
#include <vector>


using namespace std::chrono_literals;

static const std::vector<std::string> TOPICS = {
    "celima/data",
    "celima/error",
    "celima/join",
    "celima/ACK"
};
static const std::vector<int> QOS = {1,1,1,1};

MqttApp::MqttApp(std::string broker_uri, std::string client_id, std::string isa95_prefix)
    : broker_(std::move(broker_uri))
    , client_id_(std::move(client_id))
    , isa95_prefix_(std::move(isa95_prefix))
    , cli_(broker_, client_id_)
{
    connopts_.set_clean_session(false);
    connopts_.set_automatic_reconnect(true);
    cli_.set_callback(*this);
}

MqttApp::~MqttApp() {
    stop();
}

void MqttApp::start() {
    running_ = true;
    try {
        std::cout << "[MQTT] Connecting to " << broker_ << " as " << client_id_ << "...\n";
        cli_.connect(connopts_)->wait();
        std::cout << "[MQTT] Connected.\n";
    } catch (const mqtt::exception& e) {
        std::cerr << "[MQTT] Connect failed: " << e.what() << "\n";
        throw;
    }
}

void MqttApp::stop() {
    if (!running_) return;
    running_ = false;
    try {
        auto topic_filters = mqtt::string_collection::create(TOPICS);

        mqtt::properties props; // explicit, to satisfy some overload sets
        cli_.unsubscribe(topic_filters, props)->wait();

        cli_.disconnect()->wait();
        std::cout << "[MQTT] Disconnected.\n";
    } catch (const mqtt::exception& e) {
        std::cerr << "[MQTT] Stop error: " << e.what() << "\n";
    }
}


void MqttApp::subscribe_topics() {
    try {
        auto topic_filters = mqtt::string_collection::create(TOPICS); // << not iterators

        mqtt::iasync_client::qos_collection qos_vals(QOS.begin(), QOS.end());
        std::vector<mqtt::subscribe_options> sub_opts(TOPICS.size(), mqtt::subscribe_options());

        cli_.subscribe(topic_filters, qos_vals, sub_opts)->wait();

        std::cout << "[MQTT] Subscribed to topics (QoS1):";
        for (auto& t : TOPICS) std::cout << " " << t;
        std::cout << "\n";
    } catch (const mqtt::exception& e) {
        std::cerr << "[MQTT] Subscribe failed: " << e.what() << "\n";
    }
}

void MqttApp::connected(const std::string& cause) {
    std::cout << "[MQTT] Connected callback. Cause: " << cause << "\n";
    subscribe_topics();
}

void MqttApp::connection_lost(const std::string& cause) {
    std::cerr << "[MQTT] Connection lost: " << cause << "\n";
}

void MqttApp::message_arrived(mqtt::const_message_ptr msg) {
    try {
        const auto& topic = msg->get_topic();
        const auto& payload = msg->to_string();

        if (topic == "celima/data") {
            handle_celima_data(payload);
        } else if (topic == "celima/error") {
            std::cerr << "[celima/error] " << payload << "\n";
        } else if (topic == "celima/join") {
            std::cout << "[celima/join] " << payload << "\n";
        } else if (topic == "celima/ACK") {
            std::cout << "[celima/ACK] " << payload << "\n";
        } else {
            std::cout << "[MQTT] Message on " << topic << " (ignored)\n";
        }
    } catch (const std::exception& e) {
        std::cerr << "[MQTT] message_arrived error: " << e.what() << "\n";
    }
}

void MqttApp::delivery_complete(mqtt::delivery_token_ptr tok) {
    if (tok && tok->get_message_id() != 0)
        std::cout << "[MQTT] Delivery complete. MID=" << tok->get_message_id() << "\n";
}

void MqttApp::on_success(const mqtt::token& tok) {
    (void)tok;
}

void MqttApp::on_failure(const mqtt::token& tok) {
    std::cerr << "[MQTT] Action failed. Token: " << tok.get_message_id() << "\n";
}

void MqttApp::handle_celima_data(const std::string& payload) {
    std::string err;
    auto jopt = jsonu::parse(payload, err);
    if (!jopt) {
        std::cerr << "[celima/data] Invalid JSON: " << err << " | payload=" << payload << "\n";
        return;
    }
    auto& j = *jopt;

    int devTypeInt = j.value("deviceType", 0);
    auto dt = deviceTypeFromInt(devTypeInt);
    std::unique_ptr<IMessageProcessor> proc = dt ? createProcessor(*dt)
                                                 : createDefaultProcessor();
    auto pubs = proc->process(j, isa95_prefix_);
    for (auto& p : pubs) {
        publish_qos1(p.topic, p.payload);
    }
}

void MqttApp::publish_qos1(const std::string& topic, const std::string& payload) {
    auto msg = mqtt::make_message(topic, payload);
    msg->set_qos(1);
    try {
        cli_.publish(msg);
        // fire-and-forget; Paho retains the token internally with QoS1
        std::cout << "[PUB QoS1] " << topic << " <- " << payload << "\n";
    } catch (const mqtt::exception& e) {
        std::cerr << "[MQTT] Publish failed: " << e.what() << "\n";
    }
}
