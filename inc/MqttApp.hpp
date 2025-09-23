#pragma once
#include <string>
#include <vector>
#include <memory>
#include <atomic>
#include <mqtt/async_client.h>

/**
 * MqttApp: wraps Paho C++ async_client and routes messages.
 *
 * Env/config (or argv) you can pass to main():
 *  - MQTT_BROKER (e.g. tcp://localhost:1883)
 *  - MQTT_CLIENT_ID (default: celima-integration-<pid>)
 *  - ISA95_PREFIX (default: enterprise/site/area/line1)
 */
class MqttApp : public virtual mqtt::callback, public virtual mqtt::iaction_listener {
public:
    MqttApp(std::string broker_uri, std::string client_id, std::string isa95_prefix);
    ~MqttApp();

    void start();
    void stop();

    // mqtt::callback
    void connected(const std::string& cause) override;
    void connection_lost(const std::string& cause) override;
    void message_arrived(mqtt::const_message_ptr msg) override;
    void delivery_complete(mqtt::delivery_token_ptr tok) override;

    // mqtt::iaction_listener
    void on_success(const mqtt::token& tok) override;
    void on_failure(const mqtt::token& tok) override;

private:
    std::string broker_;
    std::string client_id_;
    std::string isa95_prefix_;
    mqtt::async_client cli_;
    mqtt::connect_options connopts_;
    std::atomic<bool> running_{false};

    void subscribe_topics();
    void handle_celima_data(const std::string& payload);
    void publish_qos1(const std::string& topic, const std::string& payload);
};
