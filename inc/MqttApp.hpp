#pragma once
#include <string>
#include <vector>
#include <memory>
#include <atomic>
#include <mqtt/async_client.h>

/**
 * MqttApp: wraps Paho C++ async_client and routes messages.
 *
 * Los tres primeros argumentos salen de env o argv en main():
 *  - MQTT_BROKER     (por defecto tcp://localhost:1883)
 *  - MQTT_CLIENT_ID  (por defecto celima-integration)
 *  - ISA95_PREFIX    (por defecto celima/punta_hermosa/planta/linea/ — la barra
 *                     final importa: los tópicos se forman concatenando lineID)
 *
 * El resto de la configuración del servicio (SHIFT_MODE, CELIMA_*) se lee en
 * main.cpp y está documentada en packaging/defaults.env, que es lo que se
 * instala en planta. No la dupliques aquí: es lo que dejó estos valores por
 * defecto desfasados durante meses.
 */
class MqttApp : public virtual mqtt::callback, public virtual mqtt::iaction_listener {
public:
    MqttApp(std::string broker_uri, std::string client_id, std::string isa95_prefix, int shift_mode = 3);
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
    int shift_mode_;
    mqtt::async_client cli_;
    mqtt::connect_options connopts_;
    std::atomic<bool> running_{false};

    void subscribe_topics();
    void handle_celima_data(const std::string& payload);
    void publish_qos1(const std::string& topic, const std::string& payload);
};
