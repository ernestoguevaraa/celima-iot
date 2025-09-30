#include "MqttApp.hpp"
#include <cstdlib>
#include <iostream>
#include <string>
#include <csignal>

static volatile std::sig_atomic_t g_stop = 0;
static void handle_sigint(int) { g_stop = 1; }

static std::string env_or(const char* key, const char* defv) {
    const char* v = std::getenv(key);
    return v ? std::string(v) : std::string(defv);
}

int main(int argc, char** argv) {
    std::signal(SIGINT, handle_sigint);
    std::signal(SIGTERM, handle_sigint);

    //Default values when no arguments 
    std::string broker = env_or("MQTT_BROKER", "tcp://localhost:1883");
    std::string client = env_or("MQTT_CLIENT_ID", "celima-integration");
    std::string isa95  = env_or("ISA95_PREFIX", "celima/punta_hermosa/planta/linea");

    if (argc > 1) broker = argv[1];
    if (argc > 2) client = argv[2];
    if (argc > 3) isa95  = argv[3];

    try {
        MqttApp app(broker, client, isa95);
        app.start();

        while (!g_stop) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        app.stop();
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
