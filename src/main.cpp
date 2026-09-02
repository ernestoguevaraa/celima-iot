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
    // Flush tras cada inserción. Bajo systemd stdout es un pipe, así que libc
    // usa buffer de bloque de 4 KB: los logs llegaban al journal en ráfagas con
    // hasta 36 s de retraso y se perdían hasta 4 KB en cada parada dura.
    // std::unitbuf no depende de sync_with_stdio ni del _IOLBF de glibc.
    std::cout << std::unitbuf;

    std::signal(SIGINT, handle_sigint);
    std::signal(SIGTERM, handle_sigint);

    //Default values when no arguments
    std::string broker = env_or("MQTT_BROKER", "tcp://localhost:1883");
    std::string client = env_or("MQTT_CLIENT_ID", "celima-integration");
    std::string isa95  = env_or("ISA95_PREFIX", "celima/punta_hermosa/planta/linea/");

    int shift_mode = 3;
    {
        std::string sm = env_or("SHIFT_MODE", "3");
        if (sm == "2") shift_mode = 2;
        else if (sm != "3")
            std::cerr << "[WARN] SHIFT_MODE inválido '" << sm << "', usando 3.\n";
    }
    std::cout << "[CONFIG] Modo de turnos: " << shift_mode << "\n";

    if (argc > 1) broker = argv[1];
    if (argc > 2) client = argv[2];
    if (argc > 3) isa95  = argv[3];

    try {
        MqttApp app(broker, client, isa95, shift_mode);
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
