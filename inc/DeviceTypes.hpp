#pragma once
#include <string>
#include <optional>
#include <unordered_map>

enum class DeviceType : int {
    PH_1 = 1,
    PH_2 = 2,
    Entrada_secador = 3,
    Salida_secador = 4,
    Esmalte = 5,
    Entrada_horno = 6,
    Salida_horno = 7,
    Calidad = 8
};

inline std::optional<DeviceType> deviceTypeFromInt(int v) {
    switch (v) {
        case 1: return DeviceType::PH_1;
        case 2: return DeviceType::PH_2;
        case 3: return DeviceType::Entrada_secador;
        case 4: return DeviceType::Salida_secador;
        case 5: return DeviceType::Esmalte;
        case 6: return DeviceType::Entrada_horno;
        case 7: return DeviceType::Salida_horno;
        case 8: return DeviceType::Calidad;
        default: return std::nullopt;
    }
}

inline const char* deviceTypeName(DeviceType dt) {
    switch (dt) {
        case DeviceType::PH_1: return "PH_1";
        case DeviceType::PH_2: return "PH_2";
        case DeviceType::Entrada_secador: return "Entrada_secador";
        case DeviceType::Salida_secador: return "Salida_secador";
        case DeviceType::Esmalte: return "Esmalte";
        case DeviceType::Entrada_horno: return "Entrada_horno";
        case DeviceType::Salida_horno: return "Salida_horno";
        case DeviceType::Calidad: return "Calidad";
    }
    return "Unknown";
}

// Segmento de estación en los tópicos ISA-95:
//   ${ISA95_PREFIX}<lineID>/<segmento>/{production,alarms,diagnostico}
// Única fuente del nombre: lo usan los procesadores de producción y el de
// diagnóstico, así que una estación no puede publicar con dos nombres.
// Cambiar un valor corta la serie aguas abajo (edge processor y AWS).
inline const char* stationSegment(DeviceType dt) {
    switch (dt) {
        case DeviceType::PH_1: return "prensa_hidraulica1";
        case DeviceType::PH_2: return "prensa_hidraulica2";
        case DeviceType::Entrada_secador: return "entrada_secador";
        case DeviceType::Salida_secador: return "salida_secador";
        case DeviceType::Esmalte: return "esmalte";
        case DeviceType::Entrada_horno: return "entrada_horno";
        case DeviceType::Salida_horno: return "salida_horno";
        case DeviceType::Calidad: return "calidad";
    }
    return "desconocido";
}
