#pragma once
#include <iostream>
#include <string>

namespace celima::log {

// Formato: [STATE] <evento> line=<n> proc=<nombre> <detalle...>
// El detalle son pares clave=valor ya formateados por el llamador.
//
// Eventos emitidos hoy: reseed, delta_rejected, reanchor, shift_change_global.
// Solo se emite en caminos excepcionales — nunca en la rama de acumulación
// normal, que son ~36.000 mensajes/día.
inline void state_event(const char* evento,
                        int line,
                        const char* proc,
                        const std::string& detalle)
{
    std::cout << "[STATE] " << evento
              << " line=" << line
              << " proc=" << proc
              << ' ' << detalle << '\n';
}

} // namespace celima::log
