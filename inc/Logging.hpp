#pragma once
#include <iostream>
#include <string>

namespace celima::log {

// Formato: [STATE] <evento> line=<n> proc=<nombre> <detalle...>
// El detalle son pares clave=valor ya formateados por el llamador.
//
// Solo se emite en caminos excepcionales — nunca en la rama de acumulación
// normal, que son ~36.000 mensajes/día.
//
// Eventos vivos (la lista real sale de `grep -o 'state_event("[a-z_]*' src/`):
//   reseed                re-siembra del acumulador de turno, con su reason
//   delta_rejected        un delta fuera de cota, con reason y la cota aplicada
//   reanchor              re-ancla tras 3 rechazos seguidos del mismo campo
//   restored / gap        estado recuperado del disco, según el tamaño del hueco
//   paro_latched          un paro de línea con su duración, de la familia latcheada
//   frame_ignored         trama que el decoder no pudo interpretar
//   shift_first_observed  primer turno visto tras arrancar (NO es un cambio)
//   shift_change_global   cambio de turno de verdad
// Y desde el store, que no pasa por aquí: stored_state_ignored y store_error.
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
