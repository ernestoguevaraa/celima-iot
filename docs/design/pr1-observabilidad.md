# PR 1 — Trazas fiables y observabilidad del estado

**Repo:** `celima-iot` · **Base:** `main@0ac7a9c` · **Rama sugerida:** `fix/logging-observabilidad`
**Dependencias:** ninguna. Desplegable y revertible por sí solo.
**Diseño de referencia:** `docs/design/persistencia-acumuladores.md`

> Revisión 2 — reescrita contra el código real. La revisión 1 proponía centralizar `diff16`; ese
> refactor ya está hecho en el repo (`diff_counter` / `diff_counter_safe`) y se eliminó de esta spec.

## Objetivo

Que los logs del servicio sean fiables y que los descartes y re-anclajes de contadores dejen rastro,
**sin cambiar ningún valor publicado**. Es prerrequisito de PR 2: hoy, cuando el servicio muere se
pierden los últimos segundos de log, y `diff_counter_safe` re-ancla `prev_ref` en absoluto silencio.

## No-objetivos

- No se toca la lógica de acumulación ni el contenido de los payloads publicados.
- No se escala la cota por tiempo (eso es PR 2).
- No se persiste nada en disco.
- No se añaden dependencias de runtime.

---

## Cambio 1 — Flush de logs

**Archivo:** `src/main.cpp`, primera sentencia de `main()`, antes de cualquier otra salida.

```cpp
std::cout << std::unitbuf;   // flush tras cada inserción
```

### Por qué esto y no `setvbuf`

`std::cout` y `stdout` están sincronizados por defecto (`sync_with_stdio(true)`), así que hoy
`setvbuf(stdout, nullptr, _IOLBF, 0)` sí gobernaría a `std::cout`. Pero deja de hacerlo en cuanto
alguien llame a `sync_with_stdio(false)`, y `_IOLBF` sobre un *pipe* es comportamiento de glibc, no
garantía del estándar. `std::unitbuf` no depende de ninguna de las dos cosas.

Costo: un `write(2)` por operación de inserción. A 0,42 msg/s medidos es irrelevante. Verificable con
`strace -c -p $(pidof iot-celima-mqtt)` durante un minuto, antes y después.

`std::cerr` ya es unit-buffered por defecto. El `std::endl` del cambio de turno en `MqttApp.cpp`
puede quedarse como está.

---

## Cambio 2 — Helper de eventos de estado

**Archivo nuevo:** `inc/Logging.hpp`, header-only. Nada de framework.

```cpp
#pragma once
#include <iostream>
#include <string>

namespace celima::log {

// Formato: [STATE] <evento> line=<n> proc=<nombre> <detalle...>
// El detalle son pares clave=valor ya formateados por el llamador.
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
```

## Cambio 3 — Instrumentar los caminos silenciosos

Los sitios que hoy no dejan rastro, en orden de importancia:

| Evento | Dónde | Detalle mínimo |
|---|---|---|
| `reseed` | rama `!st.initialized` | `reason=first_message shift=<n>` |
| `reseed` | rama `st.shift != shiftNum` | `reason=shift_change shift_prev=<n> shift_new=<n>` |
| `delta_rejected` | `diff_counter_safe`, rama `raw_delta > max_valid` | `field=<nombre> prev=<n> curr=<n> raw_delta=<n> max_valid=<n> reject_count=<n>` |
| `reanchor` | `diff_counter_safe`, `reject_count >= max_rejects` | `field=<nombre> prev=<n> curr=<n>` |
| `shift_change_global` | `detect_global_shift_change()` | `shift_prev=<n> shift_new=<n>` |

`diff_counter_safe` y `diff_counter` son `static` libres, sin contexto de línea ni de campo. Para
poder etiquetar los eventos hay que pasarles ese contexto. La forma menos invasiva es añadir dos
parámetros al final, con valores por defecto, de modo que las llamadas existentes sigan compilando:

```cpp
static uint16_t diff_counter_safe(uint16_t curr, uint16_t &prev_ref,
                                  uint8_t &reject_count,
                                  uint16_t max_valid = 5000,
                                  uint8_t max_rejects = 3,
                                  int line = -1,
                                  const char* field = "?");
```

Luego se van rellenando `line` y `field` sitio por sitio. Las llamadas sin rellenar loguean
`line=-1 field=?`, que es visible y por tanto se corrige solo con el tiempo. Si prefieres no tocar la
firma, la alternativa es devolver un pequeño struct con el motivo y que loguee el llamador; es más
limpio pero toca los siete procesadores de golpe.

**Reglas:**

- `reseed` se emite **una vez por procesador y línea**, no una por campo.
- Nada de logging dentro de la rama de acumulación normal: son ~36.000 mensajes/día y el journal ya
  crece 43,5 MB/día.
- No cambies el formato de `[PUB QoS1]`: hay análisis forense y extracciones que dependen de él.

---

## Cambio 4 — Arnés de pruebas (decisión previa)

**El repo no tiene ni una prueba ni framework.** Este PR necesita al menos una, y esa decisión no
debería tomarse a mitad de la implementación.

Recomendación: **doctest** — un header, sin build system nuevo, y el `Makefile` ya recoge
`src/*.cpp` con `wildcard`. Alternativa razonable: Catch2 v3, más completo pero exige enlazar.

Añadir al `Makefile` un target aislado que no contamine el binario de release:

```make
TEST_SRC := $(wildcard tests/*.cpp)
test: $(TEST_SRC)
	$(CXX) $(CXXSTD) $(WARN) $(INC) -Itests -O0 -g $^ src/MessageProcessor.cpp src/JsonUtils.cpp src/DeviceTypes.cpp -o bin/tests && ./bin/tests
```

Ajusta la lista de fuentes: `main.cpp` no puede entrar (tiene su propio `main`).

Si prefieres mantener PR 1 mínimo, saca el cambio 4 y el test de replay a un PR 0 dedicado. Lo que no
recomiendo es entrar a PR 2 sin arnés: la lógica de restauración tiene demasiados casos para
validarla a ojo contra un broker.

## Cambio 5 — Test de replay determinista

Primera prueba del repo, y la red de seguridad de todo lo que viene después.

Alimentar los procesadores con una secuencia grabada de payloads `celima/data` y comparar el vector
de `Publication` contra un fichero golden. La secuencia se extrae del journal de planta:

```bash
journalctl -u iot-celima-mqtt.service -o cat --no-pager \
  | grep -oP '^\[celima/data\] \K\{.*\}' > tests/data/celima_data_replay.jsonl
```

Graba al menos una hora de tráfico real: cubre las cuatro líneas y los ocho `deviceType`.

Dos detalles que rompen el determinismo si no los controlas:

- **La hora local.** `current_shift_localtime()` lee el reloj del sistema. El test debe fijar `TZ` y
  el turno, o inyectarlo, o los resultados dependerán de cuándo se ejecute.
- **`device_timestamp()`** cae a la hora del servidor cuando `gatewayTime` falta o no parsea. Elige
  una muestra donde `gatewayTime` esté siempre presente, o normaliza ese campo en el golden.

---

## Criterios de aceptación

1. **Payloads idénticos.** Misma secuencia de `celima/data` → publicaciones byte a byte iguales antes
   y después del PR. Es el propósito del test de replay.
2. **Latencia de log < 1 s.** Comparar la marca de journald de un `[PUB QoS1] .../production` con la
   del `Mensaje MQTT recibido` correspondiente del `boxer-patrol-edge-processor`. Hoy esa diferencia
   llega a 36 s.
3. **Supervivencia a parada dura.** Tras `kill -9`, la última línea `[PUB QoS1]` emitida está en el
   journal.
4. **Re-siembras visibles.** Un cambio de turno produce exactamente un `[STATE] reseed
   reason=shift_change` por procesador y línea activos.
5. **Re-anclajes visibles.** Forzar tres rechazos consecutivos en un test produce un
   `[STATE] delta_rejected` por cada uno y un `[STATE] reanchor` al tercero.
6. Sin dependencias de runtime nuevas. Sin cambios en `.service` ni en `defaults.env`.

## Riesgos

- **Volumen de log.** Si `delta_rejected` resulta ser frecuente en producción, el journal crece. Es
  información que hoy no tienes, así que mídelo la primera semana: `journalctl -u
  iot-celima-mqtt.service --since today | grep -c delta_rejected`. Si es ruidoso, ese dato ya es un
  hallazgo por sí mismo.
- **Cambiar la firma de `diff_counter_safe`** toca los siete procesadores de máquina. El test de
  replay es lo que protege ese cambio; escríbelo primero.
