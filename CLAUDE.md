# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A single-binary C++20 daemon (`iot-celima-mqtt`) that bridges a LoRaWAN network server to an edge
MQTT broker at the Celima ceramic-tile plant. It subscribes to `celima/*` topics, decodes the JSON
frames emitted by field devices (Arduino + LoRa nodes reading Mitsubishi PLC registers), accumulates
per-shift production counters in memory, and republishes ISA-95-shaped topics consumed downstream
(AWS pipeline).

Comments, logs, and docs are largely in Spanish; domain vocabulary is Spanish (`pisadas`, `paradas`,
`bancalinos`, `turno`, `quebrados`). Keep that convention when editing.

Servicio C++ que consume telemetría MQTT de las líneas de producción de Celima (planta Punta Hermosa), la normaliza a tópicos ISA-95 y la republica. Corre como iot-celima-mqtt.service en la PC industrial de planta, junto al broker Mosquitto y al boxer-patrol-edge-processor (Python) que consume lo que este servicio publica.

Cadena completa: Arduinos → LoRaWAN → gateway Milesight → MQTT 1883 → este servicio → boxer-patrol-edge-processor → InfluxDB (caché de reenvío) → API Gateway de AWS.

## Build & run

```bash
sudo apt-get install -y g++ make libpaho-mqttpp-dev nlohmann-json3-dev

make                      # release → bin/Release/iot-celima-mqtt
make debug                # → bin/Debug/iot-celima-mqtt (-O0 -g3 -DDEBUG)
make SAN=address debug     # sanitizers: address | ubsan | thread
make run-release          # runs against tcp://localhost:1883
make format               # clang-format -i inc/*.hpp src/*.cpp
make clean

./scripts/build_deb.sh 1.05.0   # .deb into ./_pkg (needs bin/Release built first)
```

```bash
make test          # compila tests/ + los .cpp de dominio y ejecuta la suite
./bin/tests -tc='replay*'          # un solo caso (doctest: -tc / --test-case)
./bin/tests -ltc                   # listar casos
make test GOLDEN_OUT=tests/data/celima_data_replay.golden   # regenerar el golden
```

The suite is doctest (vendored at [tests/doctest.h](tests/doctest.h), MIT, header-only — no runtime
dependency, not linked into the release binary). Its core is a deterministic replay:
[tests/data/celima_data_replay.jsonl](tests/data/celima_data_replay.jsonl) is fed through the
processors and every publication is compared byte-for-byte against a golden file. See
[tests/data/README.md](tests/data/README.md) — the fixture is synthetic, and the golden was generated
with pre-instrumentation code on purpose.

Beyond the suite, verification means running the binary against a broker and reading its stdout
(every accepted frame logs `[PUB QoS1] <topic> <- <payload>`, and state events log `[STATE] <event>
line=<n> proc=<name> ...`), or in production via `journalctl -u iot-celima-mqtt.service`.

## Configuration

Env vars (also positional argv: `broker`, `client_id`, `isa95_prefix`), defaults in
[packaging/defaults.env](packaging/defaults.env):

- `MQTT_BROKER` — `tcp://localhost:1883`
- `MQTT_CLIENT_ID` — `celima-integration`
- `ISA95_PREFIX` — `celima/punta_hermosa/planta/linea/` (**trailing slash matters**: processors build
  topics as `prefix + lineID + "/<machine>/<production|alarms>"`, yielding `.../linea1/...`)
- `SHIFT_MODE` — `2` (06–18 / 18–06) or `3` (three 8 h shifts, 06/14/22). Production runs `2`.

Deployed as a systemd unit ([packaging/iot-celima-mqtt.service](packaging/iot-celima-mqtt.service))
reading `/etc/default/iot-celima-mqtt`. The unit is hardened (`ProtectSystem=full`,
`ProtectHome=true`, `PrivateTmp=true`) — relevant if you ever add on-disk state.

## Architecture

Flow, all inside one process:

1. [src/main.cpp](src/main.cpp) — env/argv config, SIGINT/SIGTERM loop.
2. [src/MqttApp.cpp](src/MqttApp.cpp) — Paho C++ async client. Subscribes QoS 1 to `celima/data`,
   `celima/error`, `celima/join`, `celima/ACK`; only `celima/data` is processed. `clean_session=false`
   + `automatic_reconnect`. Publishes QoS 1 fire-and-forget.
3. Routing: `deviceType` (int) → `DeviceType` enum ([inc/DeviceTypes.hpp](inc/DeviceTypes.hpp)) →
   `createProcessor()` factory. Unknown types fall through to `DefaultProcessor`.
4. Before dispatch, `detect_global_shift_change()` compares the current shift against a global atomic;
   on change it calls `reset_all_processor_states()`, zeroing every processor's static state.
5. [src/MessageProcessor.cpp](src/MessageProcessor.cpp) (~2.1k lines) holds all eight processors and
   the shared counter helpers. This is where essentially all the work happens.

`deviceType` / `maquina_id` / topic segment, one processor each:

| id | DeviceType | topic segment |
|---|---|---|
| 1 | PH_1 | `prensa_hidraulica1` |
| 2 | PH_2 | `prensa_hidraulica2` |
| 3 | Entrada_secador | `entrada_secador` |
| 4 | Salida_secador | `salida_secador` |
| 5 | Esmalte | `esmalte` |
| 6 | Entrada_horno | `entrada_horno` |
| 7 | Salida_horno | `salida_horno` |
| 8 | Calidad | `calidad` |

Most processors emit two publications per frame: `.../alarms` and `.../production`. Calidad emits
only `.../production`.

### The processor pattern

Every processor is structurally the same; deviations are almost always deliberate and commented.

- A private `State`/`PH1State` struct per line, held in a `static std::unordered_map<int, State>`
  keyed by `lineID`, guarded by a `static std::mutex`. Statics are defined immediately after the
  class body, and `static void reset_states()` must be added to `reset_all_processor_states()`.
- Instances are created **per message** and thrown away — all continuity lives in the statics. Never
  put per-line state in a member field.
- `if (!st.initialized || st.shift != shiftNum)` re-seeds `last_*` from the current frame and zeroes
  `acc_*`; the `else` branch accumulates deltas. A shift change therefore both zeroes accumulators
  and discards the delta spanning the boundary.
- Duplicate-frame rejection (the network server republishes frames; interval ~180 s, retry ~30 s):
  processors that receive `timer1Hz` (a free-running 1 Hz counter) drop a frame whose `timer1Hz`
  equals the last accepted one. PH1/PH2 have no `timer1Hz` and instead compare all four counters
  within a 120 s window. Calidad dedups on `gatewayTime` + identical raw counters.
- Output is built with `nlohmann::json` and returned via `make_pub(topic, json)`.

### Counter arithmetic — read before touching

Field counters are `uint16_t` PLC registers sampled every ~3 minutes, so every published total is a
sum of deltas, and every hard-won fix here is about a specific field failure mode. The layers:

- `diff_counter(curr, prev, max_valid)` — unsigned 16-bit subtraction (rollover at 65536 is
  automatic); rejects `delta > max_valid` (default 5000) as corruption.
- `diff_counter_safe(curr, prev_ref, reject_count, max_valid, max_rejects, line, proc, field)` — the
  one to use. Only advances `prev_ref` on a *valid* delta, so one corrupt frame doesn't also poison
  the next, plus stale recovery: after 3 consecutive rejections it force-re-anchors `prev_ref`
  (otherwise a single bad value could freeze an accumulator for the rest of the shift). The last three
  arguments only label the `[STATE]` events; pass them at every new call site (`line` is the local
  `line` variable, `proc` the topic segment, `field` the counter name) or the log shows
  `line=-1 proc=? field=?`.
- `safe_delta_u16()` in [inc/MessageProcessor.hpp](inc/MessageProcessor.hpp) — used by
  CalidadProcessor for its monotonic accumulators.
- `spike_detected()` / `spike_ema_update()` — EMA-based corruption detection used by
  SalidaHornoProcessor: a frame is discarded only when ≥2 independent counters spike at once
  (`floor 300`, `10×` EMA), so a genuine burst on one counter is not thrown away.
- Products are derived from press strokes: `productos = pisadas × L<n>_PIEZAS_PISADA` (per-line
  constants in the header — L1/L2 ×3, L3 ×2, L4 ×4, L5 ×2).

Timestamps: use `device_timestamp(msg)` from [inc/TimeUtils.hpp](inc/TimeUtils.hpp), which converts
the gateway's `gatewayTime` (ISO-8601 with offset) to UTC and only falls back to server time when
absent/unparseable. Do not reintroduce bare `iso8601_utc_now()` in processors — it was deliberately
replaced everywhere (commit 38e85cc).

Shift boundaries come from `current_shift_localtime()` ([inc/Shift.hpp](inc/Shift.hpp)) and depend on
the host's local timezone (`TZ`), not UTC.

### Known limitation: accumulator state is RAM-only

All per-shift accumulators are lost on restart, which has cost real production totals in the field
(9 h 17 min across four lines on 2026-09-01). Before changing anything about accumulator lifetime,
`initialized`/re-seeding, or the delta bounds, read *Trampas conocidas* and *Diseño de referencia*
below — they carry the D1/D2/D3 framing and the ordering rule that persistence and a time-scaled
delta bound must ship in the same release.

### Adding a device type

1. Add the enum value plus both `switch` arms in [inc/DeviceTypes.hpp](inc/DeviceTypes.hpp).
2. Write the processor in [src/MessageProcessor.cpp](src/MessageProcessor.cpp) following the pattern
   above, with its statics defined below the class.
3. Register it in `createProcessor()` and its `reset_states()` in `reset_all_processor_states()`.
4. Emit `reseed` in its init branch and pass `line`/`proc`/`field` to every `diff_counter_safe` call.
5. Add frames for it to [tests/data/celima_data_replay.jsonl](tests/data/celima_data_replay.jsonl) and
   regenerate the golden — otherwise the replay covers everything except the new processor.

New `.cpp` files are picked up automatically (`SRC := $(wildcard src/*.cpp)`), and so are new
`tests/*.cpp`.

### Invariantes del dominio

No los cambies sin entender la consecuencia aguas abajo — el edge processor y AWS asumen todo esto.

- Todo publish es QoS 1. Sin excepción.
- Tópicos ISA-95: `${ISA95_PREFIX}<lineID>/<maquina>/production` y `.../alarms`. El prefijo por defecto
  es `celima/punta_hermosa/planta/linea/`.
- Turnos: producción corre en `SHIFT_MODE=2` — dos de 12 h, 06:00–18:00 y 18:00–06:00, hora local
  America/Lima. El binario admite además `SHIFT_MODE=3` (tres de 8 h, 06/14/22) y ese es su valor por
  defecto si la variable no está puesta, así que no supongas 12 h al leer el código.
  `current_shift_localtime()` usa la hora local del SO: si el host queda en UTC, las fronteras se
  corren cinco horas y la producción se atribuye al turno equivocado sin generar ningún error.
- Anchos de contador: hoy **todos** los campos se tratan como `uint16_t` completo, con rollover en
  65536. El enmascarado de 15 bits (`0x7FFF` / módulo 32768) existió mientras el firmware Arduino leía
  las palabras pseudo-I2C con un desplazamiento de 1 bit; corregido el firmware, se eliminó. Solo
  queda la nota histórica en [MessageProcessor.cpp:25-29](src/MessageProcessor.cpp#L25-L29). Si
  reaparece un campo con semántica de 15 bits, es un cambio de firmware, no una convención del código.
- `*_instantaneo` y `*_raw` son el contador crudo del PLC tal como llegó. No se derivan, no se
  corrigen, no se normalizan. Son la única fuente de verdad recuperable y la base de cualquier
  reconstrucción posterior.
- Calidad acumula en la aplicación, no en el dispositivo. `CalidadProcessor` v4 recibe contadores
  monotónicos (`boxesQ1/Q2/Q6`, `totalBroken`), calcula el delta con `safe_delta_u16` y publica sus
  propias sumas de turno en `extra_c1`/`extra_c2`/`comercial`/`quebrados`
  ([MessageProcessor.cpp:264-296](src/MessageProcessor.cpp#L264-L296)). Solo el formato antiguo
  `cajaCalidad` pasa valores del dispositivo tal cual. Consecuencia: **calidad también está expuesta a
  D1**, al contrario de lo que concluyó el documento de diseño con la evidencia del 1 de septiembre.
- Estado por procesador: `static std::mutex mtx_` + `static std::unordered_map<int, State> states_`
  con la línea como clave. Un procesador nuevo replica ese patrón.

### Trampas conocidas

Léelas antes de tocar [src/MessageProcessor.cpp](src/MessageProcessor.cpp).

- **La cota de delta existe, pero es plana** (defecto D2, en su forma actual). No hay `diff16` en el
  código: los siete procesadores de máquina usan el `diff_counter_safe` compartido, que rechaza
  `delta > max_valid` (5000 por defecto) y re-ancla `prev_ref` tras 3 rechazos consecutivos; calidad
  usa `safe_delta_u16` con sus propios techos (`MAXR_BOXES = 300`, `MAXR_BROKEN = 1200`). El problema
  no es que falte protección, es que **ninguna cota escala con el tiempo transcurrido**: a la tasa
  medida de L1 prensa (~1.288 pzs/h), `max_valid = 5000` se agota en unas **3,9 h** de producción, así
  que cualquier hueco mayor produce un delta legítimo que se rechaza y se cuenta como 0 — justo el
  caso que interesa recuperar. El techo duro real es el módulo de 65.536 (~51 h a esa tasa); pasado
  eso el delta es ambiguo y no hay aritmética que lo salve.
- **Un rechazo de cota cuesta ~9 minutos de producción.** Cuando la cota rechaza, `prev_ref` no
  avanza, así que los dos mensajes siguientes también se rechazan y solo el tercero fuerza el
  re-anclaje: a ~180 s de intervalo, tres tramas. No pasa tras un reinicio del proceso (ahí
  `initialized == false` re-siembra de inmediato), sino con el proceso vivo y los mensajes cortados:
  broker caído, dispositivo mudo, cambio de banco de registros.
- **El estado acumulado está solo en RAM** (defecto D1). Cada reinicio del proceso pone los
  acumuladores del turno en curso a cero, calidad incluida. Con turnos de 12 h, una caída de tarde
  borra casi un turno entero, y el pipeline hacia AWS no admite corregir datos ya enviados. Lo agrava
  el `Restart=on-failure` con `RestartSec=3` de la unidad systemd: un ciclo de caídas vacía los
  acumuladores cada tres segundos, en silencio.
- **D1 y D2 se corrigen en el mismo release.** Persistir el estado sin acotar el delta por tiempo
  desenmascara D2 y convierte un reporte corto en uno inflado, que es peor porque nadie lo cuestiona.
- **El buffering de stdout ya está corregido** (`std::cout << std::unitbuf;` como primera sentencia de
  `main()`). No lo quites ni metas salida antes de esa línea: sin ella, stdout bajo systemd es un pipe
  con buffer de bloque de 4 KB, los logs llegaban al journal con hasta 36 s de retraso y cada parada
  dura se llevaba las líneas que explicaban la caída. Medido: con `kill -9`, antes sobrevivían 0
  bytes; después, todo lo ya emitido.
- **Los caminos de estado dejan rastro `[STATE]`**, vía `celima::log::state_event()`
  ([inc/Logging.hpp](inc/Logging.hpp)): `reseed` (con `reason=first_message|shift_change`),
  `delta_rejected`, `reanchor` y `shift_change_global`. Si añades lógica de descarte o de re-siembra,
  emítela por ahí — en silencio no es reconstruible: tras el incidente del 1 de septiembre identificar
  la re-siembra exigió comparar `timer1Hz_turno` entre tópicos del journal. Dos reglas al hacerlo:
  nada de logging en la rama de acumulación normal (~36.000 mensajes/día, el journal ya crece
  43,5 MB/día), y un `reseed` por procesador y línea, no uno por campo.
- **`EsmalteProcessor` sí loguea en la rama normal** (`[ESM diag] ...`, una línea por mensaje). Es
  anterior a esta convención y contradice la regla de arriba; si toca revisar volumen de journal,
  empieza por ahí.

### Contexto operativo

- La PC industrial no reenciende sola tras un corte de energía: alguien debe pulsar un botón, lo que
  ocurre horas después. La planta produce durante esa ventana y nadie escucha. Hay huecos reales de 26
  y 31 h registrados. (Defecto D3.)
- Hay una migración a VM en curso que elimina esa espera, pero no resuelve la pérdida de estado ante
  un reinicio del host.
- Cualquier cambio que altere los valores publicados afecta datos de producción que se usan para
  control de planta y que no se pueden reexpresar una vez enviados a AWS. Ante la duda, prefiere
  añadir un campo nuevo a cambiar la semántica de uno existente.
- Para reconstruir un turno perdido, la única fuente es el journal (retención ~7 semanas, menos los
  4 KB que se pierden en cada parada dura): InfluxDB guarda solo un subconjunto de lo publicado, sin
  los contadores crudos. El procedimiento está en el documento de diseño.

### Diseño de referencia

[docs/design/presistencia-acumuladores.md](docs/design/presistencia-acumuladores.md) — ojo con el
nombre, el archivo en disco lleva la errata "presistencia". Contiene el análisis de los defectos
D1/D2/D3, la cota de plausibilidad escalada por tiempo, qué persistir y cómo restaurar, la evidencia
del incidente del 1 de septiembre de 2026, y las pruebas que debe cubrir el cambio.

Está alineado con el código actual (revisión del 2026-09-02: se corrigieron sus afirmaciones sobre
`diff16`, los contadores de 15 bits y la supuesta inmunidad de calidad a D1; los pasajes corregidos
lo señalan en el propio texto). Aun así es referencia, no un plan de ejecución: contiene preguntas
abiertas —entre ellas la retentividad de los contadores de los PLC, que no está medida— y decisiones
sin tomar. No lo implementes de corrido.

### Convenciones

- Comentarios y nombres de dominio en español (`turno`, `paradas`, `bancalino`); identificadores de
  C++ en inglés, como está hoy. Mantén la mezcla existente, no la unifiques.
- No ejecutes comandos contra la PC de planta ni contra el broker de producción desde este repo.
