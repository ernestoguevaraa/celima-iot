# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A single-binary C++20 daemon (`iot-celima-mqtt`) that bridges a LoRaWAN network server to an edge
MQTT broker at the Celima ceramic-tile plant. It subscribes to `celima/*` topics, decodes the JSON
frames emitted by field devices (Arduino + LoRa nodes reading Mitsubishi PLC registers), accumulates
per-shift production counters (persisted, so they survive a restart), and republishes ISA-95-shaped
topics consumed downstream (AWS pipeline).

Comments, logs, and docs are largely in Spanish; domain vocabulary is Spanish (`pisadas`, `paradas`,
`bancalinos`, `turno`, `quebrados`). Keep that convention when editing.

Servicio C++ que consume telemetría MQTT de las líneas de producción de Celima (planta Punta Hermosa), la normaliza a tópicos ISA-95 y la republica. Corre como iot-celima-mqtt.service en la PC industrial de planta, junto al broker Mosquitto y al boxer-patrol-edge-processor (Python) que consume lo que este servicio publica.

Cadena completa: Arduinos → LoRaWAN → gateway Milesight → MQTT 1883 → este servicio → boxer-patrol-edge-processor → InfluxDB (caché de reenvío) → API Gateway de AWS.

## Build & run

```bash
sudo apt-get install -y g++ make libpaho-mqttpp-dev nlohmann-json3-dev libsqlite3-dev

make                      # release → bin/Release/iot-celima-mqtt
make debug                # → bin/Debug/iot-celima-mqtt (-O0 -g3 -DDEBUG)
make SAN=address debug     # sanitizers: address | ubsan | thread
make run-release          # runs against tcp://localhost:1883
make format               # clang-format -i inc/*.hpp src/*.cpp
make clean

./scripts/build_deb.sh 1.06.0   # .deb into ./_pkg (needs bin/Release built first)
```

```bash
make test          # compila tests/ + los .cpp de dominio y ejecuta la suite
                   # (ejecuta desde la raíz del repo: el golden y
                   #  packaging/rates.json se abren por ruta relativa)
./bin/tests -tc='replay*'          # un solo caso (doctest: -tc / --test-case)
./bin/tests -ltc                   # listar casos
make test GOLDEN_OUT=tests/data/celima_data_replay.golden   # regenerar el golden
```

The suite is doctest (vendored at [tests/doctest.h](tests/doctest.h), MIT, header-only — no runtime
dependency, not linked into the release binary): 48 cases across
[test_replay.cpp](tests/test_replay.cpp) (golden), [test_state_events.cpp](tests/test_state_events.cpp),
[test_scaled_bound.cpp](tests/test_scaled_bound.cpp), [test_persistence.cpp](tests/test_persistence.cpp)
and [test_shifts.cpp](tests/test_shifts.cpp). Its core is a deterministic replay:
[tests/data/celima_data_replay.jsonl](tests/data/celima_data_replay.jsonl) is fed through the
processors and every publication is compared byte-for-byte against a golden file. See
[tests/data/README.md](tests/data/README.md) — the fixture is synthetic, and the golden was generated
with pre-instrumentation code on purpose. It is the guard that says the observability and
persistence work never touched the normal path: **if the golden changes, something is wrong**, so
investigate before regenerating it.

Two things make the suite deterministic regardless of when it runs: `testsup::pin_local_hour()` pins
`TZ` to a computed offset (shift boundaries are on the hour), and every fixture frame carries
`gatewayTime` so `device_timestamp()` never falls back to server time. Assert *outside*
`capture_streams()` — doctest reports failures on `std::cout`, so a live capture swallows them.

Beyond the suite, verification means running the binary against a broker and reading its stdout
(every accepted frame logs `[PUB QoS1] <topic> <- <payload>`, and state events log `[STATE] <event>
line=<n> proc=<name> ...`), or in production via `journalctl -u iot-celima-mqtt.service`.

The fixture is synthetic, but the numbers it models were measured on a 24 h capture of real
`celima/data` traffic (11.180 frames, 2026-09-02 → 09-03). What that capture settled, so nobody has
to re-derive it: `gatewayTime` was present and parseable in **every** frame, so the "a missing
`gatewayTime` stops the line counting" hazard is real but not currently occurring; retries are always
re-stamped by the gateway, land 1–26 s after the original, and are cleanly separable from a stalled
machine republishing identical counters at ≥179 s; `timer1Hz` tracks `gatewayTime` to ±1 s typically,
not exactly. Capturing it again is a `journalctl -o short-iso ... | grep -F '[celima/data]'` away —
keep the reception timestamp, it is what makes the retry analysis possible. Those logs are
gitignored: they are production data and weigh megabytes.

## Configuration

Env vars (also positional argv: `broker`, `client_id`, `isa95_prefix`), defaults in
[packaging/defaults.env](packaging/defaults.env):

- `MQTT_BROKER` — `tcp://localhost:1883`
- `MQTT_CLIENT_ID` — `celima-integration`
- `ISA95_PREFIX` — `celima/punta_hermosa/planta/linea/` (**trailing slash matters**: processors build
  topics as `prefix + lineID + "/<machine>/<production|alarms>"`, yielding `.../linea1/...`)
- `SHIFT_MODE` — `2` (06–18 / 18–06) or `3` (three 8 h shifts, 06/14/22). Production runs `2`.
- `CELIMA_RATES_CONFIG` — rates file for the time-scaled bound, default
  `/etc/iot-celima-mqtt/rates.json`. Missing or unparseable is not fatal: it logs `[CONFIG] rates
  file not usable (...)` once and falls back to a deliberately low `default_rate_per_h`.
- `CELIMA_STATE_DB` — SQLite state, default `/var/lib/iot-celima-mqtt/state.db`.
- `CELIMA_STATE_PERSISTENCE` — `0` disables load and save. This is the **field kill switch**: it
  reverts to pre-persistence behavior without recompiling or redeploying.
- `CELIMA_GAP_SHORT_S` — gap above which a restart is logged as `[STATE] gap`, default `900`.
- `CELIMA_INCOMPLETE_SHIFT_MARKER` — `1` publishes `turno_segundos_no_observados`. **Off by
  default**, pending the open question of whether the edge processor forwards unknown fields.
- `CELIMA_DEBUG_ESM` — `1` restores the per-message `[ESM diag]` trace on stderr.

Deployed as a systemd unit ([packaging/iot-celima-mqtt.service](packaging/iot-celima-mqtt.service))
reading `/etc/default/iot-celima-mqtt`. The unit is hardened (`ProtectSystem=full`,
`ProtectHome=true`, `PrivateTmp=true`), so on-disk state lives in the `StateDirectory=` systemd
creates — `/tmp` is not an option.

Two deployment artifacts, treated differently on purpose:

- **`rates.json` is configuration and ships in the package.** [packaging/rates.json](packaging/rates.json)
  is installed to `/usr/share/iot-celima-mqtt/` and copied to `/etc/iot-celima-mqtt/` by postinst
  *only if absent*, so a reinstall never overwrites what the plant edited. It is not a dpkg conffile
  — same convention as `defaults.env`. **Strict JSON, no comments**, so `jq` and CI linters can read
  it; the reasoning behind every number lives in
  [docs/design/cota-plausibilidad-y-tasas.md](docs/design/cota-plausibilidad-y-tasas.md), and the
  rates themselves are derived with [scripts/derive_rates.py](scripts/derive_rates.py) from 30 days
  of journal. A comment in the file is rejected with `[CONFIG] rates file not usable` and the
  conservative default — visible, and the safe side.
- **`state.db` is runtime state and must never be packaged or committed.** systemd's
  `StateDirectory=iot-celima-mqtt` creates the directory and the service creates the schema. If dpkg
  owned that file, an upgrade or a purge could take real production totals with it. `*.db` and its
  WAL siblings are gitignored.

The `postrm` draws the usual Debian line, and the difference matters here more than usual:
`remove` keeps both config files **and** `state.db`, so reinstalling resumes the shift where it left
off; `purge` deletes `/etc/iot-celima-mqtt/`, `/etc/default/iot-celima-mqtt` and
`/var/lib/iot-celima-mqtt/`. **Purging mid-shift loses that shift** — the totals exist nowhere else
(InfluxDB keeps only a subset of what was published, without the raw counters, and the AWS pipeline
does not accept restating what it already received).

## Architecture

Flow, all inside one process:

1. [src/main.cpp](src/main.cpp) — env/argv config, SIGINT/SIGTERM loop.
2. [src/MqttApp.cpp](src/MqttApp.cpp) — Paho C++ async client. Subscribes QoS 1 to `celima/data`,
   `celima/error`, `celima/join`, `celima/ACK`; only `celima/data` is processed. `clean_session=false`
   + `automatic_reconnect`. Publishes QoS 1 fire-and-forget.
3. Routing: `deviceType` (int) → `DeviceType` enum ([inc/DeviceTypes.hpp](inc/DeviceTypes.hpp)) →
   `createProcessor()` factory. Unknown types fall through to `DefaultProcessor`, whose two
   publications are placeholder scaffolding (`quantity: 0`, `alarms: 0`, server time, and a
   double-slash topic because the prefix already ends in `/`). A frame carrying `_error` — the
   gateway decoder failed to parse it — publishes **nothing**: those are the 1-byte keep-alive pings
   the Arduinos send so the gateway does not drop them, 10,7% of the traffic measured over 24 h, and
   each one used to emit two of those placeholder messages into InfluxDB. The first occurrence of
   each distinct `_error` string logs `[STATE] frame_ignored`; repeats are silent, so a *new* decoder
   error is still visible. The raw `[celima/data]` ingest line is unaffected — the pings still show
   up there, which is where you want them for forensics.
4. Before dispatch, `detect_global_shift_change()` compares the current shift against a global atomic;
   on change it calls `reset_all_processor_states()`, zeroing every processor's static state. The
   **first** message after boot deliberately does *not* count as a change — see the trap below.
5. [src/MessageProcessor.cpp](src/MessageProcessor.cpp) (~3.4k lines) holds all eight processors, the
   counter helpers and the persistence glue. This is where essentially all the work happens.
6. Supporting pieces: [inc/Logging.hpp](inc/Logging.hpp) (`[STATE]` events),
   [src/RateConfig.cpp](src/RateConfig.cpp) (per-line/machine max rates for the delta bound),
   [src/StateStore.cpp](src/StateStore.cpp) + [src/SqliteStateStore.cpp](src/SqliteStateStore.cpp)
   (shift state that survives a restart).

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
- **Order inside the lock matters and is load-bearing:** take the state → `restore_state_if_needed()`
  → build `CounterCtx` (rate, margin, `elapsed_s`) → seed-or-accumulate → `persist_state()`. The
  restore must come *first*: it brings back `last_accepted_epoch_s`, and without it `elapsed_s` is 0
  on the first message after a restart, which makes every delta implausible and silently drops a
  whole interval. That ordering bug is what [tests/test_persistence.cpp](tests/test_persistence.cpp)
  caught.
- Restoring is a *boot* event, not a shift event: the "already consulted the store" set is a
  process-scoped static, not a state field, because `reset_all_processor_states()` wipes state on
  every shift change and would otherwise re-read the DB and mislabel it as an across-restart change.
- **Publish interval, measured over 24 h of plant traffic: 187 s for most classes, 127 s for
  `entrada_horno`, 180 s for `calidad`.** The Arduino clock runs slightly long, hence 187 and not 180.
  Nothing in the code hardcodes it — the bound derives the gap from `gatewayTime` — but it is the
  number behind `CELIMA_GAP_SHORT_S`, the dedup window and the cost of a rejection.
- Duplicate-frame rejection. Duplicates are routine, not exceptional: the Arduino retries a frame up
  to 3 times (~30 s apart) until the LoRaWAN gateway ACKs it, and **those ACKs get lost on the way
  back**, so a delivered frame is often retransmitted anyway. The gateway re-stamps `gatewayTime` on
  the retry, so the copy does *not* arrive with the original timestamp. Three different mechanisms
  catch it: processors with `timer1Hz` (a free-running 1 Hz counter) drop a frame whose `timer1Hz`
  equals the last accepted one — the payload is byte-identical, so this always fires; PH1/PH2 have no
  `timer1Hz` and compare all four counters within a 120 s window; calidad compares `gatewayTime` +
  raw counters, so a re-stamped retry slips through, is accepted, and contributes a delta of 0 — it
  publishes a redundant message but never double-counts. A test drives all eight processors with a
  re-stamped retry and asserts no `*_turno` total moves.
- **The 120 s dedup window is calibrated, and the 24 h capture confirms both edges.** Real retries
  land 1–26 s after the original (p50 = 5 s), so the window has ~4,6x margin; and a *stalled* machine
  republishes identical counters at its normal interval (≥179 s), which must NOT be deduped because
  it is a legitimate frame. The window sits between the two on purpose — do not widen it past ~150 s.
- **`timer1Hz` is not an exact clock.** Its delta matches the `gatewayTime` delta in only 58% of real
  pairs; ±1 s is routine and ±6 s happens. The 1,5 margin on the `tiempo_s` bound absorbs it, and the
  fixture guard allows ±10 s for the same reason.
- Output is built with `nlohmann::json` and returned via `make_pub(topic, json)`.

### Counter arithmetic — read before touching

Field counters are `uint16_t` PLC registers sampled every ~3 minutes (~2 in `entrada_horno`), so
every published total is a sum of deltas, and every hard-won fix here is about a specific field failure mode. The layers:

- `CounterCtx` ([inc/MessageProcessor.hpp](inc/MessageProcessor.hpp)) carries everything a delta
  needs: `line`/`proc`/`field` (labels only, for `[STATE]`), `elapsed_s`, `rate_max_per_s`, `margin`,
  `max_valid`, `max_rejects`. Each processor builds one per message and calls `ctx.with("<field>")`
  per counter. A call site with no context logs `line=-1 proc=? field=?`, which is visible on purpose.
- `diff_counter_scaled(curr, prev, ctx)` — the bound. In order: `raw == 0` is always plausible;
  `elapsed_s <= 0` → `no_elapsed`; `rate_max_per_s <= 0` → `no_rate`; a bound reaching the 65536
  modulus → `ambiguous_module`; otherwise the ceiling is
  **`max(rate × elapsed × margin, max_valid)`**. That `max` is the important part: the scaled bound
  can only be *more* permissive than the old fixed 5000, never stricter, which is why a ~180 s
  interval behaves exactly as before and the replay golden is unchanged.
- `diff_counter_safe(curr, prev_ref, reject_count, ctx)` — wraps the above. Only advances `prev_ref`
  on a plausible delta, so one corrupt frame doesn't also poison the next, plus stale recovery: after
  3 consecutive rejections it force-re-anchors `prev_ref` (otherwise a single bad value could freeze
  an accumulator for the rest of the shift).
- `safe_delta_u16(prev, curr, ctx)` — the same bound for CalidadProcessor's monotonic accumulators,
  with `MAXR_BOXES`/`MAXR_BROKEN` as the floor. Calidad has no re-anchor path: it discards and logs.
- `diff_level_safe(curr, prev_ref, reject_count, ctx)` — the `Level` counterpart, returning the
  **signed** change. Same re-anchor and `[STATE]` events as `diff_counter_safe`; a level has no rate,
  so its ceiling is `ctx.max_valid` (500 for `numero_grades`) and a breach logs `reason=level_jump`.
  `entrada_horno` publishes the ups as `numero_grades_turno` — **the same value as before**, because
  the bound was already discarding the downs — plus two new fields with what used to be thrown away:
  `numero_grades_bajadas_turno` and `buffer_vacio_turno_s`. The latter is sampled per frame, so its
  resolution is the ~127 s publish interval, not a stopwatch.
- `spike_detected()` / `spike_ema_update()` — EMA-based corruption detection used by
  SalidaHornoProcessor: a frame is discarded only when ≥2 independent counters spike at once
  (`floor 300`, `10×` EMA), so a genuine burst on one counter is not thrown away.
- Products are derived from press strokes: `productos = pisadas × L<n>_PIEZAS_PISADA` (per-line
  constants in the header — L1/L2 ×3, L3 ×2, L4 ×4, L5 ×2).

Timestamps, two functions with deliberately different fallback behavior — do not unify them:

- `device_timestamp(msg)` → the **published** string. Falls back to server time so no record is left
  without a timestamp. Do not reintroduce bare `iso8601_utc_now()` in processors (commit 38e85cc).
- `device_epoch_s(msg)` → the numeric epoch used to measure gaps. Returns `nullopt` when
  `gatewayTime` is missing or unparseable and **never** falls back to server time: without a
  trustworthy device clock the gap isn't measurable, so the delta must count as implausible.

Both share one parser, `parse_gateway_time()`, so the two readings cannot drift apart.

Shift boundaries come from `current_shift_localtime()` ([inc/Shift.hpp](inc/Shift.hpp)) and depend on
the host's local timezone (`TZ`), not UTC.

### Shift state survives a restart

Per-shift accumulators used to be RAM-only, which cost 9 h 17 min of totals across four lines on
2026-09-01. They are now persisted per `(proc, line)` in SQLite
([inc/StateStore.hpp](inc/StateStore.hpp)) and restored on the first message per key after boot.
What to know before touching it:

- **Everything** is serialized, not just `acc_*`: raw tracking, reject counters, spike EMAs and dedup
  state, plus a `"v"` schema version. An unknown or newer `v` is ignored and the key re-seeds.
- Saves happen after every accepted message, inside the same mutex, with `synchronous = FULL` — a
  power cut gives no chance to close cleanly, so `SIGTERM`-only saving would miss the real scenario.
- Restore cases: no row → seed (old behavior); stored shift ≠ current → zero and seed, logging
  `reseed reason=shift_change_across_restart`; same shift → restore everything and let the scaled
  bound handle the first delta, logging `restored` or `gap` depending on `CELIMA_GAP_SHORT_S`.
- The store is never a new crash cause: unopenable DB, corrupt file, or a failing save all degrade to
  in-memory-only with one log line (repeats suppressed).
- `to_json`/`from_json` in each state struct are **generated** from the member list. If you add a
  field to a state struct, add it to both — a field missing from `to_json` silently resets on every
  restart, which is the exact bug this section exists to prevent.

### Adding a device type

1. Add the enum value plus both `switch` arms in [inc/DeviceTypes.hpp](inc/DeviceTypes.hpp).
2. Write the processor in [src/MessageProcessor.cpp](src/MessageProcessor.cpp) following the pattern
   above, with its statics defined below the class.
3. Register it in `createProcessor()` and its `reset_states()` in `reset_all_processor_states()`.
4. Emit `reseed` in its init branch and pass a `CounterCtx` to every delta call.
5. Give its state struct `to_json`/`from_json` covering every member, and call
   `restore_state_if_needed()` / `persist_state()` in the order described above.
6. Add frames for it to [tests/data/celima_data_replay.jsonl](tests/data/celima_data_replay.jsonl) and
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

- **D1 y D2 están corregidos, y se corrigieron juntos a propósito.** La cota escala con el tiempo
  (`diff_counter_scaled`) y el estado de turno se persiste. Persistir sin escalar la cota habría
  desenmascarado D2 y convertido un reporte corto en uno inflado, que es peor porque nadie lo
  cuestiona; escalar la cota sin persistir no habría servido de nada tras un reinicio. Si tocas uno
  de los dos, piensa en el otro.
- **Ante la duda, subcontar.** Es la regla que resuelve los empates de este código: un total bajo se
  nota y se investiga; uno inflado se reporta como producción real y nadie lo cuestiona nunca. Cuando
  la cota, el reloj o la configuración dejen ambiguo qué hacer, la salida correcta es re-sembrar y
  sumar 0 — nunca adivinar hacia arriba.
- **`gatewayTime` es ahora un campo crítico.** Sin él `device_epoch_s()` devuelve `nullopt`,
  `elapsed_s` queda en 0 y **todos** los deltas de ese mensaje se declaran implausibles: la línea deja
  de contar mientras el campo falte. Es la consecuencia deliberada de no caer a la hora del servidor,
  y por eso deja rastro (`delta_rejected ... reason=no_elapsed`) en lugar de fallar en silencio. Si
  aparece en el journal, es un problema del gateway, no del cálculo.
- **Un rechazo de cota cuesta tres tramas de producción.** Cuando la cota rechaza, `prev_ref` no
  avanza, así que los dos mensajes siguientes también se rechazan y solo el tercero fuerza el
  re-anclaje: ~9 min a 180 s de intervalo, ~6 min en `entrada_horno`, que publica cada 120 s. Con la cota escalada esto ya no se dispara en huecos
  legítimos; si lo ves en el journal, es corrupción real o un cambio de banco de registros.
- **Las tasas por línea y máquina siguen sin medirse.** `default_rate_per_h = 600` es un valor
  conservador puesto a mano, no derivado del percentil 99 de los deltas observados. Mientras siga así,
  la recuperación de huecos largos es más corta de lo que podría ser — lo que subcuenta, que es el
  lado seguro. Derivarlas de los `[STATE] delta_rejected` y los `*_raw` del journal es trabajo
  pendiente, y no requiere tocar código: es un archivo de configuración.
- **No todos los registros son contadores monótonos.** `counter_family_for(proc, field)` clasifica
  cada uno y `diff_counter_scaled` aplica la aritmética que le toca. Los de **tiempo** no usan la tasa
  configurada sino su máximo analítico (1 tick/s en `tiempo_s`, 10 en `tiempo_ds`); los de **nivel**
  (`Level`) se leen con signo y sin tasa ni hueco, porque suben y bajan. Hoy el único nivel es
  `entrada_horno`/`numero_grades`, el buffer de filas que esperan entrar al horno: restarlo sin signo
  convertía cada bajada en ~65.532 y generaba 836 rechazos al día, el 78% de los del servicio
  (defecto D5). La tabla está en [src/MessageProcessor.cpp](src/MessageProcessor.cpp) y lista **solo**
  tiempo y nivel; lo que no está cae en `Event` y usa la tasa medida, que es el lado conservador. **Marcar como tiempo un contador de
  evento agranda su cota 40x y es el error peligroso**, así que ante la duda no se lista. La
  clasificación no se deduce del sufijo: en las prensas `metrica_tiempo` son decisegundos y en salida
  horno `paradas_tempo` son segundos. Un test estructural recorre los sitios de llamada del propio
  código y se pone rojo si aparece un contador de tiempo sin clasificar.
- **La ventana recuperable depende de la familia, y para `tiempo_ds` es corta.** A 10 ticks/s el
  contador de 16 bits da la vuelta en 1,8 h, así que pasado ~1,2 h de hueco su delta es ambiguo por
  construcción y se re-siembra. No hay configuración que lo cambie. Los de segundos aguantan ~12 h y
  los de evento, según la tasa, entre 26 y 87 h.
- **Una tasa alta no es "más segura por si acaso": recorta el horizonte de recuperación**, porque la
  cota alcanza antes el módulo de 65.536. Tenlo presente antes de subir un número "por margen".
- **El buffering de stdout ya está corregido** (`std::cout << std::unitbuf;` como primera sentencia de
  `main()`), aunque **el efecto en planta aún no está medido**: en la captura de 24 h, hecha con el
  binario anterior, la latencia entre el sello del gateway y la línea del journal daba p50 = 8 s con
  ráfagas de hasta 7 líneas en el mismo segundo —130 veces más de lo que daría el azar—, consistente
  con un buffer de 4 KB a los ~500 B/s que crece el journal. Para separar eso de la latencia de radio
  y del network server hay que comparar contra la marca de recepción del `boxer-patrol-edge-processor`,
  no contra `gatewayTime`. No lo quites ni metas salida antes de esa línea: sin ella, stdout bajo systemd es un pipe
  con buffer de bloque de 4 KB, los logs llegaban al journal con hasta 36 s de retraso y cada parada
  dura se llevaba las líneas que explicaban la caída. Medido: con `kill -9`, antes sobrevivían 0
  bytes; después, todo lo ya emitido.
- **Los caminos de estado dejan rastro `[STATE]`**, vía `celima::log::state_event()`
  ([inc/Logging.hpp](inc/Logging.hpp)): `reseed` (`reason=first_message|shift_change|shift_change_across_restart`),
  `delta_rejected` (con `reason`, `max_plausible`, `elapsed_s` y `family`), `reanchor`, `restored`, `gap`,
  `shift_first_observed`, `shift_change_global`, `frame_ignored`, `stored_state_ignored` y
  `store_error`. Si añades lógica de descarte o de re-siembra,
  emítela por ahí — en silencio no es reconstruible: tras el incidente del 1 de septiembre identificar
  la re-siembra exigió comparar `timer1Hz_turno` entre tópicos del journal. Dos reglas al hacerlo:
  nada de logging en la rama de acumulación normal (~36.000 mensajes/día, el journal ya crece
  43,5 MB/día), y un `reseed` por procesador y línea, no uno por campo.
- **La traza `[ESM diag]` de esmalte está apagada** tras `CELIMA_DEBUG_ESM=1`. Escribía una línea
  por mensaje en `std::cerr`, en plena rama de acumulación. El test "la rama de acumulación normal no
  loguea" captura ahora los dos streams y recorre los 8 procesadores: si alguien vuelve a meter
  salida en el camino caliente, se pone rojo.
- **El primer mensaje tras arrancar no es un cambio de turno.** `detect_global_shift_change()`
  devuelve `false` la primera vez que ve un turno (y emite `shift_first_observed`). Antes devolvía
  `true`, y el `reset_all_processor_states()` que provoca borraba el estado recién restaurado sin
  fallar ninguna prueba que no lo buscara. El cambio de turno a través de un reinicio lo detecta cada
  clave por su cuenta.

### Contexto operativo

- La PC industrial no reenciende sola tras un corte de energía: alguien debe pulsar un botón, lo que
  ocurre horas después. La planta produce durante esa ventana y nadie escucha. Hay huecos reales de 26
  y 31 h registrados. (Defecto D3.)
- Hay una migración a VM en curso que elimina esa espera. La pérdida de estado ante un reinicio del
  host ya no depende de ella: el estado de turno está en disco.
- **El marcador de turno incompleto (`turno_segundos_no_observados`) no se publica por defecto.**
  Espera una decisión con el equipo del `boxer-patrol-edge-processor`: si descarta los campos
  desconocidos, el marcador se pierde en el siguiente salto y no sirve. La contabilidad sí se lleva
  siempre y se ve en los eventos `[STATE] gap`. Activarlo es `CELIMA_INCOMPLETE_SHIFT_MARKER=1`, y
  publicar un campo nuevo a AWS no se puede deshacer.
- Cualquier cambio que altere los valores publicados afecta datos de producción que se usan para
  control de planta y que no se pueden reexpresar una vez enviados a AWS. Ante la duda, prefiere
  añadir un campo nuevo a cambiar la semántica de uno existente.
- Para reconstruir un turno perdido, la única fuente es el journal (retención ~7 semanas, menos los
  4 KB que se pierden en cada parada dura): InfluxDB guarda solo un subconjunto de lo publicado, sin
  los contadores crudos. El procedimiento está en el documento de diseño.
- **Hay dispositivos apagados por fallos de join de LoRaWAN**, y por eso `rates.json` tiene más
  entradas que dispositivos emitiendo. En la captura de 24 h del 2026-09-02 reportaban **20 de las 26
  claves configuradas**; las seis silenciosas son L1/`entrada_secador`, L1/`prensa_hidraulica2`,
  L1/`salida_horno`, L2/`entrada_horno`, L2/`esmalte` y L3/`esmalte`. Se pidió reiniciar esos
  Arduinos, así que **no borres esas entradas**: la configuración sobra sin coste —una clave sin
  tráfico nunca se consulta— y hace falta cuando el dispositivo vuelva. Ojo con leer el documento de
  tasas sin esto en mente: da `prensa_hidraulica2` como en servicio con 8.275 muestras, y en esas
  24 h no envió ninguna.
- **L4/`esmalte` es un dispositivo enfermo, no una línea parada.** En la misma captura mandó 70 de las
  ~480 tramas esperadas y provocó 8 de los 33 huecos mayores de 600 s, el peor de 2,9 h. Es el que
  explica la anomalía del 1 de septiembre que el documento de persistencia dejó anotada. Sus tasas
  derivadas (L3 y L4 de esmalte) salen de muy pocas muestras.

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
