# PR 2 — D1 + D2: persistencia de estado y cota escalada por tiempo

**Repo:** `celima-iot` · **Base:** `main@8800369` (PR 1 ya mergeado)
**Rama sugerida:** `feat/persistencia-acumuladores`
**Diseño de referencia:** `docs/design/persistencia-acumuladores.md`

> Revisión 3 — contra el árbol real tras PR 1. Cambios respecto a la revisión 2: contexto por struct
> en lugar de parámetros posicionales, estrategia explícita para el golden, y reutilización del arnés
> que ya existe en `tests/`.

## Objetivo

Que el total de turno sobreviva a un reinicio del proceso o del host (**D1**), y que las cotas
existentes dejen de ser techos fijos para escalar con el hueco transcurrido (**D2**). Los dos en el
mismo release: persistir sin escalar la cota cambia un reporte corto por uno inflado, que es peor
porque nadie lo cuestiona.

## Principio que resuelve los empates

**Ante la duda, subcontar.** Un total bajo se nota y se investiga; uno inflado se reporta como
producción real y nadie lo cuestiona nunca. Cuando la cota, el reloj o la configuración dejen
ambiguo qué hacer, la salida correcta es re-sembrar y sumar 0.

---

## Punto de partida real

No hay que introducir protección: hay que hacerla dependiente del tiempo. Existen dos caminos y
**los dos hay que tocarlos**:

| Camino | Quién lo usa | Techo actual |
|---|---|---|
| `diff_counter_safe(curr, prev_ref, reject_count, max_valid, max_rejects, line, proc, field)` | los siete procesadores de máquina | `max_valid = 5000` fijo |
| `safe_delta_u16(prev, curr, max_reasonable)` | `CalidadProcessor` | `MAXR_BOXES = 300`, `MAXR_BROKEN = 1200` |

Con la tasa medida de L1 prensa (~1.288 pzs/h), `max_valid = 5000` se agota en **~3,9 h de
producción**: cualquier hueco mayor descarta la recuperación legítima. El módulo real es 65.536
(los contadores ya no llevan máscara de 15 bits), así que el horizonte de ambigüedad son ~51 h — los
huecos reales de agosto, de 26 h y 31 h, caben dentro.

`spike_detected()` / `spike_ema_update()` de `SalidaHornoProcessor` **no se tocan**: detectan
corrupción por correlación entre contadores, un problema distinto al del hueco temporal.

---

## Prerrequisitos

### Que no son código

1. **Medir la retentividad de los contadores del PLC** (procedimiento en el documento de diseño).
   Determina qué comportamiento es correcto en las pruebas, no qué código se escribe.
2. **Derivar `tasa_max` por línea y máquina** del percentil 99 de los deltas observados. Con PR 1
   desplegado, los `[STATE] delta_rejected` y los `*_raw` del journal dan la muestra. Mídelos.

Sin (2) el PR puede desplegarse con el valor conservador por defecto, que subcuenta igual que hoy.
Nunca con uno optimista.

### Arreglo previo, dentro de este PR (commit 0)

El test `"la rama de acumulación normal no loguea"` de `tests/test_state_events.cpp` **pasa con un
falso negativo**: solo captura `stdout`, y `src/MessageProcessor.cpp:1429` imprime `[ESM diag]` en
`std::cerr` en cada mensaje aceptado de esmalte.

1. Extiende el helper de captura de `tests/support.hpp` para capturar también `stderr`.
2. Comprueba que el test se pone **rojo**.
3. Elimina la línea `[ESM diag]` (o condiciónala a una variable de entorno de depuración).
4. Test en verde.

Hazlo primero: PR 2 añade lógica de descarte y necesita que ese test detecte de verdad el ruido en el
camino caliente.

## Decisión abierta que bloquea el apartado F

**¿El `boxer-patrol-edge-processor` propaga campos desconocidos hacia InfluxDB y AWS, o los
descarta?** El marcador de turno incompleto es inútil si se pierde en el siguiente salto. Es una
dependencia con otro equipo. Si los descarta, el apartado F sale del alcance.

---

## A. Contexto por struct

`diff_counter_safe` ya tiene ocho parámetros y los sitios de llamada quedaron así:

```cpp
diff_counter_safe(pisadas, st.last_pisadas, st.rc_pisadas, 5000, 3, line, "prensa_hidraulica1", "pisadas");
```

Añadir `elapsed_s` y `rate_max_per_s` posicionalmente lo vuelve ilegible. Sustituye por:

```cpp
struct CounterCtx {
    int         line           = -1;
    const char* proc           = "?";
    const char* field          = "?";
    double      elapsed_s      = 0.0;   // desde el último mensaje ACEPTADO de esta clave
    double      rate_max_per_s = 0.0;   // de RateConfig
    uint16_t    max_valid      = 5000;  // se conserva como límite absoluto superior
    uint8_t     max_rejects    = 3;
};

static uint16_t diff_counter_safe(uint16_t curr, uint16_t &prev_ref,
                                  uint8_t &reject_count,
                                  const CounterCtx &ctx);
```

Un argumento en lugar de siete, y desaparecen los `5000, 3` mágicos repetidos en dieciséis sitios.
Los valores por defecto del struct preservan el fallback `line=-1 proc=? field=?` que PR 1 introdujo
para delatar llamadas sin contexto.

Cada procesador construye su `CounterCtx` una vez por mensaje y lo reutiliza cambiando solo `field`.

## B. Cota escalada por tiempo

**Archivo:** `src/MessageProcessor.cpp`, junto a los helpers existentes.

```cpp
struct DeltaResult {
    uint16_t value     = 0;
    bool     plausible = false;   // false → re-sembrar y sumar 0
};

DeltaResult diff_counter_scaled(uint16_t curr, uint16_t prev, const CounterCtx &ctx);
```

Reglas, en orden:

1. `ctx.elapsed_s <= 0` → `plausible = false`. Cubre el reloj hacia atrás y los timestamps
   duplicados. No intentes "arreglar" el tiempo negativo.
2. `ctx.rate_max_per_s <= 0` → `plausible = false`. Configuración ausente o absurda subcuenta.
3. `max_plausible = ctx.rate_max_per_s * ctx.elapsed_s * margin`.
4. **Límite duro:** si `max_plausible >= 65536`, el delta es ambiguo por construcción — el contador
   pudo dar más de una vuelta. `plausible = false` sin más aritmética.
5. `raw = static_cast<uint16_t>(curr - prev)` — misma resta sin máscara que hoy.
6. `plausible = (raw <= max_plausible)`; `value = raw`.

`diff_counter_safe` pasa a envolver `diff_counter_scaled` en lugar de `diff_counter`, conservando su
contrato actual: solo avanza `prev_ref` con delta plausible, re-ancla tras `max_rejects` rechazos
consecutivos, y emite los eventos `[STATE] delta_rejected` / `reanchor` de PR 1. Añade
`max_plausible=` y `elapsed_s=` al detalle de `delta_rejected`.

**Con la cota escalada, el camino de re-anclaje deja de dispararse en huecos legítimos**, y con él
desaparecen los ~9 minutos de producción que hoy se pierden en cada evento.

`safe_delta_u16` recibe el mismo tratamiento para calidad. No basta con arreglar uno de los dos.

## C. `elapsed_s`: hace falta un timestamp numérico

`device_timestamp(msg)` de `inc/TimeUtils.hpp` devuelve un **string** ISO-8601. Para calcular
`elapsed_s` necesitas época numérica. Añade en `TimeUtils.hpp`, reutilizando el parseo que ya existe
en `gateway_time_to_iso8601_utc`:

```cpp
inline std::optional<int64_t> device_epoch_s(const nlohmann::json& msg);
```

Devuelve `std::nullopt` cuando `gatewayTime` falta o no parsea — y en ese caso **no caigas a la hora
del servidor** para este cálculo: sin timestamp de dispositivo fiable, `elapsed_s` no es medible y el
delta debe tratarse como implausible. Es distinto de lo que hace `device_timestamp()` para el campo
publicado, que sí tiene fallback; no unifiques los dos comportamientos.

Guarda `last_accepted_epoch_s` en cada `State`. Varios procesadores ya tienen un
`last_accepted_time` para deduplicación: **no lo reutilices**, tiene otra semántica y otra unidad;
añade un campo aparte.

## D. Configuración de tasas

**Archivos nuevos:** `inc/RateConfig.hpp`, `src/RateConfig.cpp`.

Carga en el arranque desde `$CELIMA_RATES_CONFIG`, por defecto `/etc/iot-celima-mqtt/rates.json`:

```json
{
  "default_rate_per_h": 600,
  "margin": 1.5,
  "lines": {
    "1": { "prensa_hidraulica1": 1500, "salida_secador": 1400 },
    "2": { "salida_secador": 1400, "salida_horno": 900 }
  }
}
```

Resolución: `lines[<lineID>][<maquina>]` → `default_rate_per_h` si falta.

Si el archivo no existe o no parsea, **no abortes el servicio**: registra una vez
`[CONFIG] rates file not usable (<motivo>), using default_rate_per_h=<n>` y sigue. El valor por
defecto debe ser deliberadamente **bajo**: tasa baja → cota baja → más descartes → total subcontado,
que es el lado seguro del error.

Añade `CELIMA_RATES_CONFIG` a `packaging/defaults.env`.

## E. Persistencia del estado

**Archivos nuevos:** `inc/StateStore.hpp`, `src/SqliteStateStore.cpp`.

```cpp
class IStateStore {
public:
    virtual ~IStateStore() = default;
    virtual bool load(const std::string& proc, int line, nlohmann::json& out) = 0;
    virtual bool save(const std::string& proc, int line,
                      int shift, int64_t updated_at, const nlohmann::json& st) = 0;
};
```

Interfaz para que los tests usen un doble en memoria y la implementación sea sustituible.

**SQLite.** Añade `libsqlite3-dev` a las build-deps, `-lsqlite3` a `LDFLAGS` y al target `test` del
`Makefile`.

```sql
CREATE TABLE IF NOT EXISTS processor_state (
  proc        TEXT    NOT NULL,
  line        INTEGER NOT NULL,
  shift       INTEGER NOT NULL,
  updated_at  INTEGER NOT NULL,   -- epoch en segundos, UTC
  state_json  TEXT    NOT NULL,
  PRIMARY KEY (proc, line)
);
```

`PRAGMA journal_mode = WAL;` y `PRAGMA synchronous = FULL;` — a 0,42 msg/s el costo es irrelevante y
el escenario que importa es el corte sin cierre limpio.

`shift` y `updated_at` como columnas propias, no dentro del JSON: la restauración los necesita antes
de deserializar el resto.

**Clave `proc`:** usa exactamente los nombres que PR 1 ya emite en los eventos `[STATE]` —
`prensa_hidraulica1`, `prensa_hidraulica2`, `entrada_secador`, `salida_secador`, `esmalte`,
`entrada_horno`, `salida_horno`, `calidad`. Así un log y una fila de la base se cruzan sin traducir.

**Qué serializar**, con `to_json` / `from_json` de nlohmann por cada tipo de estado:

- todos los `acc_*`
- todos los `last_*` / `prev_ref`
- los `rc_*` (contadores de rechazo) y las EMA de spike, para no reiniciar la heurística en cada
  arranque
- `last_accepted_epoch_s` (apartado C)
- el estado de deduplicación (`last_gateway_time`, `last_accepted_time`, `baseline_set`)
- un campo `"v"` de versión de esquema: si es desconocida o mayor que la soportada, **ignora el
  estado guardado y re-siembra**. Nunca interpretes un formato que no conoces.

Son ocho procesadores con diez tipos de estado (`PH1State`, `PH2State`, cinco `State` anónimos,
`ShiftAcc` + `RawTrack` en calidad). Es mecánico, pero es el grueso del PR.

**Incluye a `CalidadProcessor`.** Acumula en la aplicación desde la v4 y está tan expuesto a D1 como
los demás: el 1 de septiembre perdió 16.483 piezas quebradas de turno entre las cuatro líneas.

**Ruta:** `$CELIMA_STATE_DB`, por defecto `/var/lib/iot-celima-mqtt/state.db`. En
`packaging/iot-celima-mqtt.service` añade `StateDirectory=iot-celima-mqtt`; systemd crea el
directorio con permisos correctos incluso cuando el servicio deje de correr como root. La unidad ya
tiene `ProtectSystem=full`, `ProtectHome=true` y `PrivateTmp=true`, así que `/tmp` no es opción.

**Cadencia:** guardar tras procesar cada mensaje, dentro del mismo `mtx_` que protege el estado. Si
mides que el `fsync` añade latencia a la publicación, mueve el `save` a después del `publish_qos1` —
pero nunca lo dejes solo en `SIGTERM`: un corte de energía no da oportunidad de cerrar limpio, y ese
es exactamente el escenario.

## F. Lógica de restauración

Primera vez que llega un mensaje para una clave `(proc, line)` tras arrancar:

| Caso | Condición | Acción |
|---|---|---|
| 1 | Sin estado guardado | Sembrar, `acc_* = 0`. Comportamiento actual |
| 2 | `stored.shift != shiftNum` | `acc_* = 0`, sembrar. Log `reseed reason=shift_change_across_restart` |
| 3 | Mismo turno, `elapsed <= gap_short` | Restaurar todo, continuar normal |
| 4 | Mismo turno, `elapsed > gap_short` | Restaurar los `acc_*`, aplicar `diff_counter_scaled` al primer delta. Log `gap elapsed_s=…` |

`gap_short`: configurable. El intervalo normal entre tramas es ~180 s con reintentos a ~30 s, así que
5× ≈ 900 s es un punto de partida defendible. Anótalo como valor a revisar con datos.

### La trampa que más fácil rompe este PR

`detect_global_shift_change()` compara contra `g_last_global_shift`, un atómico inicializado a `-1`.
El **primer mensaje tras arrancar** siempre cuenta como cambio de turno y hoy dispara
`reset_all_processor_states()`, que llama al `reset_states()` de los ocho procesadores.

Si eso queda intacto, **borrará el estado que acabas de restaurar** y el PR no servirá para nada,
sin fallar ningún test que no lo busque explícitamente. Dos salidas: inicializa `g_last_global_shift`
desde el turno persistido al arrancar, o haz que el primer disparo no invoque el reset. La primera es
más limpia porque deja el atómico coherente con el estado en disco.

Test obligatorio: *el primer mensaje tras arrancar con estado restaurado no pone los acumuladores a
cero*.

## G. Marcador de turno incompleto

**Sujeto a la decisión abierta.** Acumula por clave los segundos de turno no observados (la suma de
los `elapsed` de los casos 2 y 4 dentro del turno en curso) y publícalos como **campo nuevo** en el
payload de `production`:

```
"turno_segundos_no_observados": 33420
```

**No cambies la semántica de ningún campo existente.** Añadir es seguro; redefinir no, porque los
datos ya enviados a AWS no se pueden reexpresar.

## H. Disciplina ante fallos

La persistencia no puede convertirse en una causa de caída nueva. Con `Restart=on-failure` y
`RestartSec=3`, un fallo que tire el proceso lo reinicia cada tres segundos y vacía los acumuladores
en bucle — exactamente lo que este PR intenta evitar.

- El store no abre → registrar una vez, continuar **solo en memoria**, comportamiento de hoy.
- Un `save` falla → registrar con supresión de repetidos, continuar. Nunca reintentar en bucle ni
  bloquear la ingesta.
- Base corrupta o ilegible → tratarla como inexistente. No la repares en caliente.
- **Interruptor de emergencia:** `CELIMA_STATE_PERSISTENCE=0` desactiva carga y guardado. Permite
  revertir en planta sin recompilar ni redesplegar un binario.

---

## Pruebas

Sobre el arnés que ya existe: **reutiliza `tests/support.hpp`** (captura de logs, construcción de
mensajes) en lugar de inventar helpers nuevos.

### El golden existente no debe cambiar

`tests/data/celima_data_replay.jsonl` es una hora de tráfico continuo, sin huecos largos. Con la cota
escalada, ese fixture **debe seguir produciendo el golden byte a byte idéntico**. Si cambia, es señal
de que el PR alteró el camino normal, que no debe tocar — investígalo antes de regenerar el golden.

Los casos de hueco van en **fixtures nuevos**, no reemplazando ese.

### Casos

- Reinicio limpio a mitad de turno → el acumulador continúa donde estaba.
- **El primer mensaje tras arrancar no borra el estado restaurado** (la trampa del apartado F).
- `SIGKILL` → no se pierde más de un mensaje.
- **Hueco de 4–6 h** → hoy se descarta por `max_valid = 5000`; con la cota escalada debe recuperarse.
  Es el test que demuestra el valor del PR.
- Rechazo legítimo por hueco → **no** debe disparar el camino de los 3 rechazos ni el re-anclaje.
- Contador que retrocede a 0 con hueco corto → se detecta, no suma.
- Hueco que excede el módulo (65.536) → re-siembra sin sumar.
- Cambio de turno durante un hueco → acumuladores a cero, sin arrastre.
- Hueco que abarca un turno completo → cero y marcado como incompleto.
- `gatewayTime` ausente o no parseable → implausible, sin caer a la hora del servidor.
- Reloj hacia atrás → implausible, no delta gigante.
- Config de tasas ausente o corrupta → arranca, registra, usa el valor conservador.
- Base de estado corrupta → el servicio arranca y se comporta como hoy.
- `state_json` con `v` desconocida → se ignora y se re-siembra.
- `SHIFT_MODE=2` y `SHIFT_MODE=3` → fronteras correctas en ambos.
- `TZ=UTC` → la prueba de fronteras de turno **debe fallar**.

## Orden de commits

0. Captura de `stderr` en `tests/support.hpp` + eliminación de `[ESM diag]`.
1. `CounterCtx` — refactor puro de firma, sin cambio de comportamiento. El golden lo protege.
2. `device_epoch_s()` + `last_accepted_epoch_s` en los estados.
3. `diff_counter_scaled` + `RateConfig` + tests. Sin persistencia el comportamiento apenas cambia:
   solo se recuperan deltas que hoy se descartan por el techo fijo.
4. `IStateStore` + SQLite + serialización de los diez tipos de estado.
5. Restauración + arreglo de `detect_global_shift_change` + tests.
6. Marcador de turno incompleto, si la decisión abierta lo permite.

Si hay que revertir del 4 en adelante, los commits 1–3 se sostienen solos y siguen siendo una mejora.

## Despliegue

- El primer arranque encuentra la base vacía → se comporta exactamente como hoy. El despliegue en sí
  no tiene riesgo.
- **Verificación en planta antes de confiar:** reiniciar el servicio a mitad de turno de forma
  controlada y comprobar que el siguiente `production` publicado continúa el acumulador en lugar de
  empezar en cero. Hasta que eso se vea una vez, el cambio no está validado.
- Revertir = `CELIMA_STATE_PERSISTENCE=0` y reiniciar. Sin recompilar.

## Fuera de alcance

- Corregir datos históricos ya enviados: el pipeline hacia AWS no admite reexpresión.
- La detección de picos por EMA de `SalidaHornoProcessor`: resuelve corrupción, no huecos.
- La causa del reinicio del host del 1 de septiembre.
- El desfase de arranque entre PLC y PC (D3): lo mitiga la migración a VM, no este código.
