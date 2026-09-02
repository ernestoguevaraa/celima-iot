# PR 2 — D1 + D2: persistencia de estado y cota escalada por tiempo

**Repo:** `celima-iot` · **Base:** `main@0ac7a9c` · **Rama sugerida:** `feat/persistencia-acumuladores`
**Depende de:** PR 1 mergeado (necesita el arnés de pruebas y los logs de estado).
**Diseño de referencia:** `docs/design/persistencia-acumuladores.md`

> Revisión 2 — reescrita contra el código real. La revisión 1 asumía que no existía cota alguna y
> excluía a `CalidadProcessor` de la persistencia. Ambas cosas eran incorrectas.

## Objetivo

Que el total de turno sobreviva a un reinicio del proceso o del host (D1), y que las cotas existentes
dejen de ser techos fijos para escalar con el hueco transcurrido (D2). **Los dos en el mismo
release**: persistir sin escalar la cota cambia un reporte corto por uno inflado, que es peor porque
nadie lo cuestiona.

## Principio que resuelve los empates

**Ante la duda, subcontar.** Un total bajo se nota y se investiga; uno inflado se reporta como
producción real y nadie lo cuestiona nunca. Cada vez que la cota, el reloj o la configuración dejen
ambiguo qué hacer, la salida correcta es re-sembrar y sumar 0.

---

## Punto de partida real

No hay que introducir protección: hay que hacerla dependiente del tiempo. Hoy existen dos caminos, y
**los dos hay que tocarlos**:

| Camino | Quién lo usa | Techo actual |
|---|---|---|
| `diff_counter_safe(curr, prev_ref, reject_count, max_valid, max_rejects)` | los siete procesadores de máquina | `max_valid = 5000` fijo |
| `safe_delta_u16(prev, curr, max_reasonable)` | `CalidadProcessor` | `MAXR_BOXES = 300`, `MAXR_BROKEN = 1200` |

Con la tasa medida de L1 prensa (~1.288 pzs/h), `max_valid = 5000` se agota en **~3,9 h de
producción**: cualquier hueco mayor descarta la recuperación legítima. El módulo real es 65.536
(los contadores ya no llevan máscara de 15 bits), así que el horizonte de ambigüedad son ~51 h — los
huecos reales de agosto, de 26 h y 31 h, caben dentro.

`spike_detected()` / `spike_ema_update()` de `SalidaHornoProcessor` **no se tocan**: detectan
corrupción por correlación entre contadores, que es un problema distinto al del hueco temporal.

---

## Prerrequisitos que no son código

Se pueden hacer en paralelo, pero **el PR no se despliega sin ellos**:

1. **Medir la retentividad de los contadores del PLC** (procedimiento en el documento de diseño).
   Determina qué comportamiento es correcto en las pruebas, no qué código se escribe.
2. **Derivar `tasa_max` por línea y máquina** del percentil 99 de los deltas observados. Con PR 1
   desplegado, los `[STATE] delta_rejected` y los `*_raw` del journal dan la muestra. Mídelos, no los
   estimes.

Sin (2) el PR puede desplegarse con el valor conservador por defecto, que subcuenta igual que hoy.
Nunca con uno optimista.

## Decisión abierta que bloquea el apartado E

**¿El `boxer-patrol-edge-processor` propaga campos desconocidos hacia InfluxDB y AWS, o los
descarta?** El marcador de turno incompleto es inútil si se pierde en el siguiente salto. Es una
dependencia con otro equipo. Si los descarta, el apartado E sale del alcance de este PR.

---

## A. Cota escalada por tiempo

**Archivo:** `src/MessageProcessor.cpp` (helpers compartidos, ~línea 30).

```cpp
struct DeltaResult {
    uint16_t value     = 0;
    bool     plausible = false;   // false → el llamador re-siembra y suma 0
};

// elapsed_s: segundos desde el último mensaje aceptado de esta clave
// rate_max_per_s: tasa máxima de la máquina, de configuración
DeltaResult diff_counter_scaled(uint16_t curr, uint16_t prev,
                                double elapsed_s,
                                double rate_max_per_s,
                                double margin = 1.5);
```

Reglas, en orden:

1. `elapsed_s <= 0` → `plausible = false`. Cubre el reloj hacia atrás y los timestamps duplicados.
   No intentes "arreglar" el tiempo negativo.
2. `max_plausible = rate_max_per_s * elapsed_s * margin`.
3. **Límite duro:** si `max_plausible >= 65536`, el delta es ambiguo por construcción — el contador
   pudo dar más de una vuelta. `plausible = false` sin más aritmética.
4. `raw = static_cast<uint16_t>(curr - prev)` — misma resta sin máscara que hoy.
5. `plausible = (raw <= max_plausible)`; `value = raw`.

`diff_counter_safe` pasa a envolver a `diff_counter_scaled` en lugar de a `diff_counter`, conservando
su contrato actual: solo avanza `prev_ref` con delta plausible, y re-ancla tras `max_rejects`
rechazos consecutivos. **Con la cota escalada, ese camino de re-anclaje deja de dispararse en huecos
legítimos**, y con él desaparecen los ~9 minutos de producción que hoy se pierden en cada evento.

`safe_delta_u16` recibe el mismo tratamiento para calidad. No basta con arreglar uno de los dos
caminos.

`elapsed_s` sale de `device_timestamp(msg)` (`inc/TimeUtils.hpp`) contra el timestamp del último
mensaje aceptado, que hay que guardar en el `State`. No uses la hora del servidor: el intervalo real
lo marca el gateway.

## B. Configuración de tasas

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
`[CONFIG] rates file not usable (<motivo>), using default_rate_per_h=<n>` y sigue. Ese valor por
defecto debe ser deliberadamente **bajo**: tasa baja → cota baja → más descartes → total subcontado,
que es el lado seguro del error.

Añadir la nueva variable a `packaging/defaults.env`.

## C. Persistencia del estado

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

**SQLite** como implementación: un archivo, sin servicio nuevo, atómica ante `SIGKILL`, inspeccionable
con `sqlite3`. Añadir `libsqlite3-dev` a las build-deps y `-lsqlite3` a `LDFLAGS` en el `Makefile`.

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

**Qué serializar por procesador**, con `to_json` / `from_json` de nlohmann:

- todos los `acc_*`
- todos los `last_*` / `prev_ref`
- **`reject_count` y las EMA de spike**, para no reiniciar la heurística en cada arranque
- el timestamp del último mensaje aceptado (necesario para `elapsed_s`)
- un campo `"v"` de versión de esquema: si es desconocida o mayor que la soportada, **ignora el
  estado guardado y re-siembra**. Nunca interpretes un formato que no conoces.

**Incluye a `CalidadProcessor`.** Acumula en la aplicación desde la v4 y está tan expuesto a D1 como
los demás: el 1 de septiembre perdió 16.483 piezas quebradas de turno entre las cuatro líneas.

**Ruta:** `$CELIMA_STATE_DB`, por defecto `/var/lib/iot-celima-mqtt/state.db`. En
`packaging/iot-celima-mqtt.service` añadir `StateDirectory=iot-celima-mqtt`; systemd crea el
directorio con permisos correctos incluso cuando el servicio deje de correr como root. La unidad ya
tiene `ProtectSystem=full`, `ProtectHome=true` y `PrivateTmp=true`, así que `/tmp` no es opción.

**Cadencia:** guardar tras procesar cada mensaje, dentro del mismo `mtx_` que protege el estado. Si
mides que el `fsync` añade latencia a la publicación, mueve el `save` a después del `publish_qos1` —
pero nunca lo dejes solo en `SIGTERM`: un corte de energía no da oportunidad de cerrar limpio.

## D. Lógica de restauración

Primera vez que llega un mensaje para una clave `(proc, line)` tras arrancar:

| Caso | Condición | Acción |
|---|---|---|
| 1 | Sin estado guardado | Sembrar, `acc_* = 0`. Comportamiento actual |
| 2 | `stored.shift != shiftNum` | `acc_* = 0`, sembrar. Log `reseed reason=shift_change_across_restart` |
| 3 | Mismo turno, `elapsed <= gap_short` | Restaurar todo, continuar normal |
| 4 | Mismo turno, `elapsed > gap_short` | Restaurar los `acc_*`, aplicar `diff_counter_scaled` al primer delta. Log `gap elapsed_s=…` |

`gap_short`: configurable. El intervalo normal entre tramas es ~180 s con reintentos a ~30 s, así que
5× ≈ 900 s es un punto de partida defendible. Anótalo como valor a revisar con datos, no como
constante definitiva.

**Cuidado con la interacción del arranque.** `detect_global_shift_change()` compara contra un atómico
global inicializado a `-1`, de modo que el **primer** mensaje tras arrancar siempre cuenta como
cambio de turno y hoy dispara `reset_all_processor_states()`. Si eso se deja intacto, borrará el
estado que acabas de restaurar. Es el punto más fácil de romper de todo el PR: inicializa el atómico
desde el estado persistido, o excluye el primer disparo.

## E. Marcador de turno incompleto

**Sujeto a la decisión abierta.** Acumular por clave los segundos de turno no observados (la suma de
los `elapsed` de los casos 2 y 4 dentro del turno en curso) y publicarlos como **campo nuevo** en el
payload de `production`:

```
"turno_segundos_no_observados": 33420
```

**No cambies la semántica de ningún campo existente.** Añadir es seguro; redefinir no, porque los
datos ya enviados a AWS no se pueden reexpresar.

## F. Disciplina ante fallos

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

Sobre el arnés que monta PR 1:

- Reinicio limpio a mitad de turno → el acumulador continúa donde estaba.
- `SIGKILL` → no se pierde más de un mensaje.
- **El primer mensaje tras arrancar no borra el estado restaurado** (la trampa del apartado D).
- Contador que retrocede a 0 con hueco corto → se detecta, no suma.
- Contador que avanza mucho con hueco largo dentro de la cota → suma la recuperación.
- Hueco que excede el módulo (65.536) → re-siembra sin sumar.
- **Hueco de 4–6 h** → hoy se descarta por `max_valid = 5000`; con la cota escalada debe recuperarse.
  Es el test que demuestra el valor del PR.
- Rechazo legítimo por hueco → **no** debe disparar el camino de los 3 rechazos y el re-anclaje.
- Cambio de turno durante un hueco → acumuladores a cero, sin arrastre.
- Hueco que abarca un turno completo → cero y marcado como incompleto.
- Reloj hacia atrás → implausible, no delta gigante.
- Config de tasas ausente o corrupta → arranca, registra, usa el valor conservador.
- Base de estado corrupta → el servicio arranca y se comporta como hoy.
- `state_json` con `v` desconocida → se ignora y se re-siembra.
- `SHIFT_MODE=2` y `SHIFT_MODE=3` → fronteras correctas en ambos.
- `TZ=UTC` → la prueba de fronteras de turno **debe fallar**.

## Orden de commits

1. `diff_counter_scaled` + `RateConfig` + tests. Sin persistencia el comportamiento apenas cambia:
   solo se recuperan deltas que hoy se descartan por el techo fijo.
2. `IStateStore` + SQLite + restauración + arreglo del `detect_global_shift_change` inicial + tests.
3. Marcador de turno incompleto, si la decisión abierta lo permite.

Si hay que revertir el 2, el 1 se sostiene solo y sigue siendo una mejora.

## Despliegue

- El primer arranque encuentra la base vacía → se comporta exactamente como hoy. El despliegue en sí
  no tiene riesgo.
- **Verificación en planta antes de confiar:** reiniciar el servicio a mitad de turno de forma
  controlada y comprobar que el siguiente `production` continúa el acumulador en lugar de empezar en
  cero. Hasta que eso se vea una vez, el cambio no está validado.
- Revertir = `CELIMA_STATE_PERSISTENCE=0` y reiniciar. Sin recompilar.

## Fuera de alcance

- Corregir datos históricos ya enviados: el pipeline hacia AWS no admite reexpresión.
- La detección de picos por EMA de `SalidaHornoProcessor`: resuelve corrupción, no huecos.
- La causa del reinicio del host del 1 de septiembre.
- El desfase de arranque entre PLC y PC (D3): lo mitiga la migración a VM, no este código.
