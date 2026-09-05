# PR 4 — Familia `Level` para contadores de nivel (`numero_grades`)

**Estado:** especificación lista para implementar.
**Orden:** este PR va **antes** que PR 3. Es autocontenido, no cambia ninguna API y
elimina el 78 % del ruido de log de un golpe.
**Defecto que corrige:** D5 — un registro de *nivel* tratado como contador monótono.

---

## 1. Qué está mal

`numero_grades` (entrada horno) es el **nivel del buffer de filas que esperan
entrar al horno**: sube cuando se cargan filas y baja cuando el horno se
desocupa. No es un contador monótono.

El código lo pasa por `diff_counter_safe()`, que resta en `uint16_t`. Una bajada
de 4 unidades produce `(uint16_t)(curr - prev) = 65532`.

Medido en producción, ventana del 4-Sep 18:00 al 5-Sep ~19:00:

| línea | rechazos | ticks/rechazo |
|---|---|---|
| L1 | 273 | 65.532,0 |
| L2 | 238 | 65.532,6 |
| L3 | 175 | 65.532,3 |
| L4 | 150 | 65.532,1 |

Cuatro líneas independientes clavadas en 65.532, a cuatro unidades del módulo.
Es `curr = prev − 4`: **836 bajadas de buffer al día**, todas registradas como
`delta_rejected`. Son el 78 % de los 1.068 rechazos diarios del servicio.

### Lo que hace hoy cada versión

- **Antes del 4-Sep:** `diff16` sin cota sumaba ~65.532 por cada bajada.
  `numero_grades_turno` era basura pura.
- **Después del 4-Sep:** la cota rechaza las bajadas y acepta las subidas.
  `numero_grades_turno` = **suma de las subidas del nivel** = filas que entraron
  al buffer durante el turno. Eso sí es una métrica real.

PR 2 arregló el campo por accidente. Este PR conserva ese valor y quita el ruido.

---

## 2. Objetivo

1. Interpretar el delta de un nivel **con signo**, no como resta sin signo.
2. **No cambiar el valor publicado de `numero_grades_turno`.**
3. Publicar como campos **nuevos** la información que hoy se tira: bajadas del
   nivel y tiempo con el buffer vacío.
4. Dejar `delta_rejected` de `numero_grades` en cero.

### No objetivos

- No tocar `numero_grades_instantaneo`. Invariante del repo: el crudo del PLC se
  publica tal como llega, no se deriva ni se corrige.
- No inferir la capacidad del buffer. Sigue siendo pregunta abierta para planta
  (D29007 en el mapa de entrada horno). El diseño no la necesita.
- No tocar ningún otro campo. Solo `entrada_horno` / `numero_grades`.

---

## 3. Diseño

### 3.1 Nueva familia

`inc/MessageProcessor.hpp`, línea 81:

```cpp
enum class CounterFamily { Event, TimeSeconds, TimeDeciseconds, Level };
```

`Level` significa: registro de 16 bits que representa una **magnitud
instantánea acotada** que sube y baja. La diferencia entre lecturas es un
**cambio con signo**, no un incremento.

### 3.2 Resultado con signo

`DeltaResult` hoy expone `uint16_t value`. Añadir el signo sin romper a los
llamadores existentes:

```cpp
struct DeltaResult {
    uint16_t    value         = 0;    // magnitud, como hasta hoy
    int32_t     signed_value  = 0;    // == value salvo en Level, donde puede ser < 0
    bool        plausible     = false;
    double      max_plausible = 0.0;
    const char* reason        = "";
};
```

Para toda familia distinta de `Level`, `signed_value == value`. Los llamadores
actuales no cambian.

### 3.3 Aritmética en `diff_counter_scaled()`

En `src/MessageProcessor.cpp`, tras resolver `family` y **antes** del bloque de
tasa, insertar la rama `Level`:

```cpp
if (family == CounterFamily::Level) {
    // Un nivel se mueve poco y en los dos sentidos. La resta sin signo
    // convierte una bajada de 4 en 65532; con signo es -4.
    r.signed_value = static_cast<int16_t>(curr - prev);
    r.value        = static_cast<uint16_t>(std::abs(r.signed_value));
    r.max_plausible = static_cast<double>(ctx.max_valid);
    r.plausible = (std::abs(r.signed_value) <= ctx.max_valid);
    if (!r.plausible) r.reason = "level_jump";
    return r;
}
```

No se usa `rate_max_per_s` ni `elapsed_s`: un nivel no tiene tasa. La cota es
`ctx.max_valid`, que el llamador fija con el `with(field, mv)` de dos argumentos
que ya existe en `CounterCtx`.

`ctx.max_valid` por defecto es 5.000, demasiado laxo para un buffer de filas. El
llamador pasa **500**: holgado frente a cualquier movimiento físico posible en
127 s y estricto frente a corrupción. Si aparecieran rechazos `level_jump` con
ese valor, es señal a investigar, no a subir el número.

### 3.4 Tabla de familias

`src/MessageProcessor.cpp`, dentro de `kCounterFamilies`, en el bloque de
entrada horno:

```cpp
constexpr CounterFamily kLv = CounterFamily::Level;
...
{"entrada_horno",      "numero_grades",            kLv},
```

Actualizar `family_name()` para devolver `"nivel"`.

Actualizar el comentario que precede a la tabla (hoy dice «Se lista SOLO lo que
es tiempo; todo lo demás es Event por defecto») para reflejar que ahora también
se listan niveles.

### 3.5 Acumulación en `EntradaHornoProcessor`

Estado nuevo en `State` (junto a `acc_numero_grades`), con su `to_json` /
`from_json` — sin esto no sobreviven al reinicio y se reintroduce D1:

```cpp
uint32_t acc_numero_grades_bajadas = 0;   // suma de las bajadas, en valor absoluto
uint32_t acc_buffer_vacio_s        = 0;   // segundos con nivel == 0
```

En la rama de acumulación (hoy línea ~2818):

```cpp
// numero_grades es un NIVEL: subidas y bajadas por separado. La subida
// conserva exactamente el valor que numero_grades_turno viene publicando
// desde el 4-Sep, cuando la cota empezó a descartar las bajadas.
const DeltaResult ng = diff_counter_scaled(
        numero_grades, st.last_numero_grades, ctx.with("numero_grades", 500));
if (ng.plausible) {
    if (ng.signed_value > 0) st.acc_numero_grades += ng.signed_value;
    else                     st.acc_numero_grades_bajadas += -ng.signed_value;
    st.last_numero_grades = numero_grades;
    st.rc_numero_grades = 0;
} else {
    // mismo re-anclaje por rechazos consecutivos que el resto de campos
}
```

**Ojo:** `diff_counter_safe()` no sirve aquí porque devuelve `uint16_t` y suma
siempre. Hay dos opciones; elige una y sé consistente:

- (a) llamar directo a `diff_counter_scaled()` en este campo y replicar a mano
  el re-anclaje (`rc_numero_grades`, `max_rejects`);
- (b) añadir `diff_level_safe()` en el mismo bloque que `diff_counter_safe()`,
  con la misma lógica de `reject_count` / `reanchor` / evento `[STATE]`, pero
  devolviendo `int32_t`.

**Preferida: (b).** Mantiene la observabilidad y el re-anclaje en un solo sitio
y deja `diff_counter_safe()` intacto para los ocho procesadores.

Tiempo con buffer vacío, en la misma rama y con el patrón que ya usa la
detección de vacío de entrada (`delta_mcf == 0 && delta_timer > 0`):

```cpp
if (numero_grades == 0 && delta_timer > 0) {
    st.acc_buffer_vacio_s += delta_timer;
}
```

Es una aproximación por muestreo: cuenta el intervalo entero si el nivel estaba
a cero en el instante de la trama. Con tramas cada 127 s la resolución es esa, y
hay que **documentarlo en el propio comentario del código** para que nadie lo
lea como una medida exacta.

### 3.6 Publicación

```cpp
prod["numero_grades_instantaneo"]    = numero_grades;              // sin cambios
prod["numero_grades_turno"]          = acc_numero_grades_out;      // MISMO VALOR
prod["numero_grades_bajadas_turno"]  = acc_numero_grades_bajadas_out;  // NUEVO
prod["buffer_vacio_turno_s"]         = acc_buffer_vacio_s_out;         // NUEVO
```

Campos nuevos, no reinterpretación de campos existentes: es lo que manda el
invariante del repo cuando el dato ya salió hacia AWS.

---

## 4. Pruebas

En `tests/test_scaled_bound.cpp`:

1. **Bajada normal.** `prev=40, curr=36`, familia `Level` → `signed_value == -4`,
   `plausible == true`. Hoy esto da 65532 y se rechaza.
2. **Subida normal.** `prev=36, curr=41` → `signed_value == 5`, plausible.
3. **Sin movimiento.** `prev=40, curr=40` → `signed_value == 0`, plausible, y no
   emite evento.
4. **Salto imposible.** `prev=40, curr=9000` → `plausible == false`,
   `reason == "level_jump"`.
5. **Cruce del módulo con signo.** `prev=2, curr=65534` → `signed_value == -4`.
   Este es el caso que hace correcto usar `int16_t` y no una resta condicional.
6. **La familia no filtra por tasa.** Con `rate_max_per_s = 0` (configuración
   ausente) un `Level` sigue siendo plausible; no debe devolver `no_rate`.

En `tests/test_replay.cpp`, la prueba que de verdad protege el invariante:

7. **`numero_grades_turno` no cambia.** Reproducir una secuencia real de
   `entrada_horno` con subidas y bajadas y comprobar que el acumulado publicado
   es **idéntico** al que produce el binario actual. Si difiere, el PR está mal:
   el objetivo es conservar el valor, no mejorarlo.
8. **Bajadas y vacío.** Sobre la misma secuencia, verificar
   `numero_grades_bajadas_turno` y `buffer_vacio_turno_s` contra valores
   calculados a mano.

En `tests/test_persistence.cpp`:

9. Los dos acumuladores nuevos sobreviven a un `to_json` / `from_json` y a la
   restauración dentro del mismo turno.

---

## 5. Verificación en planta

Tras desplegar, en el cambio de turno siguiente:

```bash
# 1. Cero rechazos de numero_grades. Antes: ~836/día.
journalctl -u iot-celima-mqtt.service --no-pager --since '-2 hours' \
| grep -c 'delta_rejected.*field=numero_grades'

# 2. El resto del ruido sigue igual (este PR no lo toca): ~239/día en _tiempo_ds.
journalctl -u iot-celima-mqtt.service --no-pager --since '-2 hours' \
| grep delta_rejected | grep -oP 'field=\K\S+' | sort | uniq -c | sort -rn

# 3. Los campos nuevos aparecen y son coherentes.
journalctl -u iot-celima-mqtt.service --no-pager --since '-30 min' \
| grep -oP 'entrada_horno.*"numero_grades_turno":\K\d+' | tail -3
journalctl -u iot-celima-mqtt.service --no-pager --since '-30 min' \
| grep -oP 'entrada_horno.*"numero_grades_bajadas_turno":\K\d+' | tail -3
```

Criterio de aceptación: (1) devuelve **0**; (3) `numero_grades_turno` sigue en el
mismo orden de magnitud que antes del despliegue, y `bajadas_turno` es del mismo
orden que `turno` (un buffer que sube tiene que bajar aproximadamente lo mismo a
lo largo de un turno completo).

Si `bajadas_turno` fuera mucho menor que `turno`, o el nivel crecería sin límite
o hay bajadas que se siguen perdiendo: investigar antes de dar el PR por bueno.

---

## 6. Riesgos

- **`numero_grades_turno` cambia de valor.** Es el riesgo principal y lo cubre la
  prueba 7. Si cambia, el dato ya publicado hacia AWS deja de ser comparable y no
  se puede reexpresar.
- **`max_valid = 500` demasiado bajo.** Se manifestaría como rechazos
  `level_jump`. La respuesta correcta es preguntar a planta por la capacidad del
  buffer, no subir el número a ojo.
- **`buffer_vacio_turno_s` se lee como medida exacta.** Es muestreo cada 127 s.
  Documentado en el código y aquí; si alguien construye un indicador encima, que
  sepa la resolución.

## 7. Lo que NO hay que hacer

- No aplicar `Level` a ningún otro campo sin evidencia. El criterio empírico es
  el de `numero_grades`: **ticks/rechazo ≈ 65.532**, que delata una bajada. Los
  `*_tiempo_ds` tienen otra firma (saltos de 8.000–24.000 hacia adelante) y otro
  mecanismo; van en PR 3.
- No dejar de publicar `numero_grades_turno`. Aunque su valor fuese inútil, el
  invariante del repo es añadir campos, no quitarlos.
- No tocar `numero_grades_instantaneo`.
