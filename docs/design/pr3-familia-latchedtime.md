# PR 3 — Familia `LatchedTime` para los acumuladores latcheados (`*_tiempo_ds`)

**Estado:** especificación lista para implementar.
**Orden:** va **después** de PR 4. Toca `CounterCtx`, y por tanto los ocho
procesadores; PR 4 no toca nada compartido.
**Defecto que corrige:** D6 — la cota por tasa aplicada a un acumulador que no
tiene tasa.

---

## 1. Qué son realmente estos registros

Del ladder de `entrada_secador.pdf`, sección `END`:

```
CF100 P_0_1s  →  @++ D29106                        ; contador libre, +1 cada 100 ms, SIEMPRE
Q:3.06        →  @++ D29007                        ; cuenta de eventos
CF100 P_0_1s  →  @++ D29108                        ; otro contador libre
Q:3.06        →  + (400) D29108 D29008 D29008      ; D29008 += D29108
                 MOV (021) &0 D29108               ; y D29108 vuelve a cero
```

`D29008` **no mide tiempo en estado**. Cuando ocurre el evento le suma el
contador libre acumulado desde la vez anterior y lo reinicia. Es un **reloj de
pared latcheado en los eventos**: se congela mientras la máquina está parada y
suelta el paro entero de golpe en el primer evento tras el arranque.

D29006 (con `T0480`) y D29010 (con `Q:3.07`) siguen el mismo patrón.

### Mapa de words confirmado

El ladder hace `@XFER &25 D29000 D29200` → `XFER #19 D29200 400` → `SFT 400 424`
y lo serializa por `Q:3.01` (dato) y `Q:3.04` (reloj). El Arduino lo declara:
`W1 = D29001 = timer de 1 Hz`. Con `PAYLOAD_WORDS 11`, **W0..W10 = D29000..D29010**:

| word | registro | ladder | campo publicado | familia hoy |
|---|---|---|---|---|
| W1 | D29001 | `@++` en `P_1s` | `timer1Hz` | kS ✔ |
| W4 | D29004 | `@++` en `P_1s` condicionado | `paradas_tempo` | kS ✔ |
| W5 | D29005 | `@++` en evento | *(cuenta)* | Event ✔ |
| **W6** | **D29006** | **`+= D29106`, reset** | `ingreso_elevador_tiempo` | kDs ✘ |
| W7 | D29007 | `@++` en `Q:3.06` | *(cuenta)* | Event ✔ |
| **W8** | **D29008** | **`+= D29108`, reset** | `bancalino_l1_tiempo` | kDs ✘ |
| W9 | D29009 | `@++` en `Q:3.07` | *(cuenta)* | Event ✔ |
| **W10** | **D29010** | **`+= D29110`, reset** | `bancalino_l2_tiempo` | kDs ✘ |

`D29007` y `D29008` se disparan con **la misma señal**. La cuenta nunca salta;
el tiempo siempre. Por eso los contadores de cantidad tienen cero rechazos y los
de tiempo los tienen todos.

Mapa de líneas, de los `config.h`: **enaplic4 = L1, enaplic5 = L2, enaplic6 = L3,
sacmi5 = L4**. `MIN_TX_INTERVAL_MS` = 180.000 en entrada secador y 120.000 en
entrada horno; los 187 s y 127 s observados son eso más ~7 s de cuantización del
ciclo de decodificación (el PLC publica cada ~5 s), **no deriva del reloj del
Arduino** como se creía.

---

## 2. Qué está mal en el código

La cota es `max(rate × elapsed_desde_la_última_trama × margin, max_valid)`. Para
kDs eso es `max(15 × elapsed, 5000)`. Pero el incremento de un campo latcheado
vale **el paro anterior**, magnitud que no guarda ninguna relación con el
intervalo de trama. En cuanto un paro supera ~500 s, el delta cruza el suelo de
5.000 y se rechaza.

Y como `diff_counter_safe()` no actualiza `prev_ref` al rechazar, el delta crece
~1.870 por trama y sigue fuera de cota hasta que `reject_count` llega a 3 y
dispara el `reanchor`. De ahí que casi todos los conteos de rechazo sean
múltiplos de tres.

**Medido:** 239 rechazos/día repartidos en nueve campos `_tiempo_ds`, todos
`over_bound` con `elapsed_s < 300`.

### Evidencia de que son relojes

Comparando el acumulado del turno contra `timer1Hz_turno`:

- **L3:** `bancalino_l1_tiempo_turno_ds / timer1Hz_turno = 10,00` constante,
  trama tras trama, todo el turno. L3 no tuvo ningún rechazo en ese campo.
- **L2:** ratio 9,40 → 9,45, con un **déficit fijo de ~23.600 ticks** (39 min
  20 s) que no se mueve en 3.544 s de turno. Es exactamente el paro que se
  rechazó, y no se recupera nunca porque el `reanchor` fija `prev_ref` al valor
  posterior al salto.

O sea: el campo es una réplica del reloj del turno, y la versión actual le resta
los paros.

### Consecuencia incómoda

El dato que se pierde **no vale nada**: la versión anterior al 4-Sep publicaba
una copia redundante de `timer1Hz_turno`. Esto no se arregla para recuperar
información, se arregla para (a) devolver el valor que se venía publicando, que
el pipeline hacia AWS no admite reexpresar, y (b) callar 239 rechazos diarios que
enmascaran problemas reales en el log.

---

## 3. Diseño

### 3.1 Nueva familia

```cpp
enum class CounterFamily { Event, TimeSeconds, TimeDeciseconds, Level, LatchedTime };
```

`LatchedTime`: acumulador de 16 bits que avanza a saltos, cada salto igual al
tiempo transcurrido desde el evento anterior. **No tiene tasa.** Ninguna cota
basada en el intervalo entre tramas es aplicable.

### 3.2 La invariante, con el arrastre de turno incluido

El campo es un reloj, así que su acumulado tiende al tiempo transcurrido del
turno. Pero **el contador libre del PLC no sabe nada de turnos**: sigue corriendo
a través de las 06:00 y las 18:00. Un paro que empieza a las 17:40 y termina a
las 18:20 se suelta **entero dentro del turno 2**, cuyo reloj lleva 1.200 s.
El acumulado va legítimamente por delante del reloj al principio de cada turno.

Medido: sobre el turno **completo** L2 y L3 dan 10,00 exacto —los arrastres se
compensan, cada turno regala su cola y hereda la del anterior— pero **dentro** del
turno, y sobre todo al principio, `acc > 10 × elapsed`.

El exceso tiene un techo físico: el arrastre es un solo salto de un registro de
16 bits, así que **no puede pasar de un módulo**. Por encima de 65.536 ticks
(6.553,6 s = 1,82 h) el contador libre ha dado la vuelta y el valor ya es
ambiguo por construcción. De ahí la cota:

```
acc + delta  ≤  10 ticks/s × segundos_transcurridos_del_turno × (1 + TOL)  +  65536
```

`TOL = 0,05` cubre la deriva entre el reloj del PLC y el del host. El sumando de
65.536 es el arrastre máximo posible, no un margen a ojo.

**Honestidad sobre lo que esta cota vale.** Al principio del turno el término del
arrastre domina y la cota admite casi cualquier delta; al final domina el reloj.
Es decir: **para un `LatchedTime` la plausibilidad de un delta concreto no es
decidible desde esta aplicación** — el salto vale el paro anterior, y la app no
sabe cuándo ocurrió el evento previo. Esto es un guardarraíl contra un acumulado
desbocado, no un filtro fino. Y está bien que lo sea: el Arduino ya descarta las
tramas que no valida contra W1, así que aquí no llega basura que filtrar.

Esto exige dos datos nuevos en `CounterCtx`:

```cpp
struct CounterCtx {
    ...
    double   shift_elapsed_s = 0.0;   // desde el inicio del turno hasta esta trama
    uint32_t acc_current     = 0;     // acumulado actual del campo
};
```

`shift_elapsed_s` se calcula con lo que ya existe:
`*dev_epoch - shift_start_epoch(*dev_epoch, shift_mode)` (ver `inc/Shift.hpp`,
usado igual en `restore_state_if_needed()`). **No** usar `acc_timer1Hz`: es una
magnitud observada y quedaría corta tras un hueco, volviendo la cota demasiado
estricta justo en el caso que este PR intenta admitir.

### 3.3 Rama en `diff_counter_scaled()`

Antes del bloque de tasa:

```cpp
if (family == CounterFamily::LatchedTime) {
    // Acumulador latcheado: el incremento vale el paro ANTERIOR, no el
    // intervalo entre tramas. Ninguna cota por tasa aplica. Lo único que
    // se puede afirmar es que el acumulado no supera el reloj del turno.
    if (ctx.shift_elapsed_s <= 0.0) {
        r.reason = "no_shift_elapsed";   // sin turno no hay cota: se descarta
        return r;
    }
    // El +65536 es el arrastre: un paro que cruza la frontera de turno se
    // suelta entero en el turno nuevo y su tiempo es de antes. Como es un
    // solo salto de un registro de 16 bits, no puede pasar de un módulo.
    const double techo = 10.0 * ctx.shift_elapsed_s * 1.05 + 65536.0;
    r.max_plausible = techo;
    r.plausible = (static_cast<double>(ctx.acc_current) + r.value <= techo);
    if (!r.plausible) r.reason = "over_shift_clock";
    return r;
}
```

`r.value` sigue siendo `(uint16_t)(curr - prev)`, calculado arriba como siempre.

### 3.4 Observabilidad: el paro sale gratis

Un delta grande en un campo `LatchedTime` **es un paro de línea con su duración
medida**. Hoy eso se escribe como `delta_rejected` y se tira. Emitir un evento
propio, informativo, no un rechazo:

```cpp
// En el llamador, cuando el delta es plausible y grande.
// Umbral: 3 intervalos de TX. Por debajo es operación normal.
if (r.plausible && r.value > 3 * 10 * intervalo_tx_nominal_s) {
    celima::log::state_event("paro_latched", ctx.line, ctx.proc,
        "field=" + std::string(ctx.field) +
        " duracion_s=" + std::to_string(r.value / 10) +
        (r.value > 60000 ? " AVISO=posible_wrap" : ""));
}
```

**Límite físico que hay que documentar en el propio comentario:** `D29106` y sus
gemelos son de 16 bits a 10 Hz, así que **dan la vuelta a las 1,82 h**. Un paro
más largo produce un salto no solo grande sino *equivocado* (corto en múltiplos
de 6.553,6 s). El aviso `posible_wrap` marca los casos cercanos al módulo. No se
puede corregir desde esta aplicación; es una limitación del PLC.

Este PR **no publica** ningún campo nuevo con el paro. Eso es un PR aparte, y
requiere decidir antes qué se hace con el techo de 1,82 h.

### 3.5 Campos que migran

De `kCounterFamilies`, pasan de `kDs` a `LatchedTime` los nueve que muestran la
firma en producción:

```
prensa_hidraulica1  metrica_tiempo
prensa_hidraulica2  metrica_tiempo
entrada_secador     ingreso_elevador_tiempo
entrada_secador     bancalino_l1_tiempo
entrada_secador     bancalino_l2_tiempo
salida_secador      metrica_mds_tiempo
esmalte             metrica_esm_tiempo
entrada_horno       metrica_mcf_tiempo
entrada_horno       metrica_formador_tiempo
salida_horno        metrica_tiempo
salida_horno        barreira1_tiempo
```

**`salida_horno / sentido_escolha_tiempo` NO migra.** Lleva más de un mes
congelado, no ha producido un solo delta y por tanto no hay evidencia de su
firma. `LatchedTime` es más permisiva que `kDs`, así que clasificar de más es el
error peligroso: se queda en `kDs` hasta que alguien lo vea moverse.

**Ningún campo `kS` migra.** `paradas_tempo`, `parada_mds_tiempo`,
`parada_esm_tiempo`, `parada_mcf_tiempo`, `falha_forno_tiempo` y `timer1Hz` tienen
cero rechazos y otro mecanismo en el ladder (`@++` sobre `P_1s` mientras la
condición es cierta: acumulación suave, tiempo en estado real).

`falha_forno_tiempo` merece mención aparte porque **es el campo que consume el
dashboard**. Prueba de que no es un reloj latcheado: su acumulado va del 0,7 % al
100 % del turno según la línea y el día; una réplica del reloj daría siempre
100 %. No se toca en este PR.

### 3.6 Efecto sobre los valores publicados

Los `*_tiempo_turno_ds` **suben** y vuelven a ser ≈ 10 × `timer1Hz_turno`, que es
lo que se publicaba antes del 4-Sep. Los `*_tiempo_turno_s` derivados
(`static_cast<double>(acc_..._ds) * 0.1`) siguen automáticamente.

Hay que decirlo claro a quien mire históricos: **estos campos tienen dos
discontinuidades**, una el 4-Sep 18:01 hacia abajo y otra el día de este
despliegue hacia arriba. Los turnos intermedios están cortos por los paros
rechazados. Ningún consumidor conocido los usa hoy, pero conviene que quede
escrito.

---

## 4. Pruebas

En `tests/test_scaled_bound.cpp`:

1. **Operación normal.** `LatchedTime`, `shift_elapsed_s = 4000`,
   `acc_current = 39000`, delta 1.870 → plausible (39.000+1.870 ≤ 42.000).
2. **Salto de paro.** Mismo contexto, delta **22.400** → plausible. Este es el
   caso que hoy se rechaza y es el corazón del PR.
3. **Arrastre de turno — el caso que rompió la primera versión de esta spec.**
   Recién empezado el turno: `shift_elapsed_s = 1200`, `acc_current = 0`,
   delta **24.000** (un paro de 40 min que empezó a las 17:40 y terminó a las
   18:20) → **plausible**. Con la cota sin el término de arrastre esto se
   rechazaba: 10 × 1200 × 1,05 = 12.600 < 24.000.
4. **Por encima de todo lo posible.** `shift_elapsed_s = 40000`,
   `acc_current = 450000`, delta 60.000 → `plausible == false`,
   `reason == "over_shift_clock"`.
5. **Sin turno.** `shift_elapsed_s = 0` → `reason == "no_shift_elapsed"`, no
   plausible. Nunca debe colarse un delta sin poder acotarlo.
6. **`elapsed_s` es irrelevante.** Con `elapsed_s = 127` y con `elapsed_s = 4000`,
   el mismo delta da el mismo resultado. La familia no depende del intervalo
   entre tramas: si depende, está mal implementada.
7. **La tasa configurada es irrelevante.** Con `rate_max_per_s = 0` no debe
   devolver `no_rate`.

En `tests/test_replay.cpp`:

8. **Reproducción del caso L2.** Secuencia con ~12 tramas de delta 0 y luego un
   delta de ~22.400. Comprobar que el acumulado final es ≈ 10 × el reloj del
   turno, y que **no** se emite ningún `delta_rejected`.
9. **Turno completo con paro a caballo de la frontera.** Reproducir el final de
   un turno y el principio del siguiente con un paro que cruza las 18:00.
   Comprobar que el salto se acepta en el turno nuevo y que la ratio del turno
   completo converge a 10,00. **Es la prueba de regresión de este fallo de
   diseño**; sin ella se vuelve a colar.
10. **Sin regresión en kS.** La misma secuencia no debe alterar
    `falha_forno_tiempo_turno_s` ni `paradas_tiempo_turno_s`.

En `tests/test_persistence.cpp`:

11. `shift_elapsed_s` y `acc_current` se calculan bien en la primera trama tras
    una restauración dentro del mismo turno (el acumulado viene de la base, el
    reloj del turno del `dev_epoch`).

---

## 5. Verificación en planta

El criterio es numérico y no admite interpretación: **tras un turno completo, la
ratio tiene que ser 10,00 en las cuatro líneas.**

```bash
journalctl -u iot-celima-mqtt.service -o short-iso --no-pager --since '-24 hours' \
| grep 'entrada_secador/production' | tail -40 \
| python3 -c '
import sys, re
for l in sys.stdin:
    b = re.search(r"\"bancalino_l1_tiempo_turno_ds\":(\d+)", l)
    t = re.search(r"\"timer1Hz_turno\":(\d+)", l)
    n = re.search(r"\"lineID\":(\d+)", l)
    if b and t and n and int(t.group(1)):
        print(f"L{n.group(1)}  banc_ds={int(b.group(1)):7d}  timer_s={int(t.group(1)):6d}"
              f"  ratio={int(b.group(1))/int(t.group(1)):5.2f}")
'
```

Y los rechazos de `_tiempo_ds` a cero:

```bash
journalctl -u iot-celima-mqtt.service --no-pager --since '-2 hours' \
| grep delta_rejected | grep -oP 'field=\K\S+' | sort | uniq -c | sort -rn
```

Con PR 4 ya desplegado, esa lista debe quedar **vacía**. Si sobrevive algo, es un
campo con una tercera firma que no hemos caracterizado: investigar, no
reclasificar a ojo.

Y el detector de paros, que es el subproducto útil:

```bash
journalctl -u iot-celima-mqtt.service --no-pager --since '-24 hours' \
| grep paro_latched
```

---

## 6. Riesgos

- **`LatchedTime` es más permisiva que `kDs`.** Un campo mal clasificado deja
  pasar deltas absurdos. Mitigación: solo migran los once con firma medida, y la
  cota contra el reloj del turno sigue siendo un techo duro.
- **`shift_elapsed_s` mal calculado.** Si sale 0 o negativo, todos estos campos
  dejan de acumular y el fallo es silencioso salvo por `no_shift_elapsed` en el
  log. Cubierto por la prueba 4; vigilar ese `reason` en la verificación.
- **`CounterCtx` toca los ocho procesadores.** Es la parte mecánica y repetitiva
  del PR: rellenar `shift_elapsed_s` y `acc_current` en cada llamada. Un campo
  sin rellenar cae en `no_shift_elapsed` y deja de acumular. Revisar uno por uno.
- **Los paros de más de 1,82 h se siguen midiendo mal.** No es corregible aquí.
  Queda marcado con `AVISO=posible_wrap` y va como pregunta a automatización.
- **La cota es un guardarraíl, no un filtro.** Ver §3.2: al principio del turno el
  término de arrastre la vuelve casi inoperante. Quien la lea esperando que
  detecte deltas anómalos se va a llevar una sorpresa; lo único que detecta es un
  acumulado desbocado.

## 6 bis. Un defecto que este PR NO arregla, y que hay que anotar antes de PR 5

**La atribución a turno de estos campos es incorrecta por construcción.** El
contador libre del PLC no se entera del cambio de turno, así que un paro que
cruza las 06:00 o las 18:00 se atribuye **entero al turno siguiente**, aunque la
mayor parte del tiempo pertenezca al anterior. La aplicación no puede repartirlo:
el PLC entrega el bulto sin dividir y no dice cuándo empezó.

Para los `*_tiempo_ds` da igual: son réplicas del reloj y nadie los consume. Pero
**si PR 5 llega a publicar `paro_latched` como métrica de paro de línea, esa
métrica va a colocar en el turno equivocado justamente los paros más largos**,
que son los que más probabilidad tienen de cruzar una frontera. Para un dato que
se usa en control de planta eso no es aceptable sin, como mínimo, marcar el
evento como "a caballo" y publicar el instante del evento además de la duración.

Es el mismo patrón que D4: un total de turno que no sabe a qué instancia de turno
pertenece su contenido. Anotarlo en `claude/cota-plausibilidad-y-tasas.md`.

## 7. Lo que NO hay que hacer

- **No subir `max_valid` ni `margin`.** Es la tentación obvia y es exactamente
  D2: la cota no está apretada, es que no aplica.
- **No tocar `falha_forno_tiempo`.** Es el campo del dashboard, es `kS`, tiene
  cero rechazos y es estructuralmente inmune. Cualquier cambio ahí rompe la única
  métrica que hoy se está consumiendo de verdad.
- **No sustituir el acumulado por `10 × timer1Hz_turno`.** Aunque el valor sea
  equivalente, el invariante del repo es que el acumulado sale de deltas del
  crudo. La cota contra el reloj es una validación, no una fuente.
- **No publicar todavía el paro como campo.** Primero hay que decidir qué se hace
  con el techo de 1,82 h.
  