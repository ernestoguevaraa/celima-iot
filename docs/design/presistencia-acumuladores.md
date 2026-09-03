# Persistencia de estado de los acumuladores de turno

Nota de diseño de `celima-iot`.
Fecha original: 2026-08-27. Revisado el 2026-09-02 contra `main@0ac7a9c`, que era el código real
desplegado; la versión anterior se había escrito contra una copia de `MessageProcessor.cpp` de
febrero y contenía dos afirmaciones falsas, corregidas y marcadas abajo. **Actualizado el
2026-09-03 contra `main@e860857`**, con D1 y D2 ya implementados y con el defecto D4 que apareció
al implementarlos.

## Estado

| | estado |
|---|---|
| D1 — estado en RAM | **corregido** (`bf5afb3`), persistencia en SQLite |
| D2 — cota plana | **corregido** (`bf5afb3` + `63386bb`), escalada por tiempo y por familia |
| D3 — arranque desfasado PLC/PC | abierto; lo mitiga la migración a VM, no el código |
| D4 — identidad de turno | **corregido** (`e860857`); lo creó la corrección de D1 |

Los criterios de derivación de las tasas y sus pendientes viven en
`docs/design/cota-plausibilidad-y-tasas.md`.

## Problema

Tres causas de pérdida de producción, dos en el código y una en el entorno.

**D1 — pérdida de estado en cada reinicio.** Cada procesador guarda su estado por línea en
`static std::unordered_map<int, State>` protegido por un `static std::mutex`. No hay persistencia.
El reinicio del proceso pone a cero el turno en curso:

```cpp
if (!st.initialized || st.shift != shiftNum) {
    // re-siembra last_*, acc_* = 0
} else {
    // acumula deltas
}
```

Las instancias de procesador se crean **por mensaje** y se descartan; toda la continuidad vive en los
`static`. Evidencia de campo: el 1 de septiembre de 2026 costó 9 h 17 min de totales de turno en las
cuatro líneas, calidad incluida.

Agrava D1 que la unidad systemd tenga `Restart=on-failure` con `RestartSec=3`: un ciclo de caídas
vacía los acumuladores cada tres segundos, en silencio.

**D2 — la cota de delta existe, pero es plana.** *(Corregido: la versión anterior afirmaba que no
había cota y que `diff16` sumaba cualquier retroceso como vuelta de vuelta. `diff16` ya no existe.)*

El código actual tiene cuatro mecanismos de protección:

- `diff_counter(curr, prev, max_valid = 5000)` — resta `uint16_t` sin máscara (el rollover en 65536
  es automático); devuelve `0` si `delta > max_valid`.
- `diff_counter_safe(curr, prev_ref, reject_count, max_valid = 5000, max_rejects = 3)` — el que usan
  los siete procesadores de máquina. Solo avanza `prev_ref` con un delta válido, para que una trama
  corrupta no envenene también la siguiente; y tras 3 rechazos consecutivos re-ancla `prev_ref` por
  la fuerza, evitando que un valor malo congele el acumulador el resto del turno.
- `safe_delta_u16()` — lo usa `CalidadProcessor` con techos propios (`MAXR_BOXES = 300`,
  `MAXR_BROKEN = 1200`).
- `spike_detected()` / `spike_ema_update()` — detección por EMA en `SalidaHornoProcessor`; descarta
  la trama solo si ≥2 contadores independientes saltan a la vez (piso 300, factor 10×).

El defecto no es que falte protección. Es que **ninguna cota escala con el tiempo transcurrido**.
Con `max_valid = 5000` y la tasa medida de L1 prensa (11.958 piezas en 9,28 h ≈ 1.288 pzs/h), el
techo se agota en **unas 3,9 horas de producción**. Cualquier hueco mayor produce un delta legítimo
que se rechaza y se contabiliza como 0. Es exactamente el caso que interesa recuperar.

**Efecto secundario del re-anclaje.** Cuando la cota rechaza, `prev_ref` no avanza, así que los dos
mensajes siguientes también se rechazan y solo el tercero fuerza el re-anclaje. A ~180 s de
intervalo eso son **~9 minutos de producción real perdidos por evento**. No ocurre tras un reinicio
del proceso (ahí `initialized == false` re-siembra de inmediato), sino cuando el proceso sigue vivo
y los mensajes dejan de llegar: broker caído, dispositivo mudo, cambio de banco de registros.

**D3 — arranque desfasado entre los PLC y la PC.** Tras un corte de energía, planta y PLC vuelven
con la luz; la PC industrial **no reenciende sola** —espera a que alguien pulse el botón del
gabinete, en la práctica horas después. Durante esa ventana la línea ya produce y nadie escucha: el
broker MQTT vive en la misma PC y los dispositivos de campo no almacenan lo que no pudieron
entregar. Esa producción no está en ninguna base de datos.

D3 no es un defecto del código, pero condiciona su diseño: determina cuánta producción debe
recuperar el primer delta tras el hueco. Los huecos de 31 h y 26 h de agosto son D3, no D1.

**D4 — la identidad de un turno no es su número.** *(Detectado el 2026-09-03, al revisar la
implementación de D1 contra logs reales.)* La lógica de restauración comparaba `stored.shift !=
shiftNum` para decidir si el estado guardado pertenecía al turno en curso. Pero el número de turno
es 1, 2 o 3 y **se repite cada día**.

Con un hueco de ~24 h o más —justo los de 26 h y 31 h documentados en D3— el número coincide y el
estado del día anterior se restaura como si fuera del turno de hoy:

```
día 1, 15:17  cae el servicio           turno 1 (desde las 06:00 del día 1)
día 2, 17:20  vuelve el servicio        turno 1 (desde las 06:00 del día 2)
              stored.shift == shiftNum  -> "mismo turno" -> restaura los acumuladores de ayer
```

El turno de hoy arranca con el total de ayer: **inflación**, el error peligroso, y disparándose
exactamente en el escenario que D3 describe.

**Este defecto no existía antes de corregir D1.** Sin persistencia el estado moría con el proceso,
así que el número de turno bastaba para decidir. Persistir es lo que lo creó — el mismo patrón que
la regla de oro de abajo, y el segundo caso en este mismo trabajo.

Corrección: `shift_start_epoch()` en `inc/Shift.hpp` devuelve la época del arranque del turno que
contiene un instante dado, en hora local, restando 24 h cuando el turno cruzó medianoche. Comparar
ese valor distingue dos instancias del mismo número. La restauración compara instancias, no números.

## Regla de oro del orden

**D1 y D2 se corrigen en el mismo release.** Persistir el estado sin escalar la cota por tiempo
desenmascara el problema inverso: al restaurar, `initialized` queda en `true` y el primer mensaje
tras el hueco entra por la rama de acumulación contra un contador que pudo reiniciarse. Se cambia un
reporte corto por uno inflado, que es peor porque nadie lo cuestiona.

Lo mismo ocurre al migrar a la VM aunque no se toque el código: la VM sigue encendida mientras la
planta se apaga, así que el estado sobrevive y el riesgo de inflar queda expuesto igual.

**El patrón se repitió una segunda vez.** D4 no existía mientras el estado moría con el proceso;
apareció al persistirlo. La lección operativa: cada vez que se alarga la vida del estado, hay que
volver a preguntarse qué suposiciones dependían de que fuera efímero. En este caso, que el número
de turno identificaba un turno.

La VM **sí** resuelve D3 en su mayor parte, porque elimina la espera del botón. No del todo: sigue
habiendo desfase entre el arranque de los PLC y el de los servicios, ahora de minutos.

## Aritmética de contadores: el módulo real es 65.536

*(Corregido. La versión anterior asumía contadores de 15 bits con módulo 32.768.)*

El enmascarado de 15 bits (`0x7FFF`, módulo 32.768) existió mientras el firmware Arduino leía las
palabras pseudo-I2C con un desplazamiento de 1 bit, que halvaba todos los valores. Corregido el
firmware, la máscara se eliminó: **todos los campos son `uint16_t` completos con vuelta en 65.536**.
Queda solo la nota histórica en `src/MessageProcessor.cpp:25-29`.

Consecuencia sobre el horizonte de ambigüedad, con la tasa medida de L1 prensa (~1.288 pzs/h):

| | Módulo | Horizonte |
|---|---:|---:|
| Suposición anterior (15 bits) | 32.768 | ~25 h |
| **Real (16 bits)** | **65.536** | **~51 h** |

Los huecos reales de agosto —26 h y 31 h— quedan **holgadamente dentro** de la ventana no ambigua.
Esa producción es aritméticamente recuperable; hoy se pierde por la cota plana, no por el módulo.
Es un argumento a favor de la cota escalada, no en contra.

Si reaparece un campo con semántica de 15 bits, es un cambio de firmware, no una convención del
código.

## Pregunta abierta que condiciona el diseño: retentividad de los contadores

**¿Los contadores de los PLC son retentivos ante corte de energía?**

El equipo de planta afirma que las baterías de los PLC están operativas. No basta:

1. Una batería sana garantiza que se conserva el área de memoria retentiva, no que el contador
   **esté declarado** en ella.
2. La retentividad puede diferir por PLC y por variable; la respuesta puede no ser la misma en las
   cuatro líneas.
3. Una batería reporta correcta hasta que deja de serlo, y nadie se entera hasta el corte siguiente.

### Cómo medirlo en lugar de preguntarlo

No hace falta esperar al próximo corte: los huecos de agosto están dentro de la retención del
journal, y los payloads publicados llevan los contadores crudos (`*_raw`, `*_instantaneo`). Comparar,
por máquina, el último valor crudo antes del hueco con el primero después:

```bash
journalctl -b -2 -u iot-celima-mqtt.service -o short-iso --no-pager \
  | grep '\[PUB QoS1\].*/production' | tail -25     # antes del hueco del 28 ago

journalctl -b -1 -u iot-celima-mqtt.service -o short-iso --no-pager \
  | grep '\[PUB QoS1\].*/production' | head -25     # tras el arranque del 29 ago
```

- Contador **continuó** (incremento plausible para el tiempo que la línea produjo sin escucha) →
  **retentivo**.
- Contador volvió a **0 o a un valor bajo** → **no retentivo**: detectar el reinicio, re-sembrar y
  asumir la pérdida.

Hazlo para los dos huecos y las cuatro líneas: dos muestras coincidentes son evidencia, una sola es
casualidad.

**Implicación de diseño.** La cota cubre ambos casos sin conocer la respuesta, así que esto **no
bloquea la implementación**. Determina qué comportamiento es el correcto en las pruebas y qué se le
puede prometer a Celima. Mientras no esté medido, no se promete recuperación.

## Cota de plausibilidad escalada por tiempo

Sustituir el techo fijo por uno que dependa del hueco:

```
max_plausible = tasa_max_maquina * (t_mensaje - t_ultimo_mensaje) * margen
```

- `delta <= max_plausible` → producción real acumulada; sumar.
- `delta > max_plausible` → reinicio de contador; re-sembrar y sumar 0.

Detalles:

- `tasa_max_maquina` configurable por línea y máquina. Derivarla del percentil 99 de los deltas
  observados, no de una estimación.
- **Límite duro:** si `max_plausible` supera el módulo (65.536), el delta es ambiguo y no hay forma
  de recuperarlo — re-sembrar sin intentarlo. Con ~1.288 pzs/h eso son unas 51 h de hueco.
- Aplica a los dos caminos: `diff_counter_safe` (siete procesadores) y `safe_delta_u16` (calidad).
  No basta con arreglar uno.
- Al escalar la cota, el re-anclaje tras 3 rechazos deja de dispararse en huecos legítimos, y con
  ello desaparecen los ~9 minutos perdidos por evento.

## Qué persistir

Por clave `(procesador, línea)`:

- todos los `acc_*` del estado
- todos los `last_*` / `prev_ref`
- `reject_count` y las EMA de detección de picos, para no reiniciar la heurística en cada arranque
- `shift`
- `updated_at` — **imprescindible** por partida doble: sin él no se calcula la cota, no se mide el
  hueco y no se puede identificar la instancia de turno del estado guardado (D4)

**Incluye a `CalidadProcessor`.** *(Corregido: la versión anterior lo excluía.)* Ver abajo.

## Lógica de restauración al arrancar

La comparación es por **instancia de turno**, no por número (ver D4):

```
shift_start_epoch(stored.updated_at) == shift_start_epoch(timestamp_del_mensaje)
```

1. No hay estado guardado → sembrar, `acc_* = 0`.
2. Distinta instancia de turno → `acc_* = 0`, sembrar. Cubre tanto el cambio de turno normal como
   el hueco de más de un día que cae en el mismo número.
3. Misma instancia, hueco corto → restaurar todo y continuar normal.
4. Misma instancia, hueco largo → restaurar los `acc_*` y aplicar la cota escalada al primer delta.
   Se conserva el total del turno y no se adivina lo ocurrido en el hueco.
5. El hueco abarcó una o más fronteras de turno (caso típico de D3) → cae en el caso 2 por
   construcción: acumuladores a cero, sin arrastre, y el turno en curso queda marcado como
   incompleto.

`gap_short` separa los casos 3 y 4. **El intervalo real medido en planta es 187 s**, no los 180
nominales —el reloj del Arduino corre algo largo— y **entrada horno publica cada ~127 s**. Un
`gap_short` calculado como múltiplo del nominal debe partir del valor real de cada clase.

### Qué se recupera y qué no

El caso 4 recupera lo que la cota permite, y la cota depende de la familia del contador. Con el
módulo de 16 bits, el horizonte es:

| familia | tasa | recuperación posible hasta |
|---|---:|---:|
| evento (~900 u/h) | 0,25/s | 48,5 h |
| `*_tiempo_s`, `timer1Hz` | 1 tick/s | 12,1 h |
| `*_tiempo_ds` | 10 ticks/s | **1,2 h** |

Es decir: **tras un corte largo se recupera la producción pero no el tiempo de operación**. Un
contador de decisegundos avanza 36.000 por hora y da la vuelta cada 1,8 h, así que su delta es
ambiguo por construcción pasada una hora de hueco. En los cortes de 5 h y 28 h registrados no hay
nada que hacer. La corrección de las familias acotó el problema a huecos de menos de una hora; no
lo eliminó, y conviene no darlo por resuelto. Detalle en
`docs/design/cota-plausibilidad-y-tasas.md`.

## Almacenamiento

**SQLite** (recomendado): un archivo, sin servicio nuevo, `libsqlite3-dev` en las build-deps,
atomicidad ante kill duro, e inspeccionable con `sqlite3` cuando algo se vea raro.

**Alternativa:** JSON reescrito atómicamente (temp + `fsync` + `rename`). El estado son unas decenas
de enteros por línea; evita una dependencia nueva.

En ambos casos, **escribir en cada mensaje procesado**, no solo en `SIGTERM`: un corte de energía no
da oportunidad de cerrar limpio, y ese es el escenario. A 0,42 msg/s el costo es irrelevante incluso
con `synchronous = FULL`.

Ubicación: `packaging/iot-celima-mqtt.service` hoy **no** declara `StateDirectory` y corre como root,
con `ProtectSystem=full`, `ProtectHome=true` y `PrivateTmp=true` — `/tmp` no es opción. Añadir
`StateDirectory=iot-celima-mqtt` (→ `/var/lib/iot-celima-mqtt`), que además crea el directorio con
los permisos correctos cuando el servicio deje de ejecutarse como root.

## Observabilidad

Emitir una línea de log en cada descarte de delta, cada re-anclaje y cada re-siembra. Hoy
`diff_counter_safe` re-ancla `prev_ref` **sin dejar rastro**, y solo se registran los descartes por
trama repetida y los picos de salida horno.

El incidente del 1 de septiembre confirma el costo: identificar la re-siembra exigió reconstruirla
comparando `timer1Hz_turno` entre tópicos del journal. Una línea de log habría bastado.

**Marcar el turno incompleto.** Ante un hueco largo, además de re-sembrar, publicar una marca —mejor
los segundos de turno no observados— junto al total. Como el pipeline hacia AWS **no admite corregir
datos ya enviados**, un total bajo sin marca es indistinguible de un turno flojo y nadie podrá
reexpresarlo. Depende de que el `boxer-patrol-edge-processor` propague campos desconocidos. **Implementado pero
desactivado por defecto** tras `CELIMA_INCOMPLETE_SHIFT_MARKER=1`, a la espera de esa confirmación:
publicar un campo que el siguiente salto descarta no sirve de nada, y añadirlo sin avisar cambia el
contrato de la trama.

## Pruebas que conviene cubrir

El repo **no tiene ninguna prueba ni framework**; esto implica montarlo.

- Reinicio limpio a mitad de turno → el acumulador continúa donde estaba.
- Kill duro (`SIGKILL`) → no se pierde más de un mensaje.
- Contador que retrocede a 0 con hueco corto → se detecta reinicio, no suma.
- Contador que avanza mucho con hueco largo dentro de la cota → suma la recuperación.
- Hueco que excede el módulo (65.536) → re-siembra sin sumar.
- Cambio de turno durante un hueco → acumuladores a cero, sin arrastre.
- Hueco que abarca un turno completo → cero y marcado como incompleto.
- Reloj hacia atrás → tratado como implausible, no como delta gigante.
- Rechazo seguido de re-anclaje → con la cota escalada, un hueco legítimo ya **no** debe disparar el
  camino de los 3 rechazos.
- `SHIFT_MODE=2` y `SHIFT_MODE=3` → fronteras correctas en ambos.
- `current_shift_localtime()` con `TZ=UTC` → la prueba de fronteras **debe fallar**.
- **Hueco de ~24 h que cae en el mismo número de turno → NO se restauran los acumuladores** (D4).
  Es el caso que un test por número de turno deja pasar en verde.
- Turno que cruza medianoche (18:00 y 22:00): `shift_start_epoch` debe devolver el arranque de
  ayer, no el de hoy.
- El fixture de replay debe ser físicamente coherente: `timer1Hz` avanza exactamente los segundos
  transcurridos, y los acumuladores de tiempo a 1 y 10 ticks/s según su familia.

## Tramas que el decoder no pudo interpretar

Al revisar tráfico real apareció una clase de trama que llega a `celima/data` sin ser una lectura
válida del PLC: el decoder del network server no pudo interpretarla. Procesarlas contamina los
acumuladores con valores que no vienen de ningún contador.

`is_decoder_error()` las detecta y `handle_celima_data` corta antes de crear el procesador, así que
ni se acumulan ni se publican. Es un filtro de entrada, no una cota: la cota protege de deltas
absurdos entre lecturas válidas, no de tramas que no son lecturas.

## Evidencia de campo: incidente del 1 de septiembre de 2026

### Qué pasó

La PC industrial se reinició a las 15:20:36 (hora local). El boot anterior arrancó el 29 de agosto a
las 13:21 y su journal corta en seco a las 15:17:26 en mitad de una ráfaga de publicaciones — sin
`Stopping…`, sin `Reached target Shutdown`, y sin registro `shutdown` en `wtmp`. Parada dura; hueco
de 3 min 10 s.

Al arrancar, `initialized == false` para todas las claves. El turno 1 corre de 06:00 a 18:00
(`SHIFT_MODE=2`), así que **se perdieron 9 h 17 min de totales acumulados**. Los datos no se
perdieron en el PLC: solo en el acumulador de la aplicación y, por tanto, en lo reportado a AWS.

### Producción de turno perdida (valores publicados a las 15:15–15:17)

| Línea | Métrica | Total borrado |
|---|---|---:|
| L1 | prensa hidráulica 1 — `cantidadProductos_turno` | 11.958 |
| L1 | salida secador — `metrica_mds_turno` | 6.278 |
| L2 | salida secador — `metrica_mds_turno` | 6.407 |
| L2 | salida horno — `cantidad_produccion_turno` | 6.210 |
| L3 | salida secador — `metrica_mds_turno` | 6.222 |
| L3 | salida horno — `cantidad_produccion_turno` | 6.090 |
| L4 | salida secador — `metrica_mds_turno` | 4.104 |
| L4 | salida horno — `cantidad_produccion_turno` | 3.632 |

**Calidad también se perdió.** *(Corregido: la versión anterior afirmaba que los tópicos `calidad`
sobrevivieron porque publicaban totales del dispositivo. Es falso.)* `CalidadProcessor` v4 recibe
contadores monotónicos del dispositivo (`boxesQ1/Q2/Q6`, `totalBroken`), calcula el delta con
`safe_delta_u16` y publica **sus propias sumas de turno**. La evidencia estaba en el mismo dump: para
L4 el dispositivo envió `boxesQ1: 1861, boxesQ2: 1826, totalBroken: 8401` mientras la app publicaba
`extra_c1: 814, extra_c2: 1422, quebrados: 5192` — ninguna salida coincide con ninguna entrada.

| Línea | `quebrados` | `extra_c1` | `extra_c2` | `comercial` |
|---|---:|---:|---:|---:|
| L1 | 3.431 | 1.536 | 1.463 | 0 |
| L2 | 4.625 | 2.189 | 651 | 0 |
| L3 | 3.235 | 1.822 | 322 | 647 |
| L4 | 5.192 | 814 | 1.422 | 0 |

Solo el formato antiguo `cajaCalidad` pasa valores del dispositivo tal cual.

### Causa del reinicio: no identificada, y distinta a la de los cortes de agosto

Descartados con evidencia: apagado ordenado (sin secuencia de shutdown ni registro en `wtmp`);
kernel panic (`kernel.panic = 0`, `/sys/fs/pstore/` vacío); watchdog (`RuntimeWatchdogUSec=0`,
`/dev/watchdog` inexistente); intervención por SSH (`last -x` sin sesión a esa hora); fallo de
software previo (`journalctl -p err` limpio salvo un `snap.firmware-updater` que falla cada 3 horas
desde siempre).

Hipótesis: **reset caliente de origen eléctrico** — un transitorio que la fuente aguantó a medias.
Coherente con que el equipo se recuperara solo en 3 minutos pese a no tener auto-arranque tras corte
de AC; con un corte real habría quedado apagado esperando el botón, como en agosto.

**Implicación:** la VM resuelve D3 y el arranque tras un corte de planta, pero no evita que la
aplicación pierda el estado ante un reinicio del anfitrión por cualquier causa. Solo la persistencia
cubre ambos.

### Huecos de datos del mismo periodo (casos de D3)

- 17 ago 05:00 → 18 ago 12:16 — **31 h**
- 28 ago 11:29 → 29 ago 13:21 — **26 h**

Ambos dentro del horizonte de 51 h, es decir **recuperables en principio**. Son también las dos
muestras disponibles para medir la retentividad.

### Hallazgo colateral: buffering de stdout ciega la forensia

Las líneas de `iot-celima-mqtt` llegan al journal en ráfagas al mismo segundo, con hasta 36 s de
retraso respecto a la publicación real (verificado contra los logs del edge processor, que recibió
por MQTT los mismos mensajes en tiempo real). Es el comportamiento por defecto de libc: con stdout
conectado a un pipe, `printf`/`std::cout` usan buffer de bloque de 4 KB.

Consecuencia: **en cada parada dura se pierden hasta 4 KB de log**, justo las líneas que explicarían
la caída. El log del 1 de septiembre termina a las 15:17:26, pero la aplicación siguió viva algún
tiempo más. La única línea que se vacía en el momento es el `std::endl` del cambio de turno en
`src/MqttApp.cpp`.

Corrección: `std::cout << std::unitbuf;` al inicio de `main()`. Es estándar y no depende de
`sync_with_stdio` ni de extensiones de glibc, a diferencia de `setvbuf` con `_IOLBF` sobre un pipe.
**Aplicarlo antes que la persistencia**: sin esto, cualquier depuración posterior arranca con los
últimos segundos borrados, y es también lo que mantiene utilizable el journal para medir la
retentividad.

### Recuperación manual de un turno perdido

InfluxDB **no** sirve como fuente: guarda solo un subconjunto de lo publicado, sin los contadores
crudos. La única fuente es el journal, con dos limitaciones — retención (~7 semanas) y los 4 KB
perdidos en cada parada dura.

1. Último valor `*_turno` publicado antes de la caída → del journal.
2. Sumar lo que la aplicación acumule desde el arranque hasta el fin del turno.
3. El hueco entre ambos se cierra con los contadores `*_raw`, que son del PLC y no se enteran del
   reinicio: `raw_post_reboot − raw_pre_caída`, con el ajuste de vuelta en 65.536.

El número corregido **no puede volver a AWS**: el uploader no reenvía lo ya despachado y el pipeline
no contempla corrección de datos históricos. Sirve para reportar fuera del sistema, no para
arreglarlo.

### Observación registrada, sin investigar

`linea/4/esmalte` mostraba `timer1Hz_turno = 16.499` frente a ~33.200–33.400 del resto, lo que sitúa
el inicio de su acumulador hacia las 10:34 en lugar de las 06:00. No se determinó si fue re-siembra o
un dispositivo mudo desde el inicio del turno. Anotado por si reaparece.

## Nota aparte: turnos y zona horaria

`current_shift_localtime()` (`inc/Shift.hpp`) usa la hora local del SO. La PC actual está en
`America/Lima`; Ubuntu Server se aprovisiona en UTC por omisión. Si la VM queda en UTC, las fronteras
se corren cinco horas y la producción se atribuye al turno equivocado **sin generar ningún error**.
Ya es requisito y criterio de aceptación en la especificación de la VM.

El binario admite `SHIFT_MODE=2` (dos de 12 h: 06:00–17:59 / 18:00–05:59) y `SHIFT_MODE=3` (tres de
8 h: 06/14/22). **Producción corre con `2`; el valor por defecto del código si la variable falta es
`3`.** Con solo dos fronteras diarias, cualquier caída posterior a media mañana borra casi un turno
entero — lo que agrava D1 respecto a un esquema de tres turnos.

## Nota aparte: la PC industrial corre Ubuntu Desktop

El journal muestra un gestor de sesión de usuario y snaps de escritorio (`firmware-updater`) en el
equipo de planta. Añade vectores de reinicio que no deberían existir ahí: snaps que se
auto-refrescan, `unattended-upgrades`, fwupd. Para la VM: Ubuntu Server mínimo, sin snapd, con
actualizaciones controladas y ventana de mantenimiento acordada.
