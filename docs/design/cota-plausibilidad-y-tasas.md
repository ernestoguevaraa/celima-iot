# Cota de plausibilidad: criterios de derivación de tasas y pendientes

Complementa `docs/design/persistencia-acumuladores.md`, que define **por qué** existe la cota.
Este documento cubre **cómo se obtienen los números** que la alimentan, qué se descubrió al
obtenerlos por primera vez, y qué queda sin resolver.

Fecha: 2026-09-02. Base: `celima-iot @ bf5afb3` (PR 2 desplegado) y 30 días de journal de planta.

---

## 1. Qué alimenta la cota

```
max_plausible = max( (tasa_maquina / 3600) * segundos_de_hueco * margin , max_valid )
```

Los valores viven en `packaging/rates.json`, que **no lleva comentarios**: es JSON estricto para que
`jq`, los linters de CI y cualquier parser lo abran sin sorpresas. Toda la explicación está aquí.

`RateConfig` lo carga en el arranque desde `$CELIMA_RATES_CONFIG`. Si falta o no parsea, registra
`[CONFIG] rates file not usable (<motivo>)` y sigue con `default_rate_per_h`. Verificado ejecutando
el binario contra un archivo válido, uno roto y uno inexistente.

---

## 2. Cuándo manda la tasa, y hasta dónde llega

Dos propiedades del código que cambian por completo cómo hay que leer estos números.

**`max_valid` es un piso, no un techo.** `diff_counter_scaled` calcula
`max_plausible = max(scaled, max_valid)`, con `max_valid` = 5000 en las máquinas y 300 / 1200 en
calidad. La cota escalada solo puede ser **más permisiva** que la anterior. Consecuencia: en
operación normal —tramas cada ~180 s— estas tasas **no cambian absolutamente nada**. Solo deciden
cuánta producción se recupera tras un hueco largo.

**El límite duro fija el horizonte de recuperación.** Si `scaled >= 65536` el delta es ambiguo por
construcción (el contador pudo dar más de una vuelta) y se re-siembra sin sumar.

| tasa | la cota empieza a mandar tras | recuperación posible hasta |
|---:|---:|---:|
| 500 u/h | 7,3 h | 87 h |
| 900 u/h | 3,7 h | 48 h |
| 1.700 u/h (calidad) | 0,5 h | 26 h |
| 6.000 u/h (esmalte) | 0,6 h | **7 h** |

**Una tasa alta no es "más segura por si acaso": recorta el horizonte de recuperación.** Es
contraintuitivo y conviene tenerlo presente antes de subir un número "por margen".

### Consecuencia: el esmalte no es recuperable en cortes largos

Los contadores de esmalte corren a 4.200–6.050 u/h. A esa velocidad el contador de 16 bits da la
vuelta en ~11 h, y con `margin = 1,5` el delta se declara ambiguo pasadas **~7 h de hueco**. En el
corte de ~28 h registrado en los últimos 30 días, la producción de esmalte **no es recuperable por
ninguna configuración**. No es un defecto de la cota; es el módulo del contador contra la velocidad
de la máquina. Si a la planta le importa esa métrica, la salida es de firmware (contador de 32 bits
o envío más frecuente), no de software.

---

## 3. Criterios de derivación

Descubiertos al derivar las tasas por primera vez, en este orden de importancia.

### 3.1. El intervalo se mide con `gatewayTime`, nunca con `timer1Hz`

`timer1Hz` es un contador de 16 bits a 1 Hz: da la vuelta cada 65.536 s = **18,2 h**. En un hueco de
26 h mide 28.244 s en lugar de 93.600. Si el cálculo cae a un intervalo nominal, el delta acumulado
del hueco se divide por 180 s.

Medido sobre un corte sintético de 26 h: la tasa salía **520 veces inflada** (349.060 u/h frente a
671 reales). Y una tasa inflada agranda la cota, que es exactamente lo que permite que un delta
absurdo se sume como producción real. Es el error peligroso, no el conservador.

`gatewayTime` es marca absoluta del gateway y no da la vuelta.

### 3.2. Los pares que cruzan un hueco se descartan

Todo par con intervalo > 600 s queda fuera (el nominal son ~180 s con reintentos a ~30 s, así que
eso tolera hasta dos tramas perdidas). Un par que cruza un hueco no informa de nada usable: el
contador pudo dar la vuelta, el dispositivo pudo reiniciarse, y la planta pudo estar parada parte
del tiempo.

Los huecos descartados se **reportan** en lugar de desaparecer: ese listado es, gratis, un censo de
cortes (ver §5).

### 3.3. Las unidades son las del contador crudo del PLC

No las del dato publicado. En las prensas el contador es **PISADAS** (D29005); los productos se
derivan multiplicando por el factor de línea (L1=3, L2=3, L3=2, L4=4). Una tasa expresada en
productos deja la cota 3-4x demasiado generosa.

### 3.4. La tasa la fija el contador más rápido **sano**, no el máximo bruto

Este criterio nació de un error concreto. El script proponía **2.900 u/h para L1/salida_horno**,
tomado de `bancalinos_comb1` (p99 = 2.884, máx = 12.406). Pero ese contador vale **0/0/0** en L2 y
L4, y en L3 tiene p99 = 0 con máx = 18.503: no es rápido, es errático. Los campos sanos de esa
misma máquina coinciden entre sí — `metrica_mdf_ciclos` 847, `barreira1` 847, `bancalinos_q300` 835,
`bancalinos_total` 834. El valor correcto es **950**, 3,4x menos.

Con 2.900 la cota mandaba desde 1,1 h y era 3x demasiado generosa justo en la ventana de 3,5–10 h,
donde cayeron los dos cortes de ~5 h del mes.

**Cómo distinguir un contador errático de uno legítimamente rápido:**

| | contador sano | contador errático |
|---|---|---|
| p50 frente a p99 | próximos | p50 = 0, p99 muy alto |
| máx frente a p99 | ~1,1–1,4x | 4x o más |
| entre líneas | valores comparables | 0 en unas líneas, enorme en otras |

Ejemplo de sano: `bancalino_l1` en L2/entrada_secador, p50 = 828 y p99 = 871, máx 956. **Ese sí es
legítimamente más rápido** que el ingreso al elevador (435), y es él quien debe fijar la tasa de esa
máquina. El criterio no es "usa el contador de producción" — es "usa el más rápido que esté sano".

### 3.5. Los contadores de tiempo no se miden

Su máximo es analítico:

| familia | ticks | u/h |
|---|---|---:|
| `*_tiempo_s`, `*_tempo_s`, `timer1Hz` | 1/s | 3.600 |
| `*_tiempo_ds` | 10/s | 36.000 |

Medirlos solo introduce ruido. Ver el pendiente P1.

### 3.6. Ante la duda, valor bajo

Tasa baja → cota baja → más descartes → total subcontado. Un total bajo se investiga; uno inflado se
reporta como producción real y nadie lo cuestiona nunca.

---

## 4. Procedimiento

`scripts/derive_rates.py` implementa §3.1, §3.2 y §3.5. Los criterios §3.3 y §3.4 requieren
**revisión humana de la tabla de salida** antes de aceptar los valores.

```bash
journalctl -u iot-celima-mqtt.service -o cat --no-pager --since "-30 days" \
  | grep -oP '^\[celima/data\] \K\{.*\}' > tramas.jsonl

python3 scripts/derive_rates.py tramas.jsonl
```

Base actual: ~10.000–17.000 muestras por máquina sobre 30 días. Por debajo de ~200 el script avisa y
el resultado no debe tomarse como definitivo.

---

## 5. Subproducto: censo de huecos

El listado de pares descartados separa dos fenómenos que antes se confundían.

**Cortes de planta.** Un hueco de ~28 h aparece casi simultáneo en las cuatro líneas (27,8 / 27,9 /
28,0 / 29,1 h), y otros dos de ~5,6 h y ~5,1 h también generalizados. **Tres cortes de planta en 30
días.** Es evidencia directa para el expediente de la migración a VM, obtenida sin trabajo extra.

**Dispositivos que se caen solos.** Huecos largos en una sola máquina, sin correlato en las demás:
128,2 h en L4/entrada_secador, 114,3 h en L4/entrada_horno, 64,3 h en L4/salida_horno, 55,4 y 52,5 h
en L3/entrada_horno, 50,2 h en L2/calidad. Y dispositivos con centenares de microcaídas:
L3/esmalte 1.046 huecos, L4/esmalte 896, L1/entrada_horno 802, L2/entrada_horno 451.

**La VM resuelve los tres cortes de planta. No toca los dispositivos que se caen solos.** Es un
problema de campo —LoRa, alimentación del Arduino, gateway— que hasta ahora no estaba cuantificado.

---

## 6. Hallazgos operativos

- **Prensa hidráulica 2 está en servicio**, en L1, con 8.275 muestras. Cierra la pregunta que quedó
  abierta al no verse tráfico suyo el 1-sep-2026. Y las prensas solo están instrumentadas en L1: no
  hay ninguna reportando en L2, L3 ni L4.
- **Se explica la anomalía del 1 de septiembre en L4/esmalte.** El documento de persistencia la dejó
  anotada sin resolver: su acumulador arrancó hacia las 10:34 en lugar de las 06:00. No fue una
  re-siembra: ese dispositivo tiene **896 huecos en 30 días** y solo 1.132 muestras válidas. Se cae
  constantemente.

---

## 7. Pendientes

### P1 — Una sola tasa por máquina no cubre las dos familias de contadores

`rate_max_per_s` se resuelve por `(línea, máquina)` y se aplica a **todos** los campos de esa
máquina. Pero conviven dos familias con ~50x de diferencia: en L1/salida_secador `metrica_mds`
avanza a 0,19/s y `metrica_mds_tiempo_ds` a 10/s.

Con las tasas actuales, dimensionadas para producción, **tras un hueco largo se recupera la
producción pero se descarta la recuperación de los acumuladores de tiempo**. El turno queda con
producción correcta y tiempo de operación corto — una inconsistencia que alguien notará sin poder
explicarla.

Corrección propuesta: clasificar el campo por familia en `CounterCtx` y usar la tasa configurada
solo para los de evento, con `3600` y `36000` u/h analíticos para `_s` y `_ds`. Es un cambio
acotado sobre `diff_counter_scaled`.

**Prioridad: alta.** Es el único pendiente que produce datos incorrectos de forma silenciosa.

### P2 — Dos tasas con base insuficiente

`L3/esmalte` (6.050) sale de 3.185 muestras con máx 68.958; `L4/esmalte` (4.200) de 1.132 muestras
con máx 29.348. El resto de máquinas ronda 13.000. Ambos dispositivos apenas reportan (§5).
Re-derivar cuando lo hagan con normalidad.

### P3 — Contadores muertos que se publican como producción

- `sentido_escolha_cantidad`: p50, p99 y máx en **0 en las cuatro líneas**. Nunca ha valido nada.
- `parada_mcf_cantidad`: p99 = 0 en las cuatro entradas de horno.
- `bancalinos_comb1`: errático en L1 y L3, muerto en L2 y L4 (§3.4).

Hay que decidir si son sensores desconectados, funciones no usadas, o un error de mapeo. Mientras
tanto viajan a AWS como campos de producción con valor 0.

### P4 — Criterio de dimensionado de calidad

`rates.json` **sí** aplica a `CalidadProcessor`: `safe_delta_u16` envuelve a `diff_counter_scaled`, y
`MAXR_BOXES` / `MAXR_BROKEN` actúan como pisos por delta, no como techos. Su tasa está dimensionada
por `totalBroken` (p99 1.343–1.700 u/h), que es un contador de rotura, no de producción.

El razonamiento: subcontar piezas rotas hace que la calidad se vea mejor de lo que fue, lo cual es
peor que subcontar producción. Queda escrito para que sea una decisión y no un accidente.

### P7 — La atribución a turno de los campos latcheados es incorrecta

Anotado desde PR 3 (§6 bis). El contador libre del PLC no se entera del cambio de
turno, así que un paro que cruza las 06:00 o las 18:00 se atribuye **entero al
turno siguiente**, aunque la mayor parte pertenezca al anterior. La aplicación no
puede repartirlo: el PLC entrega el bulto sin dividir y no dice cuándo empezó.

Para los `*_tiempo_ds` da igual —son réplicas del reloj y nadie los consume—, pero
si alguna vez se publica `paro_latched` como métrica de paro de línea, esa métrica
colocará en el turno equivocado justamente los paros más largos, que son los que
más probabilidad tienen de cruzar una frontera. Para un dato de control de planta
eso exige, como mínimo, marcar el evento como "a caballo" y publicar el instante
además de la duración.

### P5 — Las tasas envejecen

Salen de una foto de 30 días. Un cambio de formato, de velocidad de línea o de firmware las
invalida. Falta decidir cada cuánto se re-derivan y quién lo hace. Una revisión trimestral, o tras
cualquier cambio de proceso, parece razonable — pero no está acordado con nadie.

### P6 — Los `[STATE] delta_rejected` de producción aún no se han mirado

PR 1 los emite desde su despliegue. Son la muestra que permite validar estas tasas **en producción**
en lugar de contra el histórico: si una tasa está corta, aparecerán rechazos con `reason=over_bound`
en huecos legítimos. Conviene revisar el conteo la primera semana con las tasas nuevas puestas.

---

## 8. Verificación hecha

- `rates.json` limpio (26 entradas) carga correctamente: `[CONFIG] rates cargadas ... entradas=26`.
- Archivo roto e inexistente: el servicio **no aborta**, registra el motivo y sigue con
  `default_rate_per_h`, como pedía la especificación.
- El horizonte de recuperación y el umbral desde el que manda cada tasa están calculados a partir de
  `diff_counter_scaled`, no estimados.
