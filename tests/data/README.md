# Datos del test de replay

## `celima_data_replay.jsonl`

**Es un fixture sintético, no tráfico de planta.** Se generó con semilla fija
(`20260902`) porque este repo no tiene acceso al journal de la PC industrial.
Cubre lo que pide la spec —los 8 `deviceType` en 4 líneas, 1 h de tráfico por
combinación, 681 tramas— y además fuerza a propósito tres casos:

| Caso | Dónde |
|---|---|
| Rollover de 16 bits | contadores que arrancan cerca de 65.536 en varias líneas |
| Salto > `max_valid` (rechazo + re-ancla) | `metrica_mds_cantidad`, L2 / `deviceType` 4, intervalos 7–9 |
| Trama repetida por el NS | L1 / `deviceType` 3, intervalo 3 (duplicada) |

Todas las tramas llevan `gatewayTime`, para que `device_timestamp()` no caiga a
la hora del servidor y el replay sea determinista.

El fixture respeta dos propiedades físicas que antes no cumplía, y que un test
comprueba en cada ejecución:

- **El intervalo depende de la clase:** `entrada_horno` publica cada 120 s (30
  tramas por hora) y el resto cada 180 s (20 tramas).
- **`timer1Hz` avanza exactamente los segundos transcurridos**, con vuelta en
  65.536. Es un contador libre a 1 Hz: no puede avanzar otra cosa. La versión
  anterior le ponía incrementos al azar de hasta 360 por trama, que superan la
  cota de su familia (`tiempo_s`, 1 tick/s) y pasaban solo porque manda el techo
  mínimo de `max_valid` — el fixture estaba tapando esa comprobación.

Una simplificación que queda: la trama repetida del intervalo 3 llega con el
**mismo** `gatewayTime`. En planta el gateway re-sella la marca al recibir el
reintento, así que la copia llega con una posterior. Ese caso —el que la
deduplicación por ventana puede no reconocer— está cubierto en
`tests/test_state_events.cpp`, no aquí.

### Sustituirlo por tráfico real

Es lo que pide la spec y mejora la prueba. Desde la PC de planta:

```bash
journalctl -u iot-celima-mqtt.service -o cat --no-pager \
  | grep -oP '^\[celima/data\] \K\{.*\}' > tests/data/celima_data_replay.jsonl
```

Graba al menos una hora. Dos avisos:

- Descarta las tramas sin `gatewayTime`, o el golden dependerá de la hora del
  servidor y la prueba será inestable.
- El journal trae las retransmisiones como líneas propias. Al derivar tasas
  aparecen como pares de ~30 s con delta 0, que bajan el percentil (lado
  conservador) pero acortan el intervalo del par siguiente. `derive_rates.py`
  no las filtra hoy.
- Tras sustituir el fixture hay que regenerar el golden (abajo), y conviene
  hacerlo con el binario **anterior** al cambio que estés validando.

## `celima_data_replay.golden`

Publicaciones esperadas, una por línea, en formato `<topic>\t<payload>`.

### Procedencia, y qué garantiza exactamente

El golden nació generado con el código de `main@0ac7a9c`, anterior al PR de
observabilidad, y durante PR 1 y PR 2 se mantuvo **byte a byte** idéntico: esa
era la evidencia de que ni la instrumentación ni la persistencia cambiaron un
solo valor publicado.

Desde PR 4 ya no es byte a byte ese golden: es ese mismo **más dos campos**
(`numero_grades_bajadas_turno` y `buffer_vacio_turno_s` en
`entrada_horno/production`). Lo que se sigue garantizando, y se comprobó campo a
campo al regenerarlo, es más preciso que "no cambia nada":

- mismo número de publicaciones y mismos tópicos;
- ningún campo desaparecido;
- **cero campos existentes con un valor distinto**.

Esa es la propiedad que hay que preservar. Un PR puede añadir campos —el
invariante del repo es añadir, nunca reinterpretar—, pero si al regenerar
aparece un campo existente con otro valor, el PR está mal y hay que investigar
antes de aceptar el golden nuevo.

Regenerar:

```bash
make test GOLDEN_OUT=tests/data/celima_data_replay.golden
```

Hazlo solo cuando el cambio de payload sea intencionado, o al cambiar el
fixture. Si lo regeneras con el mismo código que quieres validar, la prueba deja
de demostrar nada.
