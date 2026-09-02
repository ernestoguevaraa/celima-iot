# Datos del test de replay

## `celima_data_replay.jsonl`

**Es un fixture sintético, no tráfico de planta.** Se generó con semilla fija
(`20260902`) porque este repo no tiene acceso al journal de la PC industrial.
Cubre lo que pide la spec —los 8 `deviceType` en 4 líneas, 20 intervalos de
180 s ≈ 1 h por combinación, 641 tramas— y además fuerza a propósito tres casos:

| Caso | Dónde |
|---|---|
| Rollover de 16 bits | contadores que arrancan cerca de 65.536 en varias líneas |
| Salto > `max_valid` (rechazo + re-ancla) | `metrica_mds_cantidad`, L2 / `deviceType` 4, intervalos 7–9 |
| Trama repetida por el NS | L1 / `deviceType` 3, intervalo 3 (duplicada) |

Todas las tramas llevan `gatewayTime`, para que `device_timestamp()` no caiga a
la hora del servidor y el replay sea determinista.

### Sustituirlo por tráfico real

Es lo que pide la spec y mejora la prueba. Desde la PC de planta:

```bash
journalctl -u iot-celima-mqtt.service -o cat --no-pager \
  | grep -oP '^\[celima/data\] \K\{.*\}' > tests/data/celima_data_replay.jsonl
```

Graba al menos una hora. Dos avisos:

- Descarta las tramas sin `gatewayTime`, o el golden dependerá de la hora del
  servidor y la prueba será inestable.
- Tras sustituir el fixture hay que regenerar el golden (abajo), y conviene
  hacerlo con el binario **anterior** al cambio que estés validando.

## `celima_data_replay.golden`

Publicaciones esperadas, una por línea, en formato `<topic>\t<payload>`.

El golden que está en el repo se generó con el código de `main@0ac7a9c`, es
decir **antes** del PR de observabilidad: es la evidencia de que añadir los
eventos `[STATE]` no cambió ningún valor publicado (criterio de aceptación 1).

Regenerar:

```bash
make test GOLDEN_OUT=tests/data/celima_data_replay.golden
```

Hazlo solo cuando el cambio de payload sea intencionado, o al cambiar el
fixture. Si lo regeneras con el mismo código que quieres validar, la prueba deja
de demostrar nada.
