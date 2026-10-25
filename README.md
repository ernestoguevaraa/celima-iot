# celima-iot

Application for communication from Lorawan NS to Edge Server

## Installation

``` bash
sudo apt-get update
sudo apt-get install -y g++ make libpaho-mqttpp-dev libpaho-mqtt-dev nlohmann-json3-dev libsqlite3-dev
```

`libpaho-mqtt-dev` va explícito: en Debian Trixie (RPi, aarch64) `libpaho-mqttpp-dev` no la arrastra
como dependencia y la compilación falla con «MQTTAsync.h: No such file or directory».

## Tópicos publicados

Todo se publica con QoS 1 bajo `${ISA95_PREFIX}<lineID>/<estación>/…` (prefijo por defecto
`celima/punta_hermosa/planta/linea/`). `<estación>` es una de `prensa_hidraulica1`,
`prensa_hidraulica2`, `entrada_secador`, `salida_secador`, `esmalte`, `entrada_horno`,
`salida_horno` y `calidad`.

| Tópico | Origen | Contenido |
|---|---|---|
| `…/production` | tramas de producción (fPort 1, sin `msgType`) | acumulados de turno y contadores crudos |
| `…/alarms` | ídem, salvo Calidad | `alarms` y `timestamp_device` |
| `…/diagnostico` | firmware v6: `msgType` `arranque` (fPort 2) y `diag` (fPort 3) | ver abajo |

### `…/diagnostico` (desde 1.09.0)

Reporte de arranque del equipo:

```json
{"tipo":"arranque","causaReinicio":"WDT","causaReinicioCodigo":4,"joinsFallidos":2,
 "devEUI":"a8610a34324f740f","deviceName":"e6-shf","timestamp_device":"2026-10-25T15:15:53.000Z"}
```

`causaReinicio` es `POR`, `BOD`, `EXT`, `WDT`, `SYST` o `desconocida`.

Reporte de diagnóstico (`tipo` es el `tipoDiag` del equipo: `periodico` o `latido`):

```json
{"tipo":"periodico","glitches":13,"rechazosConteo":0,"rechazosIntegridad":4,"descartadas":2,
 "glitches_delta":3,"rechazosConteo_delta":0,"rechazosIntegridad_delta":3,"descartadas_delta":0,
 "devEUI":"a8610a34324f740f","deviceName":"e6-shf","timestamp_device":"2026-10-25T15:15:53.000Z"}
```

- Los cuatro contadores son acumulados uint16 desde el arranque del **equipo** y vuelven a 0 en
  cada reinicio. Los `_delta` son contra el diag anterior del mismo (línea, equipo).
- Los `_delta` van en `null` si no hay base: primer diag tras arrancar el servicio, primer diag
  tras un `arranque`, o un contador que bajó sin `arranque` de por medio (se trata como un reinicio
  cuyo reporte se perdió; estos contadores no llegan en la práctica a dar la vuelta en 65535).
- `devEUI` y `deviceName` se copian tal como llegan y se omiten si faltan.
- Un `latido` (el equipo late sin datos válidos) deja además `[STATE] alarma_sin_datos_validos` en
  el journal.

Lo consumirá el `boxer-patrol-edge-processor` cuando Patrol lo adapte; hasta entonces el tópico
solo se publica en el broker local.

## Orden de despliegue del firmware v6

**Primero este servicio (≥ 1.09.0), después el codec nuevo en el UG65.** Un servicio anterior
manda las tramas `arranque`/`diag` al procesador de producción de su `deviceType`, que lee como 0
los contadores que no trae: publicaría ceros como contadores crudos y metería un salto en los
acumuladores. El orden inverso es inocuo: sin codec nuevo no llega ningún `msgType`.

```bash
make && ./scripts/build_deb.sh      # 1.09.0 por defecto
```
