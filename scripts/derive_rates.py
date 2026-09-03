#!/usr/bin/env python3
"""
Deriva tasas máximas por línea y máquina para rates.json de celima-iot,
a partir de tramas crudas del journal.

Entrada: un fichero con una trama JSON por línea, en el formato de `celima/data`.
Se extrae del journal de planta así:

    journalctl -u iot-celima-mqtt.service -o cat --no-pager --since "-30 days" \
      | grep -oP '^\\[celima/data\\] \\K\\{.*\\}' > tramas.jsonl

    python3 derive_rates.py tramas.jsonl [max_intervalo_s]

CÓMO SE TRATAN LOS CORTES DE ENERGÍA
------------------------------------
Un periodo de 30 días contiene cortes: huecos de horas en los que la planta
produjo y nadie escuchó. El par de tramas que cruza uno de esos huecos es
veneno para esta medición, por dos motivos:

  1. `timer1Hz` es un contador de 16 bits a 1 Hz: da la vuelta cada 65.536 s
     (18,2 h). En un hueco de 26 h mide 28.244 s en lugar de 93.600. No sirve
     como reloj para huecos largos — solo para intervalos normales.
  2. El delta acumulado en el hueco es enorme y, si se divide por el intervalo
     nominal, produce una tasa cientos de veces mayor que la real. Medido: un
     corte de 26 h inflaba la tasa 520x.

Y una tasa inflada es el error PELIGROSO: agranda la cota de plausibilidad,
que es justo lo que permite que un delta absurdo se sume como producción real.

Por eso este script:
  · calcula el intervalo con `gatewayTime` (marca absoluta del gateway, no da
    la vuelta), nunca con `timer1Hz`;
  · descarta todo par cuyo intervalo supere `max_intervalo_s` (600 s por
    defecto: el nominal son ~180 s, así que eso tolera hasta dos tramas
    perdidas seguidas);
  · informa cuántos pares descartó y de qué duración, para que los cortes se
    vean en lugar de desaparecer en silencio.

Cuanto más largo el periodo, mejor: el objetivo es un p99 con miles de
muestras, no con veinte. Con menos de ~200 intervalos por máquina el script
avisa y el resultado no debe tomarse como definitivo.

REGLA: ante la duda, valor BAJO. Tasa baja -> cota baja -> más descartes ->
total subcontado, que es el lado seguro del error.
"""
import sys, json, collections, statistics
from datetime import datetime

MACH = {1: "prensa_hidraulica1", 2: "prensa_hidraulica2", 3: "entrada_secador",
        4: "salida_secador", 5: "esmalte", 6: "entrada_horno", 7: "salida_horno",
        8: "calidad"}

META = {"lineID", "deviceType", "applicationID", "checksum", "alarms", "devEUI",
        "deviceName", "gatewayTime", "flagsRaw", "freshBoot", "reserved1",
        "reserved2", "reserved3"}

MAX_INTERVALO_S = 600.0     # por defecto; primer argumento opcional lo cambia
CAP_DELTA = 20000           # por encima de esto es corrupción, no producción


def familia(campo):
    """Los contadores de TIEMPO no se miden: su máximo es analítico.
       *_tiempo_s -> 1 tick/s -> 3600 u/h · *_tiempo_ds -> 10 tick/s -> 36000 u/h"""
    c = campo.lower()
    if c == "timer1hz":
        return "reloj"
    if c.endswith("_ds") or "_tiempo_ds" in c:
        return "tiempo_ds"
    if c.endswith("_s") or "_tiempo" in c or "_tempo" in c:
        return "tiempo_s"
    return "evento"


def d16(curr, prev):
    return (curr - prev) & 0xFFFF


def epoch(r):
    """Segundos desde época a partir de gatewayTime. None si falta o no parsea."""
    gw = r.get("gatewayTime")
    if not gw:
        return None
    try:
        return datetime.fromisoformat(gw).timestamp()
    except ValueError:
        return None


def main(path, max_int):
    rows = []
    for line in open(path):
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue

    by = collections.defaultdict(list)
    sin_ts = 0
    for r in rows:
        ln, dt_ = r.get("lineID"), r.get("deviceType")
        if ln is None or dt_ is None:
            continue
        if epoch(r) is None:
            sin_ts += 1
            continue
        by[(ln, dt_)].append(r)

    if sin_ts:
        print(f"AVISO: {sin_ts} tramas sin gatewayTime utilizable, descartadas.\n")

    resultado = {}
    huecos_global = []

    print(f"{'línea/máquina':32s} {'campo':28s} {'n':>6s} {'p50':>8s} {'p99':>8s} {'máx':>9s}")
    print("-" * 96)

    for (ln, dt_) in sorted(by):
        mach = MACH.get(dt_, f"dev{dt_}")
        rs = sorted(by[(ln, dt_)], key=epoch)

        # Deduplicar tramas repetidas: el network server las republica.
        vistos, limpio = set(), []
        for r in rs:
            k = r.get("timer1Hz")
            if k is not None and k in vistos:
                continue
            if k is not None:
                vistos.add(k)
            limpio.append(r)

        # Pares válidos = intervalo medido con gatewayTime y por debajo del tope.
        pares, huecos = [], []
        for a, b in zip(limpio, limpio[1:]):
            dt_s = epoch(b) - epoch(a)
            if dt_s <= 0:
                continue
            if dt_s > max_int:
                huecos.append(dt_s)
                continue
            pares.append((a, b, dt_s))

        if huecos:
            huecos_global.append((ln, mach, huecos))

        campos = [k for k, v in limpio[0].items()
                  if isinstance(v, int) and k not in META] if limpio else []

        mejor = 0.0
        for f in campos:
            if familia(f) != "evento":
                continue
            tasas = []
            for a, b, dt_s in pares:
                if f not in a or f not in b:
                    continue
                delta = d16(b[f], a[f])
                if delta > CAP_DELTA:
                    continue
                tasas.append(delta / dt_s * 3600.0)
            if len(tasas) < 3:
                continue
            tasas.sort()
            p50 = statistics.median(tasas)
            p99 = tasas[min(len(tasas) - 1, int(round(0.99 * (len(tasas) - 1))))]
            aviso = "  ⚠ pocas muestras" if len(tasas) < 200 else ""
            print(f"{f'L{ln} / {mach}':32s} {f:28s} {len(tasas):6d} "
                  f"{p50:8.0f} {p99:8.0f} {max(tasas):9.0f}{aviso}")
            mejor = max(mejor, p99)

        if mejor > 0:
            resultado.setdefault(str(ln), {})[mach] = int(round(mejor / 50) * 50)

    if huecos_global:
        print()
        print(f"HUECOS DESCARTADOS (intervalo > {max_int:.0f} s)")
        print("Cada uno es una ventana en la que la planta pudo producir sin que nadie escuchara.")
        for ln, mach, hs in huecos_global:
            largos = sorted((h for h in hs if h > 3600), reverse=True)
            print(f"  L{ln} / {mach:22s} {len(hs):4d} huecos"
                  + (f" · los mayores: " + ", ".join(f"{h/3600:.1f} h" for h in largos[:4]) if largos else ""))

    print()
    print("Propuesta para el bloque 'lines' de rates.json")
    print("(solo contadores de EVENTO; los de tiempo se acotan analíticamente)")
    print(json.dumps({"lines": resultado}, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    if len(sys.argv) not in (2, 3):
        sys.exit(__doc__)
    main(sys.argv[1], float(sys.argv[2]) if len(sys.argv) == 3 else MAX_INTERVALO_S)
