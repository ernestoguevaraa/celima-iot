#!/usr/bin/env bash
set -euo pipefail

APP=iot-celima-mqtt
VERSION=${1:-1.06.0}
ARCH=$(dpkg --print-architecture)
PKGDIR=./_pkg
ROOT=$PKGDIR/${APP}_${VERSION}_${ARCH}
BIN=bin/Release/$APP

if [ ! -x "$BIN" ]; then
  echo "Binary $BIN not found. Run 'make release' first." >&2
  exit 1
fi

rm -rf "$PKGDIR"
mkdir -p "$ROOT/DEBIAN"
mkdir -p "$ROOT/usr/local/bin"
mkdir -p "$ROOT/etc/default"
mkdir -p "$ROOT/etc/iot-celima-mqtt"
mkdir -p "$ROOT/etc/systemd/system"

# Control file
cat > "$ROOT/DEBIAN/control" <<EOF
Package: $APP
Version: $VERSION
Section: utils
Priority: optional
Architecture: $ARCH
Maintainer: Celima IoT <iot@example.com>
Description: Celima MQTT Integration (Paho C++). Subscribes to celima/* and publishes ISA-95 topics.
Depends: libpaho-mqttpp3-1 (>= 1.2), libpaho-mqtt1.3 (>= 1.3), libsqlite3-0
EOF

# Postinst (enable + start)
cat > "$ROOT/DEBIAN/postinst" <<'EOF'
#!/bin/sh
set -e
# Configuración: se instala solo si no existe, para no pisar lo editado en
# planta. Ni defaults.env ni rates.json son conffiles de dpkg; la plantilla
# de referencia vive en /usr/share/iot-celima-mqtt/.
if [ ! -f /etc/default/iot-celima-mqtt ]; then
  cp -n /usr/share/iot-celima-mqtt/defaults.env /etc/default/iot-celima-mqtt || true
fi
mkdir -p /etc/iot-celima-mqtt
if [ ! -f /etc/iot-celima-mqtt/rates.json ]; then
  cp -n /usr/share/iot-celima-mqtt/rates.json /etc/iot-celima-mqtt/rates.json || true
fi
# /var/lib/iot-celima-mqtt (state.db) NO se empaqueta: es estado de runtime, no
# un artefacto de despliegue. Lo crea systemd por el StateDirectory= de la
# unidad, con los permisos correctos. Si estuviera en el .deb, dpkg sería su
# dueño y una actualización o un purge podría llevarse totales de producción.
systemctl daemon-reload || true
systemctl enable iot-celima-mqtt.service || true
# Don't start automatically on containerized builds:
if [ -d /run/systemd/system ]; then
  systemctl try-restart iot-celima-mqtt.service || true
fi
exit 0
EOF
chmod 0755 "$ROOT/DEBIAN/postinst"

# Prerm (stop before remove)
cat > "$ROOT/DEBIAN/prerm" <<'EOF'
#!/bin/sh
set -e
if systemctl is-active --quiet iot-celima-mqtt.service; then
  systemctl stop iot-celima-mqtt.service || true
fi
exit 0
EOF
chmod 0755 "$ROOT/DEBIAN/prerm"

# Postrm (limpieza en remove y en purge)
#
# La distinción importa y es la de siempre en Debian:
#   remove → se va el binario, se queda la configuración y se queda el estado.
#            Es lo que pasa en una reinstalación o al pasar de versión.
#   purge  → se va todo, incluida la base de acumuladores de turno.
#
# ATENCIÓN: el purge borra /var/lib/iot-celima-mqtt/state.db, que contiene los
# totales del turno en curso. No hay copia en ningún otro sitio: InfluxDB solo
# guarda un subconjunto de lo publicado y el pipeline hacia AWS no admite
# reexpresar datos ya enviados. Purgar a mitad de turno pierde ese turno.
cat > "$ROOT/DEBIAN/postrm" <<'EOF'
#!/bin/sh
set -e

case "$1" in
  purge)
    rm -f /etc/default/iot-celima-mqtt
    rm -f /etc/iot-celima-mqtt/rates.json
    rmdir /etc/iot-celima-mqtt 2>/dev/null || true
    # Estado de runtime: acumuladores de turno y ficheros WAL de SQLite.
    rm -rf /var/lib/iot-celima-mqtt
    systemctl daemon-reload || true
    ;;

  remove)
    # Ni configuración ni estado: un remove debe poder deshacerse instalando
    # otra vez y seguir contando donde estaba.
    systemctl disable iot-celima-mqtt.service 2>/dev/null || true
    systemctl daemon-reload || true
    ;;

  upgrade|failed-upgrade|abort-install|abort-upgrade|disappear)
    ;;
esac

exit 0
EOF
chmod 0755 "$ROOT/DEBIAN/postrm"

# Files
install -m 0755 "$BIN" "$ROOT/usr/local/bin/$APP"

# Plantillas de configuración en /usr/share; el postinst las copia a /etc solo
# si faltan.
mkdir -p "$ROOT/usr/share/iot-celima-mqtt"
install -m 0644 packaging/defaults.env "$ROOT/usr/share/iot-celima-mqtt/defaults.env"
install -m 0644 packaging/rates.json   "$ROOT/usr/share/iot-celima-mqtt/rates.json"

# Systemd unit
install -m 0644 packaging/iot-celima-mqtt.service "$ROOT/etc/systemd/system/iot-celima-mqtt.service"

# Build the deb
DEB="$PKGDIR/${APP}_${VERSION}_${ARCH}.deb"
dpkg-deb --build "$ROOT" "$DEB"
echo "Built package: $DEB"
echo "Install with: sudo dpkg -i $DEB && sudo systemctl status iot-celima-mqtt"
