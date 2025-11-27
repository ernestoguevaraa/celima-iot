#!/usr/bin/env bash
set -euo pipefail

APP=iot-celima-mqtt
VERSION=${1:-0.7.0}
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
Depends: libpaho-mqttpp3-1 (>= 1.2), libpaho-mqtt1.3 (>= 1.3)
EOF

# Postinst (enable + start)
cat > "$ROOT/DEBIAN/postinst" <<'EOF'
#!/bin/sh
set -e
# Install defaults if missing
if [ ! -f /etc/default/iot-celima-mqtt ]; then
  cp -n /usr/share/iot-celima-mqtt/defaults.env /etc/default/iot-celima-mqtt || true
fi
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

# Files
install -m 0755 "$BIN" "$ROOT/usr/local/bin/$APP"

# Ship defaults in /usr/share and copy to /etc/default on postinst
mkdir -p "$ROOT/usr/share/iot-celima-mqtt"
install -m 0644 packaging/defaults.env "$ROOT/usr/share/iot-celima-mqtt/defaults.env"

# Systemd unit
install -m 0644 packaging/iot-celima-mqtt.service "$ROOT/etc/systemd/system/iot-celima-mqtt.service"

# Build the deb
DEB="$PKGDIR/${APP}_${VERSION}_${ARCH}.deb"
dpkg-deb --build "$ROOT" "$DEB"
echo "Built package: $DEB"
echo "Install with: sudo dpkg -i $DEB && sudo systemctl status iot-celima-mqtt"
