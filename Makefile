# Simple Makefile for iot-celima-mqtt
# Requires:
#   - g++ with C++20
#   - libpaho-mqttpp3, libpaho-mqtt3a
#   - nlohmann-json (header-only; here we vendor single include in inc/)

APP_NAME := iot-celima-mqtt
CXX := g++
CXXFLAGS := -std=gnu++20 -O2 -Wall -Wextra -Wpedantic -Iinc
LDFLAGS := -lpaho-mqttpp3 -lpaho-mqtt3a -pthread

SRC := $(wildcard src/*.cpp)
OBJ := $(SRC:.cpp=.o)
BIN := bin/$(APP_NAME)

all: $(BIN)

bin:
	mkdir -p bin

$(BIN): bin $(OBJ)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJ) $(LDFLAGS)

clean:
	rm -f $(OBJ) $(BIN)

format:
	clang-format -i inc/*.hpp src/*.cpp || true

run: $(BIN)
	MQTT_BROKER=tcp://localhost:1883 ISA95_PREFIX=enterprise/site/area/line1 ./$(BIN)

.PHONY: all clean format run
