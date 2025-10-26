# Makefile — iot-celima-mqtt (Release + Debug)

APP_NAME := iot-celima-mqtt
CXX := g++

# Common flags
CXXSTD := -std=gnu++20
WARN   := -Wall -Wextra -Wpedantic
INC    := -Iinc

# Build variants
CXXFLAGS_REL := $(CXXSTD) $(WARN) $(INC) -O2 -DNDEBUG
CXXFLAGS_DBG := $(CXXSTD) $(WARN) $(INC) -O0 -g3 -DDEBUG

# Optional sanitizers: SAN=address | ubsan | thread
SAN ?=
ifeq ($(SAN),address)
  SANFLAGS := -fsanitize=address -fno-omit-frame-pointer
else ifeq ($(SAN),ubsan)
  SANFLAGS := -fsanitize=undefined
else ifeq ($(SAN),thread)
  SANFLAGS := -fsanitize=thread
else
  SANFLAGS :=
endif

# Link
LDFLAGS := -lpaho-mqttpp3 -lpaho-mqtt3a -pthread $(SANFLAGS)

# Sources / objects
SRC      := $(wildcard src/*.cpp)
OBJ_REL  := $(patsubst src/%.cpp,build/Release/%.o,$(SRC))
OBJ_DBG  := $(patsubst src/%.cpp,build/Debug/%.o,$(SRC))

# Binaries
BINDIR_REL := bin/Release
BINDIR_DBG := bin/Debug
BIN_REL    := $(BINDIR_REL)/$(APP_NAME)
BIN_DBG    := $(BINDIR_DBG)/$(APP_NAME)

# Default target
all: release

# --- Build rules ---

release: $(BIN_REL)
	@echo "✅ Release built: $(BIN_REL)"

debug: $(BIN_DBG)
	@echo "🐞 Debug built:   $(BIN_DBG)"

$(BIN_REL): $(OBJ_REL)
	@mkdir -p $(BINDIR_REL)
	$(CXX) -o $@ $^ $(LDFLAGS)

$(BIN_DBG): $(OBJ_DBG)
	@mkdir -p $(BINDIR_DBG)
	$(CXX) -o $@ $^ $(LDFLAGS)

build/Release/%.o: src/%.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS_REL) $(SANFLAGS) -c $< -o $@

build/Debug/%.o: src/%.cpp
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS_DBG) $(SANFLAGS) -c $< -o $@

# --- Utilities ---

strip:
	strip $(BIN_REL)

run-release: release
	MQTT_BROKER=tcp://localhost:1883 ISA95_PREFIX=enterprise/site/area/line1 ./$(BIN_REL)

run-debug: debug
	MQTT_BROKER=tcp://localhost:1883 ISA95_PREFIX=enterprise/site/area/line1 ./$(BIN_DBG)

format:
	clang-format -i inc/*.hpp src/*.cpp || true

clean:
	rm -rf build bin

.PHONY: all release debug strip run-release run-debug format clean
