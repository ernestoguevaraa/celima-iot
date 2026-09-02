#include "StateStore.hpp"

#include <cstdlib>
#include <iostream>

namespace celima {

static std::string key_of(const std::string& proc, int line)
{
    return proc + "/" + std::to_string(line);
}

// ---------------------------------------------------------------------------
// MemoryStateStore

bool MemoryStateStore::load(const std::string& proc, int line, StoredState& out)
{
    std::lock_guard<std::mutex> lock(mtx_);
    const auto it = rows_.find(key_of(proc, line));
    if (it == rows_.end()) return false;
    out = it->second;
    return true;
}

bool MemoryStateStore::save(const std::string& proc, int line,
                            int shift, int64_t updated_at, const nlohmann::json& st)
{
    std::lock_guard<std::mutex> lock(mtx_);
    StoredState row;
    row.shift = shift;
    row.updated_at = updated_at;
    row.state = st;
    rows_[key_of(proc, line)] = std::move(row);
    ++saves_;
    return true;
}

size_t MemoryStateStore::size() const
{
    std::lock_guard<std::mutex> lock(mtx_);
    return rows_.size();
}

void MemoryStateStore::clear()
{
    std::lock_guard<std::mutex> lock(mtx_);
    rows_.clear();
    saves_ = 0;
}

// ---------------------------------------------------------------------------
// Store global

namespace {
std::mutex g_store_mtx;
std::unique_ptr<IStateStore> g_store;
}

IStateStore* state_store()
{
    std::lock_guard<std::mutex> lock(g_store_mtx);
    return g_store.get();
}

void set_state_store(std::unique_ptr<IStateStore> store)
{
    std::lock_guard<std::mutex> lock(g_store_mtx);
    g_store = std::move(store);
}

void init_state_store_from_env()
{
    if (const char* off = std::getenv("CELIMA_STATE_PERSISTENCE")) {
        if (off[0] == '0') {
            std::cout << "[CONFIG] persistencia de estado DESACTIVADA "
                         "(CELIMA_STATE_PERSISTENCE=0)\n";
            set_state_store(nullptr);
            return;
        }
    }

    std::string path = "/var/lib/iot-celima-mqtt/state.db";
    if (const char* p = std::getenv("CELIMA_STATE_DB"))
        if (p[0] != '\0') path = p;

    // make_sqlite_state_store() ya registra el motivo si no puede abrir, y
    // devuelve nullptr; en ese caso el servicio sigue solo en memoria.
    set_state_store(make_sqlite_state_store(path));
}

int gap_short_seconds()
{
    static const int v = [] {
        if (const char* s = std::getenv("CELIMA_GAP_SHORT_S")) {
            const int n = std::atoi(s);
            if (n > 0) return n;
        }
        return 900;
    }();
    return v;
}

} // namespace celima
