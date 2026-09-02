#include "StateStore.hpp"

#include <iostream>
#include <sqlite3.h>

namespace celima {

namespace {

const char* kSchema =
    "CREATE TABLE IF NOT EXISTS processor_state ("
    "  proc        TEXT    NOT NULL,"
    "  line        INTEGER NOT NULL,"
    "  shift       INTEGER NOT NULL,"
    "  updated_at  INTEGER NOT NULL,"   // epoch en segundos, UTC
    "  state_json  TEXT    NOT NULL,"
    "  PRIMARY KEY (proc, line)"
    ");";

class SqliteStateStore final : public IStateStore {
public:
    explicit SqliteStateStore(sqlite3* db) : db_(db) {}
    ~SqliteStateStore() override { if (db_) sqlite3_close(db_); }

    bool load(const std::string& proc, int line, StoredState& out) override
    {
        static const char* sql =
            "SELECT shift, updated_at, state_json FROM processor_state "
            "WHERE proc = ? AND line = ?;";

        sqlite3_stmt* st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) {
            warn_once("prepare(load)", sqlite3_errmsg(db_));
            return false;
        }
        sqlite3_bind_text(st, 1, proc.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 2, line);

        bool ok = false;
        if (sqlite3_step(st) == SQLITE_ROW) {
            const int   shift      = sqlite3_column_int(st, 0);
            const auto  updated_at = static_cast<int64_t>(sqlite3_column_int64(st, 1));
            const char* json_text  = reinterpret_cast<const char*>(sqlite3_column_text(st, 2));

            // Una fila ilegible se trata como inexistente: nunca se repara en
            // caliente ni se interpreta a medias.
            try {
                nlohmann::json j = nlohmann::json::parse(json_text ? json_text : "");
                const int v = j.value("v", 0);
                if (v > 0 && v <= kStateSchemaVersion) {
                    out.shift = shift;
                    out.updated_at = updated_at;
                    out.state = std::move(j);
                    ok = true;
                } else {
                    std::cout << "[STATE] stored_state_ignored proc=" << proc
                              << " line=" << line << " reason=schema_version v=" << v
                              << " supported=" << kStateSchemaVersion << "\n";
                }
            } catch (const std::exception& e) {
                std::cout << "[STATE] stored_state_ignored proc=" << proc
                          << " line=" << line << " reason=corrupt_json (" << e.what() << ")\n";
            }
        }
        sqlite3_finalize(st);
        return ok;
    }

    bool save(const std::string& proc, int line,
              int shift, int64_t updated_at, const nlohmann::json& state) override
    {
        static const char* sql =
            "INSERT INTO processor_state (proc, line, shift, updated_at, state_json) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(proc, line) DO UPDATE SET "
            "  shift = excluded.shift,"
            "  updated_at = excluded.updated_at,"
            "  state_json = excluded.state_json;";

        sqlite3_stmt* st = nullptr;
        if (sqlite3_prepare_v2(db_, sql, -1, &st, nullptr) != SQLITE_OK) {
            warn_once("prepare(save)", sqlite3_errmsg(db_));
            return false;
        }
        const std::string text = state.dump();
        sqlite3_bind_text(st, 1, proc.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 2, line);
        sqlite3_bind_int(st, 3, shift);
        sqlite3_bind_int64(st, 4, static_cast<sqlite3_int64>(updated_at));
        sqlite3_bind_text(st, 5, text.c_str(), -1, SQLITE_TRANSIENT);

        const int rc = sqlite3_step(st);
        sqlite3_finalize(st);
        if (rc != SQLITE_DONE) {
            // Nunca reintentar en bucle ni bloquear la ingesta.
            warn_once("save", sqlite3_errmsg(db_));
            return false;
        }
        return true;
    }

private:
    // Supresión de repetidos: un fallo persistente no debe inundar el journal.
    void warn_once(const char* op, const char* msg)
        {
        const std::string key = std::string(op) + ": " + (msg ? msg : "?");
        if (key == last_warn_) {
            ++suppressed_;
            return;
        }
        if (suppressed_ > 0)
            std::cout << "[STATE] store_error_suppressed count=" << suppressed_ << "\n";
        suppressed_ = 0;
        last_warn_ = key;
        std::cout << "[STATE] store_error op=" << op << " msg=" << (msg ? msg : "?") << "\n";
    }

    sqlite3* db_ = nullptr;
    std::string last_warn_;
    long suppressed_ = 0;
};

} // namespace

std::unique_ptr<IStateStore> make_sqlite_state_store(const std::string& path)
{
    sqlite3* db = nullptr;
    const int rc = sqlite3_open_v2(path.c_str(), &db,
                                   SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr);
    if (rc != SQLITE_OK) {
        std::cout << "[CONFIG] state db not usable (" << (db ? sqlite3_errmsg(db) : "open failed")
                  << "), continuando solo en memoria\n";
        if (db) sqlite3_close(db);
        return nullptr;
    }

    char* err = nullptr;
    // WAL + synchronous=FULL: a 0,42 msg/s el costo es irrelevante y el
    // escenario que importa es el corte sin cierre limpio.
    const char* pragmas =
        "PRAGMA journal_mode = WAL;"
        "PRAGMA synchronous = FULL;"
        "PRAGMA busy_timeout = 2000;";
    if (sqlite3_exec(db, pragmas, nullptr, nullptr, &err) != SQLITE_OK) {
        std::cout << "[CONFIG] state db pragmas fallaron (" << (err ? err : "?") << ")\n";
        sqlite3_free(err);
        err = nullptr;
    }

    if (sqlite3_exec(db, kSchema, nullptr, nullptr, &err) != SQLITE_OK) {
        // Base corrupta o ilegible: tratarla como inexistente.
        std::cout << "[CONFIG] state db not usable (" << (err ? err : "?")
                  << "), continuando solo en memoria\n";
        sqlite3_free(err);
        sqlite3_close(db);
        return nullptr;
    }

    std::cout << "[CONFIG] persistencia de estado en " << path << "\n";
    return std::make_unique<SqliteStateStore>(db);
}

} // namespace celima
