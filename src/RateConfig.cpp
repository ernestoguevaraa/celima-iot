#include "RateConfig.hpp"

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <sstream>
#include <nlohmann/json.hpp>

namespace celima {

using json = nlohmann::json;

RateConfig& RateConfig::instance()
{
    static RateConfig cfg;
    return cfg;
}

static std::string config_path()
{
    if (const char* p = std::getenv("CELIMA_RATES_CONFIG"))
        if (p[0] != '\0') return p;
    return "/etc/iot-celima-mqtt/rates.json";
}

bool RateConfig::apply_json(const std::string& text, std::string& err)
{
    json j;
    try {
        // ignore_comments: este archivo lo edita alguien a mano en planta y la
        // plantilla que se instala documenta el formato en comentarios. Sin esto
        // el archivo se rechazaría y el servicio caería al valor por defecto en
        // silencio, que es justo el fallo que nadie nota.
        j = json::parse(text, nullptr, /*allow_exceptions=*/true, /*ignore_comments=*/true);
    } catch (const std::exception& e) {
        err = e.what();
        return false;
    }
    if (!j.is_object()) { err = "el documento no es un objeto"; return false; }

    double def = default_rate_per_h_;
    double mar = margin_;
    if (j.contains("default_rate_per_h") && j["default_rate_per_h"].is_number())
        def = j["default_rate_per_h"].get<double>();
    if (j.contains("margin") && j["margin"].is_number())
        mar = j["margin"].get<double>();
    if (def <= 0.0) { err = "default_rate_per_h <= 0"; return false; }
    if (mar <= 0.0) { err = "margin <= 0"; return false; }

    std::unordered_map<std::string, double> table;
    if (j.contains("lines") && j["lines"].is_object()) {
        for (auto& [line, machines] : j["lines"].items()) {
            if (!machines.is_object()) continue;
            for (auto& [machine, rate] : machines.items()) {
                if (!rate.is_number()) continue;
                const double r = rate.get<double>();
                if (r <= 0.0) continue;         // una tasa absurda no se hereda
                table[line + "/" + machine] = r;
            }
        }
    }

    default_rate_per_h_ = def;
    margin_ = mar;
    rates_per_h_ = std::move(table);
    return true;
}

bool RateConfig::load_file(const std::string& path)
{
    std::string err;
    std::ifstream in(path);
    if (!in) {
        err = "no se puede abrir";
    } else {
        std::ostringstream ss;
        ss << in.rdbuf();
        std::lock_guard<std::mutex> lock(mtx_);
        if (apply_json(ss.str(), err)) {
            loaded_ = true;
            std::cout << "[CONFIG] rates cargadas de " << path
                      << " (default_rate_per_h=" << default_rate_per_h_
                      << " margin=" << margin_
                      << " entradas=" << rates_per_h_.size() << ")\n";
            return true;
        }
    }

    std::cout << "[CONFIG] rates file not usable (" << err
              << "), using default_rate_per_h=" << default_rate_per_h_ << "\n";
    return false;
}

void RateConfig::load_from_env()
{
    {
        std::lock_guard<std::mutex> lock(mtx_);
        if (loaded_) return;
    }
    load_file(config_path());
}

void RateConfig::set_for_tests(double default_rate_per_h, double margin)
{
    std::lock_guard<std::mutex> lock(mtx_);
    default_rate_per_h_ = default_rate_per_h;
    margin_ = margin;
    loaded_ = true;
}

void RateConfig::set_rate_for_tests(int line, const std::string& proc, double rate_per_h)
{
    std::lock_guard<std::mutex> lock(mtx_);
    rates_per_h_[std::to_string(line) + "/" + proc] = rate_per_h;
    loaded_ = true;
}

void RateConfig::reset_for_tests()
{
    std::lock_guard<std::mutex> lock(mtx_);
    default_rate_per_h_ = 600.0;
    margin_ = 1.5;
    rates_per_h_.clear();
    loaded_ = false;
}

double RateConfig::rate_per_s(int line, const std::string& proc) const
{
    std::lock_guard<std::mutex> lock(mtx_);
    const auto it = rates_per_h_.find(std::to_string(line) + "/" + proc);
    const double per_h = (it != rates_per_h_.end()) ? it->second : default_rate_per_h_;
    return per_h / 3600.0;
}

double RateConfig::margin() const
{
    std::lock_guard<std::mutex> lock(mtx_);
    return margin_;
}

double RateConfig::default_rate_per_h() const
{
    std::lock_guard<std::mutex> lock(mtx_);
    return default_rate_per_h_;
}

} // namespace celima
