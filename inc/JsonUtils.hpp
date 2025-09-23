#pragma once
#include <string>
#include <optional>
#include <nlohmann/json.hpp>

namespace jsonu {

using json = nlohmann::json;

std::optional<json> parse(const std::string& s, std::string& err);

template <typename T>
std::optional<T> get_opt(const json& j, const char* key) {
    if (!j.contains(key)) return std::nullopt;
    try {
        return j.at(key).get<T>();
    } catch (...) {
        return std::nullopt;
    }
}

} // namespace jsonu
