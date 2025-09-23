#include "JsonUtils.hpp"

namespace jsonu {

std::optional<json> parse(const std::string& s, std::string& err) {
    try {
        auto j = json::parse(s);
        return j;
    } catch (const std::exception& e) {
        err = e.what();
        return std::nullopt;
    }
}

} // namespace jsonu
