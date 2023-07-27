#include <common/web_utils.h>
#include <zerg_string.h>

std::string replace_solidus_character(const std::string& str) {
    auto ret = ztool::ReplaceAllCopy(str, "\\U002F", "/");
    ztool::ReplaceAll(ret, "\\U002f", "/");
    ztool::ReplaceAll(ret, "\\u002F", "/");
    ztool::ReplaceAll(ret, "\\u002f", "/");
    return ret;
}

static std::vector<size_t> find_all_pos(const std::string& str, const std::string& pattern) {
    std::vector<size_t> ret;
    auto start_pos = str.find(pattern);
    while (start_pos != std::string::npos) {
        ret.push_back(start_pos);
        start_pos = str.find(pattern, start_pos + pattern.length());
    }
    return ret;
}

std::string replace_js_script(const std::string& str) {
    const char* const end_script = "/script>";
    std::vector<size_t> pos0 = find_all_pos(str, "<script");
    std::vector<size_t> pos1 = find_all_pos(str, end_script);
    if (pos0.size() != pos1.size()) throw std::runtime_error("replace_js_script size differ");

    string ret = str;
    for (size_t i = 0; i < pos0.size(); ++i) {
        size_t p0 = pos0[i];
        size_t p1 = pos1[i] + strlen(end_script);
        std::fill(ret.begin() + p0, ret.begin() + p1, ' ');
    }
    return ret;
}