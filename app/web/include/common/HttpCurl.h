#pragma once

#include <curl/curl.h>
#include <string>

class HttpCurl {
public:
    static CURLcode CurlGlobalInit() {
        CURLcode m_curlCode = curl_global_init(CURL_GLOBAL_ALL);
        return m_curlCode;
    }
    static void CurlGlobalCleanUp() { curl_global_cleanup(); }

    CURLcode Post(const std::string& url, const std::string& fileds);
    CURLcode Get(const std::string& url);
    void reset() {
        m_curlCode = CURLE_OK;
        m_page.clear();
    }
    bool is_ok() { return m_curlCode == CURLE_OK; }
    CURLcode get_code() { return m_curlCode; }
    void SetExtra(void* extra) { m_extra = extra; }
    void* GetExtra() { return m_extra; }
    std::string get_page() const;
    void append_page(const std::string& msg);

private:
    std::string m_page;
    CURLcode m_curlCode = CURLE_OK;
    CURL* m_curl{nullptr};
    void* m_extra{nullptr};
};
