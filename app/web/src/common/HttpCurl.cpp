#include <common/HttpCurl.h>
#include <common/web_utils.h>

void HttpCurl::append_page(const std::string& msg) { m_page += msg; }

std::string HttpCurl::get_page() const { return replace_solidus_character(m_page); }

static size_t rev_data_callback(void* ptr, size_t size, size_t nmemb, void* stream) {
    HttpCurl* pHttp = (HttpCurl*)stream;
    size_t len = size * nmemb;
    std::string response;
    if (ptr && len) {
        response.assign((char*)ptr, len);
    }
    pHttp->append_page(response);
    // printf("rev_data_callback %zu %zu\n", size, nmemb);
    return size * nmemb;
}

CURLcode HttpCurl::Post(const std::string& url, const std::string& fileds) {
    reset();
    m_curl = curl_easy_init();
    curl_easy_setopt(m_curl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(m_curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(m_curl, CURLOPT_POSTFIELDS, fileds.c_str());
    if (url.find("https://") != std::string::npos) {
        curl_easy_setopt(m_curl, CURLOPT_SSL_VERIFYPEER, 0L);
        curl_easy_setopt(m_curl, CURLOPT_SSL_VERIFYHOST, 0L);
    }
    curl_easy_setopt(m_curl, CURLOPT_TIMEOUT, 3L);
    curl_easy_setopt(m_curl, CURLOPT_CONNECTTIMEOUT, 3L);
    curl_easy_setopt(m_curl, CURLOPT_WRITEFUNCTION, rev_data_callback);
    curl_easy_setopt(m_curl, CURLOPT_WRITEDATA, this);
    m_curlCode = curl_easy_perform(m_curl);
    curl_easy_cleanup(m_curl);
    return m_curlCode;
}

CURLcode HttpCurl::Get(const std::string& url) {
    reset();
    m_curl = curl_easy_init();
    curl_easy_setopt(m_curl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(m_curl, CURLOPT_URL, url.c_str());
    if (url.find("https://") != std::string::npos) {
        curl_easy_setopt(m_curl, CURLOPT_SSL_VERIFYPEER, 0L);
        curl_easy_setopt(m_curl, CURLOPT_SSL_VERIFYHOST, 0L);
    }
    curl_easy_setopt(m_curl, CURLOPT_TIMEOUT, 3L);
    curl_easy_setopt(m_curl, CURLOPT_CONNECTTIMEOUT, 3L);
    curl_easy_setopt(m_curl, CURLOPT_WRITEFUNCTION, rev_data_callback);
    curl_easy_setopt(m_curl, CURLOPT_WRITEDATA, this);
    m_curlCode = curl_easy_perform(m_curl);
    curl_easy_cleanup(m_curl);
    return m_curlCode;
}