#include <iostream>
#include <tuple>
#include "common/HttpCurl.h"
#include "common/html.hpp"

using namespace std;

int main() {
    HttpCurl::CurlGlobalInit();
    HttpCurl ht;
    // ht.Get("https://www.bilibili.com/");
    ht.Get("https://www.bilibili.com/bangumi/play/ss34917?spm_id_from=333.337.0.0&from_spmid=666.25.series.0");
    // ht.Get("https://www.jisilu.cn/");
    if (ht.is_ok()) {
        string page = ht.get_page();
        // cout << ht.get_page() << endl;

        html::parser p;
        html::node_ptr node = p.parse(page);
        std::cout << node->to_html(' ', true) << std::endl;
    } else {
        cout << ht.get_code() << endl;
    }

    HttpCurl::CurlGlobalCleanUp();
    return 0;
}