#include <common/web_utils.h>
#include <catch.hpp>
#include <iostream>

using namespace std;

TEST_CASE("replace_js_script", "[replace_js_script]") {
    string t1 = "a<script> </script>b c<script></script>d";
    string t2 = "a                  b c                 d";
    REQUIRE(replace_js_script(t1) == t2);
}
