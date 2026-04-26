#include "src/common/types.h"
#include <iostream>

int main() {
    minidb::Value v{minidb::DataType::INTEGER};
    v.data.int_val = 42;
    std::cout << "Test value: " << v.data.int_val << std::endl;
    return 0;
}
