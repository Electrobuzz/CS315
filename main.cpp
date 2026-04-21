#include "src/cli/cli.h"
#include <iostream>

using namespace minidb;

int main(int argc, char* argv[]) {
    bool debug = false;
    std::string db_file = "minidb.db";
    std::string log_file = "minidb.log";
    
    // Parse command line arguments
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--debug") {
            debug = true;
        } else if (arg == "--db" && i + 1 < argc) {
            db_file = argv[++i];
        } else if (arg == "--log" && i + 1 < argc) {
            log_file = argv[++i];
        } else if (arg == "--help" || arg == "-h") {
            std::cout << "MiniDB SQL Shell" << std::endl;
            std::cout << "Usage: minidb [options]" << std::endl;
            std::cout << "Options:" << std::endl;
            std::cout << "  --debug    Enable debug logging" << std::endl;
            std::cout << "  --db FILE  Database file (default: minidb.db)" << std::endl;
            std::cout << "  --log FILE Log file (default: minidb.log)" << std::endl;
            std::cout << "  --help     Show this help message" << std::endl;
            return 0;
        }
    }
    
    try {
        DatabaseCLI cli(db_file, log_file, debug);
        cli.Run();
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
