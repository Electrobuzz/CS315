#include "parser.h"
#include <cctype>
#include <algorithm>
#include <sstream>

namespace minidb {

SQLParser::SQLParser(const std::string& sql)
    : sql_(sql), pos_(0), line_(1), column_(1), token_pos_(0) {
}

std::unique_ptr<ASTNode> SQLParser::Parse() {
    Tokenize();
    token_pos_ = 0;
    
    if (tokens_.empty()) {
        return nullptr;
    }
    
    Token first = tokens_[0];
    if (first.type == TokenType::EOF_TOKEN) {
        return nullptr;
    }
    
    if (MatchKeyword("CREATE")) {
        return ParseCreateTable();
    } else if (MatchKeyword("INSERT")) {
        return ParseInsert();
    } else if (MatchKeyword("SELECT")) {
        return ParseSelect();
    } else {
        return nullptr; // Unknown statement
    }
}

void SQLParser::Tokenize() {
    while (pos_ < sql_.length()) {
        SkipWhitespace();
        
        if (pos_ >= sql_.length()) {
            break;
        }
        
        char c = sql_[pos_];
        
        // Single character tokens
        if (c == '(') {
            tokens_.push_back({TokenType::LPAREN, "(", line_, column_});
            pos_++;
            column_++;
            continue;
        }
        if (c == ')') {
            tokens_.push_back({TokenType::RPAREN, ")", line_, column_});
            pos_++;
            column_++;
            continue;
        }
        if (c == ',') {
            tokens_.push_back({TokenType::COMMA, ",", line_, column_});
            pos_++;
            column_++;
            continue;
        }
        if (c == ';') {
            tokens_.push_back({TokenType::SEMICOLON, ";", line_, column_});
            pos_++;
            column_++;
            continue;
        }
        if (c == '=') {
            tokens_.push_back({TokenType::EQUALS, "=", line_, column_});
            pos_++;
            column_++;
            continue;
        }
        
        // Numbers
        if (isdigit(c)) {
            std::string num;
            while (pos_ < sql_.length() && isdigit(sql_[pos_])) {
                num += sql_[pos_];
                pos_++;
                column_++;
            }
            tokens_.push_back({TokenType::NUMBER, num, line_, static_cast<int>(column_ - num.length())});
            continue;
        }
        
        // Identifiers and keywords
        if (isalpha(c)) {
            std::string word;
            while (pos_ < sql_.length() && (isalnum(sql_[pos_]) || sql_[pos_] == '_')) {
                word += sql_[pos_];
                pos_++;
                column_++;
            }
            
            // Convert to uppercase for keyword matching
            std::string upper_word = word;
            std::transform(upper_word.begin(), upper_word.end(), upper_word.begin(), ::toupper);
            
            if (IsKeyword(upper_word)) {
                tokens_.push_back({TokenType::KEYWORD, upper_word, line_, static_cast<int>(column_ - word.length())});
            } else {
                tokens_.push_back({TokenType::IDENTIFIER, word, line_, static_cast<int>(column_ - word.length())});
            }
            continue;
        }
        
        // Unknown character
        pos_++;
        column_++;
    }
    
    tokens_.push_back({TokenType::EOF_TOKEN, "", line_, column_});
}

void SQLParser::SkipWhitespace() {
    while (pos_ < sql_.length() && isspace(sql_[pos_])) {
        if (sql_[pos_] == '\n') {
            line_++;
            column_ = 1;
        } else {
            column_++;
        }
        pos_++;
    }
}

bool SQLParser::IsKeyword(const std::string& str) const {
    static const std::vector<std::string> keywords = {
        "CREATE", "TABLE", "INSERT", "INTO", "VALUES",
        "SELECT", "FROM", "WHERE", "BETWEEN", "AND", "INT"
    };
    return std::find(keywords.begin(), keywords.end(), str) != keywords.end();
}

Token SQLParser::PeekToken() {
    if (token_pos_ < tokens_.size()) {
        return tokens_[token_pos_];
    }
    return {TokenType::EOF_TOKEN, "", line_, column_};
}

Token SQLParser::ConsumeToken() {
    if (token_pos_ < tokens_.size()) {
        return tokens_[token_pos_++];
    }
    return {TokenType::EOF_TOKEN, "", line_, column_};
}

bool SQLParser::Match(TokenType type) {
    Token tok = PeekToken();
    if (tok.type == type) {
        ConsumeToken();
        return true;
    }
    return false;
}

bool SQLParser::MatchKeyword(const std::string& keyword) {
    Token tok = PeekToken();
    if (tok.type == TokenType::KEYWORD && tok.value == keyword) {
        ConsumeToken();
        return true;
    }
    return false;
}

void SQLParser::Expect(TokenType type) {
    if (!Match(type)) {
        // Error handling would go here
    }
}

void SQLParser::ExpectKeyword(const std::string& keyword) {
    if (!MatchKeyword(keyword)) {
        // Error handling would go here
    }
}

std::unique_ptr<ASTNode> SQLParser::ParseCreateTable() {
    ExpectKeyword("CREATE");
    ExpectKeyword("TABLE");
    
    Token table_tok = ConsumeToken();
    if (table_tok.type != TokenType::IDENTIFIER) {
        return nullptr;
    }
    
    std::string table_name = table_tok.value;
    Expect(TokenType::LPAREN);
    
    auto stmt = std::make_unique<CreateTableStmt>();
    stmt->table_name = table_name;
    stmt->columns = ParseColumnDefs();
    
    Expect(TokenType::RPAREN);
    
    return stmt;
}

std::vector<ColumnDef> SQLParser::ParseColumnDefs() {
    std::vector<ColumnDef> columns;
    
    do {
        ColumnDef col = ParseColumnDef();
        columns.push_back(col);
    } while (Match(TokenType::COMMA));
    
    return columns;
}

ColumnDef SQLParser::ParseColumnDef() {
    ColumnDef col;
    
    Token name_tok = ConsumeToken();
    if (name_tok.type != TokenType::IDENTIFIER) {
        return col;
    }
    col.name = name_tok.value;
    
    Token type_tok = ConsumeToken();
    if (type_tok.type == TokenType::KEYWORD) {
        if (type_tok.value == "INT") {
            col.type = DataType::INTEGER;
        } else if (type_tok.value == "VARCHAR") {
            col.type = DataType::VARCHAR;
        } else if (type_tok.value == "FLOAT") {
            col.type = DataType::FLOAT;
        } else if (type_tok.value == "BOOL") {
            col.type = DataType::BOOLEAN;
        }
    }
    
    return col;
}

std::unique_ptr<ASTNode> SQLParser::ParseInsert() {
    ExpectKeyword("INSERT");
    ExpectKeyword("INTO");
    
    Token table_tok = ConsumeToken();
    if (table_tok.type != TokenType::IDENTIFIER) {
        return nullptr;
    }
    
    std::string table_name = table_tok.value;
    
    ExpectKeyword("VALUES");
    Expect(TokenType::LPAREN);
    
    auto stmt = std::make_unique<InsertStmt>();
    stmt->table_name = table_name;
    stmt->values = ParseValues();
    
    Expect(TokenType::RPAREN);
    
    return stmt;
}

std::vector<Value> SQLParser::ParseValues() {
    std::vector<Value> values;
    
    do {
        Value val = ParseValue();
        values.push_back(val);
    } while (Match(TokenType::COMMA));
    
    return values;
}

Value SQLParser::ParseValue() {
    Token tok = ConsumeToken();
    
    Value val;
    if (tok.type == TokenType::NUMBER) {
        val.type = DataType::INTEGER;
        val.data.int_val = std::stoi(tok.value);
    }
    
    return val;
}

std::unique_ptr<ASTNode> SQLParser::ParseSelect() {
    ExpectKeyword("SELECT");
    
    std::vector<std::string> columns = ParseColumnList();
    
    ExpectKeyword("FROM");
    
    Token table_tok = ConsumeToken();
    if (table_tok.type != TokenType::IDENTIFIER) {
        return nullptr;
    }
    
    std::string table_name = table_tok.value;
    
    auto stmt = std::make_unique<SelectStmt>();
    stmt->table_name = table_name;
    stmt->columns = columns;
    
    // Parse WHERE clause if present
    if (MatchKeyword("WHERE")) {
        stmt->predicate = ParsePredicate();
    }
    
    return stmt;
}

std::vector<std::string> SQLParser::ParseColumnList() {
    std::vector<std::string> columns;
    
    do {
        Token tok = ConsumeToken();
        if (tok.type == TokenType::IDENTIFIER || tok.type == TokenType::KEYWORD) {
            columns.push_back(tok.value);
        }
    } while (Match(TokenType::COMMA));
    
    return columns;
}

std::unique_ptr<Predicate> SQLParser::ParsePredicate() {
    Token col_tok = ConsumeToken();
    if (col_tok.type != TokenType::IDENTIFIER) {
        return nullptr;
    }
    
    std::string column_name = col_tok.value;
    
    // Check for BETWEEN predicate
    if (MatchKeyword("BETWEEN")) {
        auto pred = std::make_unique<BetweenPredicate>();
        pred->column_name = column_name;
        
        Value lower = ParseValue();
        pred->lower_bound = lower;
        
        ExpectKeyword("AND");
        
        Value upper = ParseValue();
        pred->upper_bound = upper;
        
        return pred;
    }
    
    // Check for EQUALS predicate
    if (Match(TokenType::EQUALS)) {
        auto pred = std::make_unique<EqualsPredicate>();
        pred->column_name = column_name;
        
        Value val = ParseValue();
        pred->value = val;
        
        return pred;
    }
    
    return nullptr;
}

}
