#pragma once

#include "../common/types.h"
#include "../catalog/schema.h"
#include <string>
#include <vector>
#include <memory>
#include <variant>

namespace minidb {

// Token types
enum class TokenType {
    KEYWORD,
    IDENTIFIER,
    NUMBER,
    COMMA,
    LPAREN,
    RPAREN,
    SEMICOLON,
    EQUALS,
    BETWEEN,
    AND,
    EOF_TOKEN,
    UNKNOWN
};

struct Token {
    TokenType type;
    std::string value;
    int line;
    int column;
};

// AST Node Types
enum class ASTNodeType {
    CREATE_TABLE,
    INSERT,
    SELECT
};

// Base AST node
struct ASTNode {
    ASTNode(ASTNodeType t) : type(t) {}
    virtual ~ASTNode() = default;
    ASTNodeType type;
};

// Column definition for CREATE TABLE
struct ColumnDef {
    std::string name;
    DataType type;
};

// CREATE TABLE statement
struct CreateTableStmt : public ASTNode {
    std::string table_name;
    std::vector<ColumnDef> columns;
    
    CreateTableStmt() : ASTNode{ASTNodeType::CREATE_TABLE} {}
};

// INSERT statement
struct InsertStmt : public ASTNode {
    std::string table_name;
    std::vector<Value> values;
    
    InsertStmt() : ASTNode{ASTNodeType::INSERT} {}
};

// Predicate types
enum class PredicateType {
    EQUALS,
    BETWEEN
};

// Base predicate
struct Predicate {
    Predicate(PredicateType t) : type(t) {}
    virtual ~Predicate() = default;
    PredicateType type;
};

// Equality predicate (WHERE id = X)
struct EqualsPredicate : public Predicate {
    std::string column_name;
    Value value;
    
    EqualsPredicate() : Predicate{PredicateType::EQUALS} {}
};

// Between predicate (WHERE id BETWEEN A AND B)
struct BetweenPredicate : public Predicate {
    std::string column_name;
    Value lower_bound;
    Value upper_bound;
    
    BetweenPredicate() : Predicate{PredicateType::BETWEEN} {}
};

// SELECT statement
struct SelectStmt : public ASTNode {
    std::string table_name;
    std::vector<std::string> columns; // "*" means all columns
    std::unique_ptr<Predicate> predicate;
    
    SelectStmt() : ASTNode{ASTNodeType::SELECT} {}
};

// Parser class
class SQLParser {
public:
    explicit SQLParser(const std::string& sql);
    
    std::unique_ptr<ASTNode> Parse();
    
private:
    std::string sql_;
    size_t pos_;
    int line_;
    int column_;
    std::vector<Token> tokens_;
    size_t token_pos_;
    
    // Tokenizer
    void Tokenize();
    Token NextToken();
    void SkipWhitespace();
    bool IsKeyword(const std::string& str) const;
    
    // Parser helpers
    Token PeekToken();
    Token ConsumeToken();
    bool Match(TokenType type);
    bool MatchKeyword(const std::string& keyword);
    void Expect(TokenType type);
    void ExpectKeyword(const std::string& keyword);
    
    // Statement parsers
    std::unique_ptr<ASTNode> ParseCreateTable();
    std::unique_ptr<ASTNode> ParseInsert();
    std::unique_ptr<ASTNode> ParseSelect();
    
    // Helper parsers
    std::vector<ColumnDef> ParseColumnDefs();
    ColumnDef ParseColumnDef();
    std::vector<Value> ParseValues();
    Value ParseValue();
    std::unique_ptr<Predicate> ParsePredicate();
    std::vector<std::string> ParseColumnList();
};

}
