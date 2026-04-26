#!/bin/bash

# Test script for MiniDB CLI

echo "Testing MiniDB CLI..."
echo ""

# Remove existing database files
rm -f minidb.db minidb.log

echo "Test 1: CREATE TABLE"
echo "CREATE TABLE users (id INT, value INT);" | ./minidb
echo ""

echo "Test 2: INSERT"
echo "INSERT INTO users VALUES (1, 100);" | ./minidb
echo ""

echo "Test 3: SELECT with WHERE id ="
echo "SELECT * FROM users WHERE id = 1;" | ./minidb
echo ""

echo "Test 4: SELECT with WHERE id BETWEEN"
echo "INSERT INTO users VALUES (2, 200);" | ./minidb
echo "INSERT INTO users VALUES (3, 300);" | ./minidb
echo "SELECT * FROM users WHERE id BETWEEN 1 AND 3;" | ./minidb
echo ""

echo "Test 5: Persistence check (should show persistence message)"
echo "exit" | ./minidb
echo ""

echo "Test 6: Verify persistence by querying after restart"
echo "SELECT * FROM users;" | ./minidb
echo ""

echo "Test 7: help command"
echo "help" | ./minidb
echo ""

echo "Test 8: clear command"
echo "clear" | ./minidb
echo ""

echo "Test 9: Invalid SQL"
echo "INVALID COMMAND;" | ./minidb
echo ""

echo "CLI testing complete."
