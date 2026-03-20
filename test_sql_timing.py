#!/usr/bin/env python3
"""
Simple script to measure SQL execution time for a single query.
"""

import sqlite3
import time
from pathlib import Path

# Database path
db_path = Path("data/dev/dev_databases/california_schools/california_schools.sqlite")

# SQL query to test
sql = "SELECT COUNT(School) FROM schools WHERE (StatusType = 'Closed' OR StatusType = 'Active') AND SOC = 69 AND County = 'Alpine'"

print(f"Database: {db_path}")
print(f"SQL: {sql}\n")

if not db_path.exists():
    print(f"❌ Error: Database not found at {db_path}")
    exit(1)

try:
    # Connect and execute
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Warm up (optional)
    cursor.execute(sql)
    cursor.fetchall()
    
    # Measure execution time
    start = time.time()
    cursor.execute(sql)
    result = cursor.fetchall()
    end = time.time()
    
    duration_ms = (end - start) * 1000
    
    print(f"Result: {result[0][0]}")
    print(f"Execution time: {duration_ms:.2f}ms")
    
    conn.close()
    
except Exception as e:
    print(f"❌ Error: {e}")
    exit(1)
