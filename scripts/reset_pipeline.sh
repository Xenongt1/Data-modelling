#!/bin/bash

# clean up previous log
rm -f execution.log

# Set credentials
export DB_PASSWORD='Ayo05mide*'

echo "🔄 Restarting Service: Full Pipeline Reset"
echo "----------------------------------------"

echo "Step 1: Generating Fresh Data (Attributes & Encounters)..."
python3 generate_data.py

echo "Step 2: Analyzing OLTP Performance..."
python3 run_oltp_analysis.py

echo "Step 3: Running ETL (Loading Data Warehouse)..."
python3 etl_logic.py

echo "Step 4: Analyzing Star Schema Performance..."
python3 run_dw_analysis.py

echo "----------------------------------------"
echo "✅ Done! Full pipeline executed."
echo "📄 Results logged in: execution.log"
