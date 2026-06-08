#!/bin/bash
# Usage: init_db.sh <server> <password> <sql_file>

SERVER=$1
PASSWORD=$2
SQL_FILE=$3

echo "Waiting for SQL Server at $SERVER to be ready..."
for i in $(seq 1 30); do
    /opt/mssql-tools18/bin/sqlcmd -S "$SERVER" -U sa -P "$PASSWORD" -Q "SELECT 1" -No &>/dev/null
    if [ $? -eq 0 ]; then
        echo "SQL Server is ready."
        break
    fi
    echo "  attempt $i/30 — retrying in 2s..."
    sleep 2
done

echo "Running $SQL_FILE..."
/opt/mssql-tools18/bin/sqlcmd -S "$SERVER" -U sa -P "$PASSWORD" -i "$SQL_FILE" -No
echo "Done."
