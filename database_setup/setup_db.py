import os
import sys

# Try importing mysql connector, or install it if missing
try:
    import mysql.connector
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "mysql-connector-python"])
    import mysql.connector
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def get_db_connection():
    # Get credentials from env or use defaults
    host = os.environ.get('DB_HOST', 'localhost')
    user = os.environ.get('DB_USER', 'root')
    password = os.environ.get('DB_PASSWORD', '')
    
    # Establish connection to server (without DB first)
    try:
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password
        )
        return conn
    except mysql.connector.Error as err:
        logger.error(f"Error connecting to MySQL: {err}")
        sys.exit(1)

def setup_database():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    db_name = os.environ.get('DB_NAME', 'medical_lab')
    
    print(f"Creating database {db_name} if not exists...")
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    cursor.execute(f"USE {db_name}")
    
    logger.info("Executing OLTP setup SQL...")
    
    # Read execute SQL file
    script_dir = os.path.dirname(os.path.abspath(__file__))
    sql_path = os.path.join(script_dir, 'oltp_setup.sql')
    with open(sql_path, 'r') as f:
        sql_script = f.read()
    
    # Split by semicolon and execute
    # Note: simplistic splitting, might break on semicolons in strings but fine for this DDL
    statements = sql_script.split(';')
    
    for statement in statements:
        if statement.strip():
            try:
                cursor.execute(statement)
            except mysql.connector.Error as err:
                logger.error(f"Error executing statement: {statement[:50]}...")
                logger.error(err)
                
    conn.commit()
    logger.info("OLTP initialization complete.")
    
    # Verify tables
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    logger.info("Tables created:")
    for table in tables:
        logger.info(f"- {table[0]}")
        
    cursor.close()
    conn.close()

if __name__ == "__main__":
    setup_database()
