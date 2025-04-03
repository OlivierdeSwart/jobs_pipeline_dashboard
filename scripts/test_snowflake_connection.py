import snowflake.connector
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Set up Snowflake connection
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account="fvwgzsm-sp08493"  # ✅ SWITCH ORDER!
)

# Run query
cursor = conn.cursor()
cursor.execute("SELECT CURRENT_VERSION()")
print("Snowflake version:", cursor.fetchone()[0])

# Cleanup
cursor.close()
conn.close()
