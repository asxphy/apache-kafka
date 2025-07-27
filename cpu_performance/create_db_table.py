import sqlalchemy
from sqlalchemy import text  # 👈 import text

username = "root"
password = "password"
host = "localhost"
port = "3306"

# Connect to MySQL server
engine = sqlalchemy.create_engine(
    f"mysql+mysqlconnector://{username}:{password}@{host}:{port}"
)
conn = engine.connect()

# ✅ Use text() for raw SQL
conn.execute(text("CREATE DATABASE IF NOT EXISTS cpu_monitoring"))
conn.execute(text("USE cpu_monitoring"))

# ✅ Same for table creation
conn.execute(text("""
CREATE TABLE IF NOT EXISTS cpu_metrics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    cpu_percent FLOAT,
    user_time FLOAT,
    system_time FLOAT,
    idle_time FLOAT,
    timestamp DOUBLE
)
"""))

print("✅ Database and table ready.")
