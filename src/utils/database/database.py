import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from configs.config  import DB_CONFIG
def obtain_connection():
    conn =psycopg2.connect(
        dbname=DB_CONFIG["database"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"]
        ) 
    return conn

def get_jdbc_url():
    return f"jdbc:postgresql://db:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

def create_database_if_not_exists(dbname: str):
    conn = psycopg2.connect(
        dbname="postgres",
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"]
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()

    cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = %s;", (dbname,))
    exists = cursor.fetchone()

    if not exists:
        cursor.execute(f"CREATE DATABASE {dbname};")
        print(f"Database '{dbname}' created.")
    else:
        print(f"Database '{dbname}' already exists.")

    cursor.close()
    conn.close()

def create_table_if_not_exists():
    conn = obtain_connection()
    cursor = conn.cursor()

    create_sales_table = """
        CREATE TABLE IF NOT EXISTS ingested_sales (
            order_id INT PRIMARY KEY,
            product TEXT,
            quantity INT,
            price_each FLOAT,
            purchase_address TEXT,
            order_date TIMESTAMP,
            month_date INT,
            order_hour TEXT,
            street TEXT,
            city TEXT,
            state TEXT,
            zip_code TEXT,
            total_price FLOAT
        );
    """
    cursor.execute(create_sales_table)

    metrics_tables = {
        "metrics_monthly_sales": """
            CREATE TABLE IF NOT EXISTS metrics_monthly_sales (
                month_date INT,
                sales FLOAT
            );
        """,
        "metrics_top_selling_citys_states": """
            CREATE TABLE IF NOT EXISTS metrics_top_selling_citys_states (
                city TEXT,
                state TEXT,
                sales FLOAT
            );
        """,
        "metrics_most_sold_products": """
            CREATE TABLE IF NOT EXISTS metrics_most_sold_products (
                product TEXT,
                quantity INT
            );
        """,
        "metrics_best_hours": """
            CREATE TABLE IF NOT EXISTS metrics_best_hours (
                order_hour TEXT,
                quantity INT
            );
        """,
        "metrics_avg_sales_by_city_state": """
            CREATE TABLE IF NOT EXISTS metrics_avg_sales_by_city_state (
                city TEXT,
                state TEXT,
                avg_sales FLOAT
            );
        """,
        "metrics_avg_quantity_per_product": """
            CREATE TABLE IF NOT EXISTS metrics_avg_quantity_per_product (
                product TEXT,
                avg_quantity FLOAT
            );
        """,
        "metrics_avg_sales_by_month": """
            CREATE TABLE IF NOT EXISTS metrics_avg_sales_by_month (
                month_date INT,
                avg_sales FLOAT
            );
        """,
        "metrics_most_sold_products_by_month": """
            CREATE TABLE IF NOT EXISTS metrics_most_sold_products_by_month (
                month_date INT,
                product TEXT,
                total_quantity INT
            );
        """
    }

    for name, ddl in metrics_tables.items():
        print(f"Creating table: {name}")
        cursor.execute(ddl)

    conn.commit()
    cursor.close()
    conn.close()
    
def insert_to_postgres(df, table_name: str):
    jdbc_url = get_jdbc_url()
    
    df.write \
      .format("jdbc") \
      .option("url", jdbc_url) \
      .option("dbtable", table_name) \
      .option("user", DB_CONFIG["user"]) \
      .option("password", DB_CONFIG["password"]) \
      .option("driver", "org.postgresql.Driver") \
      .mode("overwrite") \
      .save()


