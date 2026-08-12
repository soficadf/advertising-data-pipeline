import pyodbc
import random
import logging
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import os
import json
from config.adls_client import get_adls_client
from procesamiento.model import DatasetsLanding, Layers

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def load_products() -> list:
    config_path = os.path.join(os.path.dirname(__file__), "products.json")
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)["products"]


def get_connection():
    """Devuelve una conexión a Azure SQL."""
    conn_string = os.getenv("AZURE_SQL_CONNECTION_STRING")
    if not conn_string:
        raise ValueError("AZURE_SQL_CONNECTION_STRING no encontrado en .env")
    return pyodbc.connect(conn_string)


def create_tables(conn):
    """Crea las tablas necesarias en Azure SQL."""
    cursor = conn.cursor()

    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='products' AND xtype='U')
        CREATE TABLE products (
            product_id VARCHAR(10) PRIMARY KEY,
            product_name VARCHAR(100) NOT NULL,
            category VARCHAR(50) NOT NULL,
            price DECIMAL(10,2) NOT NULL
        )
    """)

    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='ad_product_mapping' AND xtype='U')
        CREATE TABLE ad_product_mapping (
            ad_id VARCHAR(50) PRIMARY KEY,
            product_id VARCHAR(10) NOT NULL,
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        )
    """)

    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='daily_spend' AND xtype='U')
        CREATE TABLE daily_spend (
            id INT IDENTITY(1,1) PRIMARY KEY,
            ad_id VARCHAR(50) NOT NULL,
            date DATE NOT NULL,
            daily_spend DECIMAL(10,2) NOT NULL,
            UNIQUE(ad_id, date)
        )
    """)

    conn.commit()
    logger.info("Tablas creadas correctamente")


def insert_products(conn):
    products = load_products()
    cursor = conn.cursor()
    for p in products:
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM products WHERE product_id = ?)
            INSERT INTO products (product_id, product_name, category, price)
            VALUES (?, ?, ?, ?)
        """, p["id"], p["id"], p["name"], p["category"], p["price"])
    conn.commit()
    logger.info(f"{len(products)} productos insertados")


def insert_ad_product_mapping(conn, ad_ids: list):
    """Asigna cada anuncio a un producto."""

    cursor = conn.cursor()
    products = load_products()
    product_ids = [p["id"] for p in products]
    random.shuffle(ad_ids)

    for i, ad_id in enumerate(ad_ids):
        product_id = product_ids[i % len(product_ids)]
        cursor.execute(
            """
            IF NOT EXISTS (
                SELECT 1
                FROM ad_product_mapping
                WHERE ad_id = ?
            )
            INSERT INTO ad_product_mapping
                (ad_id, product_id)
            VALUES (?, ?)
            """,
            ad_id,
            ad_id,
            product_id
        )

    conn.commit()
    logger.info(f"{len(ad_ids)} anuncios mapeados a productos")


def insert_daily_spend(
    conn,
    ad_ids: list,
    days: int = 30
):
    """Genera gasto diario desde la fecha de inicio de cada anuncio."""

    cursor = conn.cursor()

    today = datetime.now(timezone.utc).date()
    first_date = today - timedelta(days=days - 1)

    total = 0

    for ad_id in ad_ids:

        # Cada anuncio comienza en un momento diferente.
        start_offset = random.randint(0, days - 7)
        start_date = first_date + timedelta(days=start_offset)

        base_spend = random.uniform(15, 80)
        current_date = start_date
        while current_date <= today:
            daily = round(base_spend * random.uniform(0.75, 1.25),2)
            cursor.execute(
                """
                IF NOT EXISTS (
                    SELECT 1
                    FROM daily_spend
                    WHERE ad_id = ?
                    AND date = ?
                )
                INSERT INTO daily_spend
                    (ad_id, date, daily_spend)
                VALUES (?, ?, ?)
                """,
                ad_id,
                current_date,
                ad_id,
                current_date,
                daily
            )

            total += 1
            current_date += timedelta(days=1)

    conn.commit()
    logger.info(f"{total} registros de gasto diario insertados")


def load_ad_ids_from_landing(filepath: str) -> list:
    """Carga los ad_ids extraídos de Meta desde un fichero JSON local."""
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    return [ad["id"] for ad in data.get("ads", [])]


if __name__ == "__main__":

    account_name = os.getenv("ADLS_ACCOUNT_NAME")
    container = os.getenv("ADLS_CONTAINER_NAME")
    
    client = get_adls_client()
    filesystem = client.get_file_system_client(container)
    
    paths = list(filesystem.get_paths(path=f"{Layers.LANDING.value}/{DatasetsLanding.META.value}", recursive=True))
    json_files = [p.name for p in paths if p.name.endswith(".json")]
    
    if not json_files:
        raise FileNotFoundError("No se encontró ningún fichero de Meta en landing")
    
    latest_file = sorted(json_files)[-1]
    logger.info(f"Usando fichero: {latest_file}")
    
    file_client = filesystem.get_file_client(latest_file)
    content = file_client.download_file().readall()
    data = json.loads(content)
    ad_ids = [ad["id"] for ad in data.get("ads", [])]
    logger.info(f"{len(ad_ids)} anuncios encontrados")

    conn = get_connection()
    create_tables(conn)
    insert_products(conn)
    insert_ad_product_mapping(conn, ad_ids)
    insert_daily_spend(conn, ad_ids, days=30)
    conn.close()
    
    logger.info("Base de datos configurada correctamente")