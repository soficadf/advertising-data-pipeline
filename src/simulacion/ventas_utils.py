import json
import math
import random
import logging
from datetime import datetime, timezone, date
from dotenv import load_dotenv
import os
import pyodbc

load_dotenv()

logger = logging.getLogger(__name__)

REGIONS = ["Madrid", "Barcelona", "Valencia", "Sevilla", "Bilbao"]


def load_products() -> list:
    """Carga los productos desde el fichero de configuración."""
    config_path = os.path.join(os.path.dirname(__file__), "products.json")
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)["products"]


def get_db_connection():
    """Devuelve una conexión a Azure SQL."""
    conn_string = os.getenv("AZURE_SQL_CONNECTION_STRING")
    if not conn_string:
        raise ValueError("AZURE_SQL_CONNECTION_STRING no encontrado en .env")
    return pyodbc.connect(conn_string)


def get_product_weights() -> tuple:
    """Calcula el peso de cada producto según cuántos anuncios lo promocionan."""
    products = load_products()
    weights = {p["id"]: 1 for p in products}

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT product_id FROM ad_product_mapping")
        for row in cursor.fetchall():
            if row[0] in weights:
                weights[row[0]] += 1
        conn.close()
    except Exception as e:
        logger.warning(f"No se pudo consultar el mapeo: {e}")

    return products, [weights[p["id"]] for p in products]


def get_daily_spend(target_date: date) -> float:
    """Consulta el gasto publicitario total de un día concreto en Azure SQL."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COALESCE(SUM(daily_spend), 0)
            FROM daily_spend
            WHERE date = ?
        """, target_date)
        result = cursor.fetchone()[0]
        conn.close()
        return float(result)
    except Exception as e:
        logger.warning(f"No se pudo consultar el gasto para {target_date}: {e}. Usando valor por defecto.")
        return 100.0


def ventas_por_dia(gasto_diario: float) -> int:
    """Calcula ventas totales del día según la curva de saturación."""
    a = 300
    b = 0.001
    ventas = a * (1 - math.exp(-b * gasto_diario))
    return max(5, int(ventas))


def ventas_por_hora(gasto_diario: float) -> int:
    """Calcula ventas por hora según la curva de saturación."""
    a = 50
    b = 0.005
    ventas = a * (1 - math.exp(-b * gasto_diario))
    return max(1, int(ventas))


def generate_sale_event(products: list, weights: list, target_date: date = None) -> dict:
    """
    Genera un evento de venta sintético.
    Si target_date es None usa el timestamp actual.
    """
    product = random.choices(products, weights=weights, k=1)[0]
    quantity = random.randint(1, 3)

    if target_date:
        hour = random.randint(8, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        event_dt = datetime(
            target_date.year, target_date.month, target_date.day,
            hour, minute, second, tzinfo=timezone.utc
        )
    else:
        event_dt = datetime.now(timezone.utc)

    return {
        "event_id": f"sale_{event_dt.strftime('%Y%m%d%H%M%S%f')}_{random.randint(1000,9999)}",
        "timestamp": event_dt.isoformat(),
        "product_id": product["id"],
        "product_name": product["name"],
        "quantity": quantity,
        "unit_price": product["price"],
        "total_amount": round(product["price"] * quantity, 2),
        "region": random.choice(REGIONS),
        "channel": "ecommerce"
    }