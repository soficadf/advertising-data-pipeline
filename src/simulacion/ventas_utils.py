import json
import logging
import math
import os
import random
import pyodbc

from datetime import date, datetime, timezone
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

REGIONS = ["Madrid", "Barcelona", "Valencia", "Sevilla", "Bilbao"]


def load_products() -> list:
    config_path = os.path.join(os.path.dirname(__file__), "products.json")
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)["products"]


def get_db_connection():
    conn_string = os.getenv("AZURE_SQL_CONNECTION_STRING")

    if not conn_string:
        raise ValueError(
            "AZURE_SQL_CONNECTION_STRING no encontrado en .env"
        )

    return pyodbc.connect(conn_string)


def get_product_weights() -> tuple:
    """
    Calcula la probabilidad de venta de cada producto
    según el número de anuncios que lo promocionan.
    """
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
    """Consulta el gasto publicitario total de un día."""

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT COALESCE(SUM(daily_spend), 0)
            FROM daily_spend
            WHERE date = ?
            """,
            target_date
        )

        result = cursor.fetchone()[0]
        conn.close()
        return float(result)

    except Exception as e:
        logger.warning(
            f"No se pudo consultar el gasto para "
            f"{target_date}: {e}. Usando valor por defecto."
        )

        return 100.0


def ventas_por_dia(gasto_diario: float) -> int:
    """
    Genera el número de ventas diarias en función del gasto.
    La relación es creciente pero con saturación progresiva.
    """

    a = 180
    b = 0.00015

    ventas = a * (
        1 - math.exp(-b * gasto_diario)
    )

    return max(10, int(ventas))


def ventas_por_hora(gasto_diario: float) -> int:
    """
    Genera el número de eventos de venta por hora.
    """

    ventas_dia = ventas_por_dia(gasto_diario)

    return max(1, int(ventas_dia / 16))


def generate_sale_event(
    products: list,
    weights: list,
    target_date: date = None
) -> dict:
    """
    Genera un evento de venta sintético.
    """

    product = random.choices(
        products,
        weights=weights,
        k=1
    )[0]

    quantity = random.choices(
        [1, 2, 3],
        weights=[0.80, 0.17, 0.03],
        k=1
    )[0]

    if target_date:
        hour = random.randint(8, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)

        event_dt = datetime(
            target_date.year,
            target_date.month,
            target_date.day,
            hour,
            minute,
            second,
            tzinfo=timezone.utc
        )
    else:
        event_dt = datetime.now(timezone.utc)

    return {
        "event_id": (
            f"sale_"
            f"{event_dt.strftime('%Y%m%d%H%M%S%f')}_"
            f"{random.randint(1000, 9999)}"
        ),
        "timestamp": event_dt.isoformat(),
        "product_id": product["id"],
        "product_name": product["name"],
        "quantity": quantity,
        "unit_price": product["price"],
        "total_amount": round(
            product["price"] * quantity,
            2
        ),
        "region": random.choice(REGIONS),
        "channel": "ecommerce"
    }