import json
from simulacion.ventas_utils import load_products, get_product_weights, generate_sale_event

def get_test_products():
    products, weights = get_product_weights.__wrapped__() if hasattr(get_product_weights, '__wrapped__') else (load_products(), [1] * 5)
    return products, weights

def test_generate_sale_event_structure():
    products = load_products()
    weights = [1] * len(products)
    event = generate_sale_event(products, weights)
    required_fields = ["event_id", "timestamp", "product_id", "product_name",
                       "quantity", "unit_price", "total_amount", "region", "channel"]
    for field in required_fields:
        assert field in event

def test_generate_sale_event_total_amount():
    products = load_products()
    weights = [1] * len(products)
    event = generate_sale_event(products, weights)
    expected = round(event["unit_price"] * event["quantity"], 2)
    assert event["total_amount"] == expected

def test_generate_sale_event_quantity_range():
    products = load_products()
    weights = [1] * len(products)
    for _ in range(20):
        event = generate_sale_event(products, weights)
        assert 1 <= event["quantity"] <= 3

def test_generate_sale_event_channel():
    products = load_products()
    weights = [1] * len(products)
    event = generate_sale_event(products, weights)
    assert event["channel"] == "ecommerce"