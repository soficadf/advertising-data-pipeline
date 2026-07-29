from simulacion.ventas_simulator import generate_sale_event


def test_generate_sale_event_structure():
    """Verifica que el evento generado tiene todos los campos esperados."""
    event = generate_sale_event()
    
    required_fields = [
        "event_id", "timestamp", "product_id", "product_name",
        "quantity", "unit_price", "total_amount", "region", "channel"
    ]
    
    for field in required_fields:
        assert field in event, f"Campo {field} no encontrado en el evento"


def test_generate_sale_event_total_amount():
    """Verifica que el total_amount es consistente con quantity y unit_price."""
    event = generate_sale_event()
    expected = round(event["unit_price"] * event["quantity"], 2)
    assert event["total_amount"] == expected


def test_generate_sale_event_quantity_range():
    """Verifica que la cantidad está dentro del rango esperado."""
    for _ in range(20):
        event = generate_sale_event()
        assert 1 <= event["quantity"] <= 3


def test_generate_sale_event_channel():
    """Verifica que el canal es siempre ecommerce."""
    event = generate_sale_event()
    assert event["channel"] == "ecommerce"