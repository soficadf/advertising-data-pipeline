import pytest
import json
import os
from unittest.mock import patch, MagicMock, mock_open
from datetime import date, timedelta


def test_load_products_returns_list():
    """Verifica que load_products devuelve una lista de productos."""
    mock_products = {
        "products": [
            {"id": "NP001", "name": "Hoodie Basic", "category": "Sudaderas", "price": 79.90},
            {"id": "NP002", "name": "Camiseta Oversize", "category": "Camisetas", "price": 39.90}
        ]
    }
    with patch("builtins.open", mock_open(read_data=json.dumps(mock_products))):
        from simulacion.setup_database import load_products
        products = load_products()
        assert isinstance(products, list)
        assert len(products) == 2


def test_load_products_has_required_fields():
    """Verifica que cada producto tiene los campos obligatorios."""
    mock_products = {
        "products": [
            {"id": "NP001", "name": "Hoodie Basic", "category": "Sudaderas", "price": 79.90}
        ]
    }
    with patch("builtins.open", mock_open(read_data=json.dumps(mock_products))):
        from simulacion.setup_database import load_products
        products = load_products()
        for p in products:
            assert "id" in p
            assert "name" in p
            assert "category" in p
            assert "price" in p


def test_insert_products_calls_execute_for_each_product():
    """Verifica que insert_products ejecuta un INSERT por cada producto."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    mock_products = {
        "products": [
            {"id": "NP001", "name": "Hoodie Basic", "category": "Sudaderas", "price": 79.90},
            {"id": "NP002", "name": "Camiseta Oversize", "category": "Camisetas", "price": 39.90}
        ]
    }

    with patch("builtins.open", mock_open(read_data=json.dumps(mock_products))):
        from simulacion.setup_database import insert_products
        insert_products(mock_conn)
        assert mock_cursor.execute.call_count == 2
        mock_conn.commit.assert_called_once()


def test_insert_ad_product_mapping_assigns_valid_product():
    """Verifica que cada anuncio se asigna a un producto válido."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    mock_products = {
        "products": [
            {"id": "NP001", "name": "Hoodie Basic", "category": "Sudaderas", "price": 79.90},
            {"id": "NP002", "name": "Camiseta Oversize", "category": "Camisetas", "price": 39.90}
        ]
    }

    ad_ids = ["ad_001", "ad_002", "ad_003"]

    with patch("builtins.open", mock_open(read_data=json.dumps(mock_products))):
        from simulacion.setup_database import insert_ad_product_mapping
        insert_ad_product_mapping(mock_conn, ad_ids)

        calls = mock_cursor.execute.call_args_list
        assert len(calls) == len(ad_ids)
        valid_product_ids = {"NP001", "NP002"}
        for call in calls:
            product_id = call[0][3]
            assert product_id in valid_product_ids