import pytest
from unittest.mock import patch, MagicMock
from ingesta.meta_extractor import fetch_ads, save_to_landing


def test_fetch_ads_returns_list():
    """Verifica que fetch_ads devuelve una lista."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "data": [
            {"id": "123", "page_name": "Nude Project", "ad_delivery_start_time": "2026-07-01"},
            {"id": "456", "page_name": "Nude Project", "ad_delivery_start_time": "2026-07-02"}
        ],
        "paging": {}
    }

    with patch("ingesta.meta_extractor.requests.get", return_value=mock_response):
        with patch.dict("os.environ", {"META_ACCESS_TOKEN": "fake_token"}):
            ads = fetch_ads("nude project")
            assert isinstance(ads, list)
            assert len(ads) == 2


def test_fetch_ads_empty_response():
    """Verifica que fetch_ads maneja una respuesta vacía."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": [], "paging": {}}

    with patch("ingesta.meta_extractor.requests.get", return_value=mock_response):
        with patch.dict("os.environ", {"META_ACCESS_TOKEN": "fake_token"}):
            ads = fetch_ads("marca_inexistente")
            assert ads == []


def test_fetch_ads_missing_token():
    """Verifica que fetch_ads lanza error si no hay token."""
    with patch.dict("os.environ", {}, clear=True):
        with pytest.raises(ValueError, match="META_ACCESS_TOKEN"):
            fetch_ads("nude project")


def test_fetch_ads_api_error():
    """Verifica que fetch_ads maneja errores de la API."""
    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.text = "Bad Request"

    with patch("ingesta.meta_extractor.requests.get", return_value=mock_response):
        with patch.dict("os.environ", {"META_ACCESS_TOKEN": "fake_token"}):
            ads = fetch_ads("nude project")
            assert ads == []