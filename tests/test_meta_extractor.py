from unittest.mock import patch, MagicMock

import pytest

from ingesta.meta_extractor import fetch_ads


def test_fetch_ads_returns_list():
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "data": [
            {
                "id": "123",
                "ad_delivery_start_time": "2026-07-01"
            },
            {
                "id": "456",
                "ad_delivery_start_time": "2026-07-02"
            }
        ],
        "paging": {}
    }

    mock_response.raise_for_status.return_value = None

    with patch(
        "ingesta.meta_extractor.requests.Session.get",
        return_value=mock_response
    ), patch(
        "ingesta.meta_extractor.get_secret",
        return_value="fake_token"
    ):

        ads = fetch_ads("nude project")

    assert isinstance(ads, list)
    assert len(ads) == 2
    assert ads[0]["id"] == "123"
    assert ads[1]["id"] == "456"


def test_fetch_ads_missing_token():

    with patch(
        "ingesta.meta_extractor.get_secret",
        return_value=None
    ):

        with pytest.raises(ValueError, match="META_ACCESS_TOKEN no configurado"):
            fetch_ads("nude project")