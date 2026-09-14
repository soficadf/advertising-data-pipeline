import pytest
from unittest.mock import patch, MagicMock
from datetime import date, datetime, timezone


def test_upload_to_adls_builds_correct_path():
    """Verifica que upload_to_adls construye el path correcto."""
    mock_client = MagicMock()
    mock_filesystem = MagicMock()
    mock_file = MagicMock()
    mock_client.get_file_system_client.return_value = mock_filesystem
    mock_filesystem.get_file_client.return_value = mock_file

    with patch("config.adls_client.get_secret", return_value="test_value"), \
         patch("config.adls_client.DataLakeServiceClient", return_value=mock_client):

        from config.adls_client import upload_to_adls
        upload_to_adls(
            content="test",
            layer="landing",
            folder="meta_ads",
            filename="test.json"
        )

        call_args = mock_filesystem.get_file_client.call_args[0][0]
        assert call_args.startswith("landing/meta_ads/")
        assert call_args.endswith("test.json")


def test_upload_to_adls_uses_target_date_in_path():
    """Verifica que upload_to_adls usa target_date en el path cuando se proporciona."""
    mock_client = MagicMock()
    mock_filesystem = MagicMock()
    mock_file = MagicMock()
    mock_client.get_file_system_client.return_value = mock_filesystem
    mock_filesystem.get_file_client.return_value = mock_file

    target = datetime(2026, 7, 15, tzinfo=timezone.utc)

    with patch("config.adls_client.get_secret", return_value="test_value"), \
         patch("config.adls_client.DataLakeServiceClient", return_value=mock_client):

        from config.adls_client import upload_to_adls
        upload_to_adls(
            content="test",
            layer="landing",
            folder="ventas",
            filename="ventas.json",
            target_date=target
        )

        call_args = mock_filesystem.get_file_client.call_args[0][0]
        assert "2026/07/15" in call_args


def test_upload_to_adls_calls_upload_data():
    """Verifica que upload_to_adls llama a upload_data con el contenido correcto."""
    mock_client = MagicMock()
    mock_filesystem = MagicMock()
    mock_file = MagicMock()
    mock_client.get_file_system_client.return_value = mock_filesystem
    mock_filesystem.get_file_client.return_value = mock_file

    content = '{"test": "data"}'

    with patch("config.adls_client.get_secret", return_value="test_value"), \
         patch("config.adls_client.DataLakeServiceClient", return_value=mock_client):

        from config.adls_client import upload_to_adls
        upload_to_adls(
            content=content,
            layer="landing",
            folder="meta_ads",
            filename="test.json"
        )

        mock_file.upload_data.assert_called_once_with(content, overwrite=True)


def test_upload_to_adls_uses_provided_container():
    """Verifica que usa el container proporcionado sin llamar a get_secret."""
    mock_client = MagicMock()
    mock_filesystem = MagicMock()
    mock_file = MagicMock()
    mock_client.get_file_system_client.return_value = mock_filesystem
    mock_filesystem.get_file_client.return_value = mock_file

    with patch("config.adls_client.get_secret", return_value="default_value") as mock_secret, \
         patch("config.adls_client.DataLakeServiceClient", return_value=mock_client):

        from config.adls_client import upload_to_adls
        upload_to_adls(
            content="test",
            layer="landing",
            folder="meta_ads",
            filename="test.json",
            container="my_container",
            account_name="my_account",
            account_key="my_key"
        )

        mock_client.get_file_system_client.assert_called_once_with("my_container")


def test_get_adls_client_uses_provided_credentials():
    """Verifica que get_adls_client usa las credenciales proporcionadas sin llamar a get_secret."""
    with patch("config.adls_client.DataLakeServiceClient") as mock_dls, \
         patch("config.adls_client.get_secret") as mock_secret:

        from config.adls_client import get_adls_client
        get_adls_client(account_name="my_account", account_key="my_key")

        mock_secret.assert_not_called()
        mock_dls.assert_called_once_with(
            account_url="https://my_account.dfs.core.windows.net",
            credential="my_key"
        )


def test_get_adls_client_reads_secrets_when_no_credentials():
    """Verifica que get_adls_client llama a get_secret cuando no se proporcionan credenciales."""
    with patch("config.adls_client.DataLakeServiceClient"), \
         patch("config.adls_client.get_secret", return_value="test_value") as mock_secret:

        from config.adls_client import get_adls_client
        get_adls_client()

        assert mock_secret.call_count == 2