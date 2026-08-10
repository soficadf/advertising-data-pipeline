import logging
from datetime import datetime, timezone
from azure.storage.filedatalake import DataLakeServiceClient

logger = logging.getLogger(__name__)

import logging
import os
from datetime import datetime, timezone
from azure.storage.filedatalake import DataLakeServiceClient

logger = logging.getLogger(__name__)


def _is_local() -> bool:
    """Detecta si estamos en local o en Databricks."""
    try:
        from dotenv import load_dotenv
        load_dotenv()
        return os.getenv("ENV", "prod") == "local"
    except ImportError:
        return False


def _get_credentials() -> tuple:
    """Lee las credenciales del entorno disponible."""
    if _is_local():
        account_name = os.getenv("ADLS_ACCOUNT_NAME")
        account_key = os.getenv("ADLS_ACCOUNT_KEY")
        container = os.getenv("ADLS_CONTAINER_NAME")
        logger.info("Credenciales leídas desde .env")
    else:
        account_name = dbutils.secrets.get(scope="ad-pipeline", key="adls_account_name")
        account_key = dbutils.secrets.get(scope="ad-pipeline", key="adls_account_key")
        container = dbutils.secrets.get(scope="ad-pipeline", key="adls_container_name")
        logger.info("Credenciales leídas desde Databricks Secrets")

    if not account_name or not account_key or not container:
        raise ValueError("Credenciales de ADLS no encontradas")

    return account_name, account_key, container

def get_adls_client() -> DataLakeServiceClient:
    """Devuelve un cliente autenticado de ADLS Gen2."""
    account_name, account_key, _ = _get_credentials()
    return DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=account_key
    )


def upload_to_adls(content: str, layer: str, folder: str, filename: str, target_date=None) -> str:
    """
    Sube un fichero al Data Lake en la capa y carpeta indicadas.

    Args:
        content: Contenido del fichero en formato string
        layer: Capa del Lakehouse (landing, bronze, silver, gold)
        folder: Subcarpeta dentro de la capa (meta_ads, ventas)
        filename: Nombre del fichero
        target_date: Fecha para la partición. Si es None usa la fecha actual.
    """
    account_name, account_key, container = _get_credentials()

    client = DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=account_key
    )

    date_path = target_date.strftime("%Y/%m/%d") if target_date else datetime.now(timezone.utc).strftime("%Y/%m/%d")
    path = f"{layer}/{folder}/{date_path}/{filename}"

    filesystem_client = client.get_file_system_client(container)
    file_client = filesystem_client.get_file_client(path)
    file_client.upload_data(content, overwrite=True)

    logger.info(f"Fichero subido a: {path}")
    return path