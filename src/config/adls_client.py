import logging
from datetime import datetime, timezone
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv
import os

load_dotenv()

logger = logging.getLogger(__name__)


def get_adls_client() -> DataLakeServiceClient:
    """Devuelve un cliente autenticado de ADLS Gen2."""
    account_name = os.getenv("ADLS_ACCOUNT_NAME")
    account_key = os.getenv("ADLS_ACCOUNT_KEY")

    if not account_name or not account_key:
        raise ValueError("ADLS_ACCOUNT_NAME o ADLS_ACCOUNT_KEY no encontrados en .env")

    return DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=account_key
    )


def upload_to_adls(content: str, layer: str, folder: str, filename: str) -> str:
    """
    Sube un fichero al Data Lake en la capa y carpeta indicadas.
    
    Args:
        content: Contenido del fichero en formato string
        layer: Capa del Lakehouse (landing, bronze, silver, gold)
        folder: Subcarpeta dentro de la capa (meta_ads, ventas)
        filename: Nombre del fichero
    
    Returns:
        Ruta completa del fichero en el Data Lake
    """
    client = get_adls_client()
    container = os.getenv("ADLS_CONTAINER_NAME")

    # Ruta con partición por fecha
    today = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    path = f"{layer}/{folder}/{today}/{filename}"

    filesystem_client = client.get_file_system_client(container)
    file_client = filesystem_client.get_file_client(path)

    file_client.upload_data(content, overwrite=True)
    logger.info(f"Fichero subido a: {path}")

    return path