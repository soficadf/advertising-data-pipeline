import logging
from datetime import datetime, timezone
from azure.storage.filedatalake import DataLakeServiceClient
from config.settings import get_secret

logger = logging.getLogger(__name__)

def get_adls_client(account_name=None,account_key=None) -> DataLakeServiceClient:
    """
    Devuelve un cliente autenticado de ADLS Gen2.
    """
    if not account_name:
        account_name = get_secret(key="ADLS_ACCOUNT_NAME")
    if not account_key:
        account_key = get_secret(key="ADLS_ACCOUNT_KEY")

    return DataLakeServiceClient(
        account_url=(f"https://{account_name}.dfs.core.windows.net"),
        credential=account_key
    )


def upload_to_adls(content, layer, folder, filename,
                   container=None, target_date=None, acount_name=None, account_key=None):
    """
    Sube un fichero al Data Lake.
    """

    if container is None:
        container = get_secret(key="ADLS_CONTAINER_NAME")

    client = get_adls_client(acount_name,account_key)
    date = (
        target_date
        if target_date is not None
        else datetime.now(timezone.utc)
    )

    date_path = date.strftime("%Y/%m/%d")
    path = (
        f"{layer}/"
        f"{folder}/"
        f"{date_path}/"
        f"{filename}"
    )

    filesystem_client = client.get_file_system_client(container)
    file_client = filesystem_client.get_file_client(path)
    file_client.upload_data(content,overwrite=True)

    logger.info("Fichero subido a ADLS: %s",path)
    return path

def get_adls_base_path() -> tuple[str, str]:
    """Obtiene las credenciales de ADLS y construye la ruta base."""

    storage_account =  get_secret("ADLS_ACCOUNT_NAME")
    container =  get_secret("ADLS_CONTAINER_NAME")

    base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
    return  base_path

def configure_spark_adls(spark):
    storage_account = get_secret("ADLS_ACCOUNT_NAME")
    account_key = get_secret("ADLS_ACCOUNT_KEY")

    spark.conf.set(
        f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
        account_key
    )