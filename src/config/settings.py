```python
import logging
import os

from dotenv import load_dotenv


logger = logging.getLogger(__name__)


# ─────────────────────────────────────────
# ENVIRONMENT
# ─────────────────────────────────────────

def is_local() -> bool:
    """
    Indica si la aplicación se está ejecutando
    en entorno local.
    """

    return os.getenv("ENV", "prod") == "local"


# ─────────────────────────────────────────
# SECRETS
# ─────────────────────────────────────────

def get_secret(
    local_key: str,
    databricks_key: str,
    required: bool = True
) -> str | None:
    """
    Obtiene un valor de configuración dependiendo
    del entorno de ejecución.

    Local:
        Obtiene el valor desde las variables de entorno
        o desde el fichero .env.

    Databricks:
        Obtiene el valor desde Databricks Secrets.

    Args:
        local_key:
            Nombre de la variable de entorno local.

        databricks_key:
            Nombre de la key en Databricks Secrets.

        required:
            Si es True, lanza un error cuando no existe
            el valor.

    Returns:
        Valor de configuración o None si no es obligatorio.
    """

    if is_local():

        load_dotenv()

        value = os.getenv(local_key)

        if value:
            logger.info(
                "Configuración local cargada correctamente."
            )

    else:

        try:
            value = dbutils.secrets.get(
                scope="ad-pipeline",
                key=databricks_key
            )

            logger.info(
                "Configuración cargada desde "
                "Databricks Secrets."
            )

        except Exception as exc:
            raise RuntimeError(
                "No se pudo acceder a Databricks Secrets. "
                "Comprueba que el código se está ejecutando "
                "dentro de Databricks y que el scope "
                "'ad-pipeline' existe."
            ) from exc

    if required and not value:
        raise ValueError(
            f"Configuración requerida no encontrada: "
            f"{local_key}"
        )

    return value

def get_spark_session(): 
    """ Obtiene la sesión Spark adecuada para el entorno. Local: Utiliza Databricks Connect para ejecutar Spark sobre el cluster de Databricks. Databricks: Utiliza la SparkSession proporcionada por el entorno de Databricks. """ 
    if is_local(): 
        from databricks.connect import DatabricksSession 
        logger.info( "Creando sesión Spark mediante Databricks Connect" ) 
        return ( DatabricksSession .builder .getOrCreate() ) 
    from pyspark.sql import SparkSession 
    logger.info( "Utilizando SparkSession de Databricks" ) 
    return ( SparkSession .builder .getOrCreate() )