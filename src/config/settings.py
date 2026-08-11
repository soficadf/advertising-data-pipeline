
import logging
import os


logger = logging.getLogger(__name__)


# ─────────────────────────────────────────
# ENVIRONMENT
# ─────────────────────────────────────────

def is_local() -> bool:
    """
    Indica si la aplicación se está ejecutando
    en entorno local.
    """
    from dotenv import load_dotenv
    load_dotenv()
    return os.getenv("ENV", "prod") == "local"


# ─────────────────────────────────────────
# SECRETS
# ─────────────────────────────────────────

def get_secret(
    key: str,
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
        key:
            Nombre de la variable de entorno.

        required:
            Si es True, lanza un error cuando no existe
            el valor.

    Returns:
        Valor de configuración o None si no es obligatorio.
    """
    from dotenv import load_dotenv
    load_dotenv()

    if is_local():

        value = os.getenv(key)

        if value:
            logger.info(
                "Configuración local cargada correctamente."
            )

    else:
        
        try:
            from pyspark.dbutils import DBUtils
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.getOrCreate()
            dbutils = DBUtils(spark)
            value = dbutils.secrets.get(
                scope="ad-pipeline",
                key=key
            )

            logger.info(
                "Configuración cargada desde "
                "Databricks Secrets."
            )

        except Exception as exc:
            raise RuntimeError(
                f"No se pudo obtener el secreto '{key}' "
                f"del scope 'ad-pipeline': {exc}"
            ) from exc

    if required and not value:
        raise ValueError(
            f"Configuración requerida no encontrada: "
            f"{key}"
        )

    return value

def get_spark_session(): 
    """ Obtiene la sesión Spark adecuada para el entorno. Local: Utiliza Databricks Connect para ejecutar Spark sobre el cluster de Databricks. Databricks: Utiliza la SparkSession proporcionada por el entorno de Databricks. """ 
    if is_local(): 
        from databricks.connect import DatabricksSession 
        logger.info( "Creando sesión Spark mediante Databricks Connect" ) 
        return ( DatabricksSession.builder.getOrCreate() ) 
    from pyspark.sql import SparkSession 
    logger.info( "Utilizando SparkSession de Databricks" ) 
    return ( SparkSession.builder.getOrCreate() )