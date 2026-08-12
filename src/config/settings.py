
import logging
import os
logger = logging.getLogger(__name__)



def is_local() -> bool:
    """
    Indica si la aplicación se está ejecutandoen entorno local.
    """
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        logger.info("python-dotenv no disponible; usando configuración de Databricks")

    return os.getenv("ENV", "prod") == "local"



def get_secret(key: str,required: bool = True) -> str | None:
    """
    Obtiene un valor de configuración dependiendo
    del entorno de ejecución.
    """
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        logger.info("python-dotenv no disponible; usando configuración de Databricks")

    if is_local():
        value = os.getenv(key)
        if value:
            logger.info("Configuración local cargada correctamente.")
    else:
        try:
            from pyspark.dbutils import DBUtils
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.getOrCreate()
            dbutils = DBUtils(spark)
            value = dbutils.secrets.get(scope="ad-pipeline",key=key)

            logger.info("Configuración cargada desde Databricks Secrets")

        except Exception as exc:
            raise RuntimeError(f"No se pudo obtener el secreto '{key}' del scope 'ad-pipeline': {exc}") from exc

    if required and not value:
        raise ValueError(f"Configuración requerida no encontrada: {key}")

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