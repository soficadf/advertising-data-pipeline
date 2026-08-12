
from enum import Enum


#Nombre de las carpetas del datalake
class Layers (Enum):
    LANDING="landing"
    BRONZE= "bronze"
    SILVER="silver"
    GOLD="gold"

class DatasetsLanding(Enum):
    VENTAS="ventas"
    META="meta_ads"

class DatasetsBronze(Enum):
    VENTAS="ventas"
    META="meta_ads"

class DatasetsSilver(Enum):
    VENTAS="ventas_diarias"
    META="meta_ads"
    SPEND="ad_spend"

class DatasetsGold(Enum):
    REACH="gold_demographic_reach"
    DAILY_METRICS="gold_ad_daily_metrics"
    SATURATION="gold_saturation_curve"
    TOTAL_METRICS="gold_ad_total_metrics"
    CATALOG = "masterscf002dbr"
    SCHEMA="ad-pipeline"

class TablesSQl(Enum):
    SPEND="daily_spend"
    PRODUCT_MAP="ad_product_mapping"
    PRODUCTS="products"

