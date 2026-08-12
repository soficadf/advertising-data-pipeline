import enum

#Nombre de las carpetas del datalake
class Layers (enum):
    LANDING="landing"
    BRONZE= "bronze"
    SILVER="silver"
    GOLD="gold"

class DatasetsLanding(enum):
    VENTAS="ventas"
    META="meta_ads"

class DatasetsBronze(enum):
    VENTAS="ventas"
    META="meta_ads"

class DatasetsSilver(enum):
    VENTAS="ventas_diarias"
    META="meta_ads"
    SPEND="ad_spend"

class DatasetsGold(enum):
    REACH="gold_demographic_reach"
    DAILY_METRICS="gold_ad_daily_metrics"
    SATURATION="gold_saturation_curve"
    TOTAL_METRICS="gold_ad_total_metrics"

class TablesSQl(enum):
    SPEND="daily_spend"
    PRODUCT_MAP="ad_product_mapping"
    PRODUCTS="products"

