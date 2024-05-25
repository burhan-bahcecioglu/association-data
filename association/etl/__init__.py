"""ETL sub package"""
import os
from typing import Optional, Union

from libretranslatepy import LibreTranslateAPI
from pyspark.sql import functions as F, types as T

from association import dbutils, ROOT_PATH
from association.utils import LoggerMixin

try:
    from typing import LiteralString
except ImportError:
    from typing_extensions import LiteralString


LOG = LoggerMixin().log
INPUT_PATH = "dbfs:/mnt/input"
ETL_PATH = os.path.join(ROOT_PATH, "etl")
STORE_PATH = os.path.join(ETL_PATH, "stores")
STORE_TRANSLATIONS_PATH = os.path.join(ETL_PATH, "store_translations")
PRODUCT_PATH = os.path.join(ETL_PATH, "products")
PRODUCT_TRANSLATIONS_PATH = os.path.join(ETL_PATH, "product_translations")
TRANSACTIONAL_SALES_PATH = os.path.join(ETL_PATH, "transactional_sales")
LIBRE_TRANSLATE_API_URL = "https://translate.terraprint.co/"
LIBRE_TRANSLATE_API_KEY = ""


def find_raw_path(
        target_date: str, feed_name: str
) -> Optional[Union[str, LiteralString, bytes]]:
    """
    finds path target date

    :param target_date: targeted date of the transactional_sales feed.
    :param feed_name: name of the feed to search for.
        defaults to transactional_sales.csv
    :returns: raw csv path for the given feed name at the targeted date.
    """
    final_path_to_read = None

    for path in dbutils.fs.ls(INPUT_PATH):
        if target_date in path.path:
            folder_path = os.path.join(path.path)
            for feed_names in dbutils.fs.ls(folder_path):
                if feed_name in feed_names.path:
                    final_path_to_read = os.path.join(feed_names.path)

    return final_path_to_read


libre_translate = LibreTranslateAPI(
    url=LIBRE_TRANSLATE_API_URL, api_key=LIBRE_TRANSLATE_API_KEY
)


@F.udf(T.StringType())
def translate_from_uk_to_en(text: str):
    """translation udf from ukrainian to english"""
    return libre_translate.translate(q=text, source='uk', target="en")
