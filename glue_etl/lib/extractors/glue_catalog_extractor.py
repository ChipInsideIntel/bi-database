# lib/extractors/glue_catalog_extractor.py
from awsglue.context import GlueContext
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

class GlueCatalogExtractor:
    def __init__(self, glue_context: GlueContext, spark_session: SparkSession, db: str):
        self.glue_context = glue_context
        self.spark = spark_session
        self.db = db

    def load_table(self, table_name: str) -> DataFrame:
        # Read table from Glue Catalog (database.table_name)
        dyf = self.glue_context.create_dynamic_frame.from_catalog(
            database=self.db,
            table_name=table_name
        )
        df = dyf.toDF()
        return df

    def load_tables(self, table_names: list) -> dict:
        d = {}
        for t in table_names:
            d[t] = self.load_table(t)
            # cache to speed repeated operations
            d[t].persist()
        return d
