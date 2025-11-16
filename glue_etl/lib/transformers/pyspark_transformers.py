# lib/transformers/pyspark_transformers.py
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
from lib.utils.logger import Logger
logger = Logger()

class AnimalsTransformerSpark:
    """
    Transformer for animals data using PySpark.
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def transform_animals(self, df: DataFrame) -> DataFrame:
        """
        Transforms animals page data.
        
        Args:
            df: DataFrame with animals data
            
        Returns:
            Transformed DataFrame
        """
        logger.info("Transforming animals data")
        
        # Add reproductive status column
        df_transformed = df.withColumn(
            'status_reprodutivo',
            F.when(F.col('total_deliveries') == 0, F.lit('Novilha'))
             .when(F.col('total_deliveries') == 1, F.lit('Primipara'))
             .otherwise(F.lit('Multípara'))
        )
        
        logger.info("Finished transforming animals data")
        
        return df_transformed


class ColeirasReportandoTransformerSpark:
    """
    Transformer for collar reporting data using PySpark.
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def transform_coleiras_reportando(self, df: DataFrame) -> DataFrame:
        """
        Transforms collar reporting data.
        
        Args:
            df: DataFrame with collar reporting data
            
        Returns:
            Transformed DataFrame
        """
        logger.info("Transforming coleiras reportando data")
        
        # Add any specific transformations for collar reporting here
        # For now, just return the dataframe as-is
        df_transformed = df
        
        logger.info("Finished transforming coleiras reportando data")
        
        return df_transformed


class ColeirasVinculadasTransformerSpark:
    """
    Transformer for linked collars data using PySpark.
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def transform_coleiras_vinculadas(self, df: DataFrame) -> DataFrame:
        """
        Transforms linked collars data.
        
        Args:
            df: DataFrame with linked collars data
            
        Returns:
            Transformed DataFrame
        """
        logger.info("Transforming coleiras vinculadas data")
        
        # Add any specific transformations for linked collars here
        # For now, just return the dataframe as-is
        df_transformed = df
        
        logger.info("Finished transforming coleiras vinculadas data")
        
        return df_transformed


class ContagemAcessosPorUsuarioTransformerSpark:
    """
    Transformer for user access counts using PySpark.
    Combines users_active and farm_states data to count manager accesses.
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def transform_contagem_acessos(self, df_active: DataFrame, df_farm_states: DataFrame) -> DataFrame:
        """
        Transforms user activity data to count accesses per user per month.
        
        Args:
            df_active: DataFrame with user activity data
            df_farm_states: DataFrame with farm states data
            
        Returns:
            DataFrame with access counts per user per month (managers only)
        """
        logger.info("Starting Contagem Acessos por Usuario transformation")
        
        # 1. Merge df_active with df_farm_states to get manager_user_id
        # Select only needed columns from farm_states
        df_farm_states_subset = df_farm_states.select(
            F.col("code"),
            F.col("manager_user_id")
        )
        
        # Join on farm_code = code
        df_active_merged = df_active.join(
            df_farm_states_subset,
            df_active["farm_code"] == df_farm_states_subset["code"],
            how="left"
        ).drop("code")
        
        logger.info("Merged users_active with farm_states")
        
        # 2. Create manager_acess boolean column
        df_active_merged = df_active_merged.withColumn(
            "manager_acess",
            F.col("manager_user_id") == F.col("user_id")
        )
        
        # 3. Create year_month column from acesso_timestamp
        df_active_merged = df_active_merged.withColumn(
            "year_month",
            F.date_format(F.col("acesso_timestamp"), "yyyy-MM")
        )
        
        logger.info("Added manager_acess and year_month columns")
        
        # 4. Group by year_month, user_id, manager_acess and count
        df_grouped = df_active_merged.groupBy("year_month", "user_id", "manager_acess") \
            .agg(F.count("*").alias("count"))
        
        # 5. Pivot manager_acess to create separate columns for True/False
        df_pivoted = df_grouped.groupBy("year_month", "user_id") \
            .pivot("manager_acess", [True, False]) \
            .agg(F.first("count"))
        
        # 6. Rename columns and fill nulls with 0
        df_counts = df_pivoted \
            .withColumnRenamed("true", "Contagem_Manager_True") \
            .withColumnRenamed("false", "Contagem_Manager_False") \
            .fillna(0, subset=["Contagem_Manager_True", "Contagem_Manager_False"])
        
        logger.info(f"Grouped and pivoted data")
        
        # 7. Get list of unique manager_user_ids
        lista_de_managers = df_active_merged.select("manager_user_id") \
            .distinct() \
            .rdd.flatMap(lambda x: x) \
            .collect()
        
        # 8. Filter to keep only managers
        df_managers_counts = df_counts.filter(
            F.col("user_id").isin(lista_de_managers)
        )
        
        logger.info("Filtered to managers only")
        logger.info("Finished Contagem Acessos por Usuario transformation")
        
        return df_managers_counts
