import sys
from datetime import datetime
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from lib.extractors.glue_catalog_extractor import GlueCatalogExtractor
from lib.queries.spark_queries import SparkQueries
from lib.transformers.pyspark_transformers import (
    AnimalsTransformerSpark,
    ColeirasReportandoTransformerSpark,
    ColeirasVinculadasTransformerSpark,
    ContagemAcessosPorUsuarioTransformerSpark,
)
from lib.loaders.s3_loader import S3Loader
from lib.utils.logger import Logger

def main():
    # -----------------------------------------
    # Glue job init
    # -----------------------------------------
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
    spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")

    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    logger = Logger()
    logger.info("Starting Glue ETL job")
    logger.info(f"Start time: {datetime.now()}")

    # -----------------------------------------
    # Configs
    # -----------------------------------------
    GLUE_DB = "smartfarm-dev"
    OUTPUT_BUCKET = "s3://intel-business-intelligence/bi-database"

    # -----------------------------------------
    # Extract — load base tables from Glue Catalog
    # -----------------------------------------
    extractor = GlueCatalogExtractor(glueContext, spark, db=GLUE_DB)
    # Tables used in Athena queries
    tables = [
        "animals",
        "farms",
        "collars",
        "animal_collar",
        "animal_breed",
        "user_activities",
        "users",
        "farm_user",
        "company_user",
        "farm_addresses",
        "cities",
        "states",
        "countries",
        "company_farm",
        "companies",
        "company_portfolio_farm",
        "company_portfolios",
        "collar_farm",
        "c_envs"
    ]

    logger.info("Loading base tables from Glue Catalog")
    dfs = extractor.load_tables(tables)  # dict: name -> DataFrame

    # -----------------------------------------
    # Build flattened query DataFrames
    # -----------------------------------------
    queries = SparkQueries(spark, dfs)

    logger.info("Building query DataFrames")

    df_coleiras_reportando = queries.build_coleiras_reportando()
    df_coleiras_vinculadas = queries.build_coleiras_vinculadas()
    df_animals = queries.build_animals()
    df_users_active = queries.build_users_active()
    df_farm_states = queries.build_farm_states()
    df_users = queries.build_users()
    df_c_env_list = queries.build_c_env_list()


    logger.info("All query DataFrames built successfully")
    
    # -----------------------------------------
    # Transformations
    # -----------------------------------------
    logger.info("Applying transformations")

    animals_transformer = AnimalsTransformerSpark(spark)
    coleiras_reportando_transformer = ColeirasReportandoTransformerSpark(spark)
    coleiras_vinculadas_transformer = ColeirasVinculadasTransformerSpark(spark)
    contagem_acessos_transformer = ContagemAcessosPorUsuarioTransformerSpark(spark)

    df_animals_t = animals_transformer.transform_animals(df_animals)
    df_coleiras_reportando_t = coleiras_reportando_transformer.transform_coleiras_reportando(df_coleiras_reportando)
    df_coleiras_vinculadas_t = coleiras_vinculadas_transformer.transform_coleiras_vinculadas(df_coleiras_vinculadas)
    df_contagem_acessos_t = contagem_acessos_transformer.transform_contagem_acessos(df_users_active, df_farm_states)
    logger.info("All transformations applied successfully")

    # -----------------------------------------
    # Write outputs (parquet + single CSV per page)
    # -----------------------------------------
    loader = S3Loader(spark)

    """
    bloco para escrever os datasets transformados de teste no s3

    try:
        logger.info("Writing alerts transform parquet")
        df_alertas_t = loader.write_parquet(df_alertas_t, parquet_path, final_name="alertas_transformed_teste")
        logger.info("Alerts parquet written successfully")

        logger.info("Writing validador transform parquet")
        df_validador_t = loader.write_parquet(df_validador_t, parquet_path, final_name="validador_transformed_teste")
        logger.info("Validador parquet written successfully")

        logger.info("Writing concepcao transform parquet")
        df_concepcao_t = loader.write_parquet(df_concepcao_t, parquet_path, final_name="concepcao_transformed_teste")
        logger.info("Concepcao parquet written successfully")

        logger.info("Writing protocol alerts transform parquet")
        df_protocols = loader.write_parquet(df_protocols, parquet_path, final_name="protocol_alerts_transformed_teste")
        logger.info("Protocol alerts parquet written successfully")

    except Exception as e:
        logger.error(f"Error writing alertas parquet: {e}")
    """

    
    outputs = {
       "coleiras_reportando": (df_coleiras_reportando_t, 'page_coleiras_reportando'),
        "coleiras_vinculadas": (df_coleiras_vinculadas_t, 'page_coleiras_vinculadas'),
        "animals": (df_animals_t, 'page_animals'),
        "users_active": (df_users_active, 'users_active'),
        "farm_states": (df_farm_states, 'farm_states'),
        "contagem_acessos": (df_contagem_acessos_t, 'contagem_acessos_por_usuario'),
        "users": (df_users, 'users'),
        "c_env_list": (df_c_env_list, 'c_env_list')
    }

    for page, (df_page, file_name) in outputs.items():

        parquet_path = f"{OUTPUT_BUCKET}/etl_parquet/"
        csv_path = f"{OUTPUT_BUCKET}/etl_csv/"
        
        logger.info(f"Writing {page} parquet to {parquet_path}")
        loader.write_parquet(df_page, parquet_path, file_name)

        logger.info(f"Writing {page} csv to {csv_path}")
        loader.write_csv_singlefile(df_page, csv_path, file_name)

    logger.info("ETL job finished successfully ✅")
    logger.info(f"End time: {datetime.now()}")

    job.commit()

if __name__ == "__main__":
    main()
