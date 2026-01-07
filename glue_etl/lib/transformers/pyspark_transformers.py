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


class UsersActiveTransformerSpark:
    """
    Transformer for users_active data using PySpark.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.page_ptbr_map = {
            'unknown': 'desconhecido',
            'farm-animal-daily-monitoring': 'animal-monitoramento-diario',
            'farm-dashboard': 'painel',
            'farm-animal-reproduction': 'animal-reproducao',
            'farm-infirmary-no-diagnosis': 'enfermaria-sem-diagnostico',
            'farm-herd-animals': 'rebanho-animais',
            'farm-reproduction-protocol-detail-animals': 'reproducao-detalhe-do-protocolo-animais',
            'farm-health-challenge-animals': 'saude-desafio-animais',
            'farm-notifications-herd': 'notificacoes-rebanho',
            'farm-nutrition-monitored-batches': 'nutricao-monitorados-lotes',
            'farm-reproduction-heat-alerts-tabs': 'reproducao-alertas-de-cio-abas',
            'farm-reproduction-calving-scheduled': 'reproducao-parto-agendado',
            'farm-reproduction-pre-calving': 'reproducao-pre-parto',
            'farm-reproduction-protocols': 'reproducao-protocolos',
            'farm-reproduction-pregnancy-confirmation': 'reproducao-confirmacao-de-gestacao',
            'farm-reproduction-heat-detail': 'reproducao-cio-detalhe',
            'farm-import-imports': 'importar-importacoes',
            'farm-animal-health': 'animal-saude',
            'farm-batch-monitoring': 'lote-monitoramento',
            'farm-monitoring-c-tech-all': 'monitoramento-c-tec-todos',
            'farm-reproduction-inseminations': 'reproducao-inseminacoes',
            'farm-reproduction-protocols-tabs': 'reproducao-protocolos-abas',
            'farm-animal-suspected': 'animal-suspeito',
            'farm-reproduction-heat-alerts': 'reproducao-alertas-de-cio',
            'farm-health-pasture': 'saude-pasto',
            'farm-health-dashboard': 'saude-painel',
            'farm-health-treatments': 'saude-tratamentos',
            'farm-reproduction-bulls': 'reproducao-touros',
            'farm-reproduction-semen': 'reproducao-semen',
            'farm-reproduction-insemination-detail': 'reproducao-inseminacao-detalhe',
            'farm-reproduction-pregnancy-confirmation-tabs': 'reproducao-confirmacao-de-gestacao-abas',
            'farm-reproduction-calving-scheduled-tabs': 'reproducao-parto-agendado-abas',
            'farm-reproduction-pre-calving-tabs': 'reproducao-pre-parto-abas',
            'farm-health-treatment-detail': 'saude-tratamento-detalhe',
            'farm-health-withdrawal-milk': 'saude-carencia-leite',
            'farm-milk-control-dashboard': 'leite-controle-painel',
            'farm-milk-control-production': 'leite-controle-producao',
            'farm-milk-control-analysis': 'leite-controle-analise',
            'farm-milk-control-quality': 'leite-controle-qualidade',
            'farm-milk-control-production-per-animal': 'leite-controle-producao-por-animal',
            'farm-milk-control-production-per-batch': 'leite-controle-producao-por-lote',
            'farm-milk-control-induced-marked': 'leite-controle-induzido-marcados',
            'farm-milk-control-induced-realized': 'leite-controle-induzido-realizado',
            'farm-nutrition-dashboard': 'nutricao-painel',
            'farm-nutrition-diet': 'nutricao-dieta',
            'farm-nutrition-ingredient': 'nutricao-ingrediente',
            'farm-nutrition-intake': 'nutricao-ingestao',
            'farm-nutrition-formulation': 'nutricao-formulacao',
            'farm-nutrition-formulation-detail': 'nutricao-formulacao-detalhe',
            'farm-animal': 'animal',
            'farm-animal-detail': 'animal-detalhe',
            'farm-animal-heat-alerts': 'animal-alertas-de-cio',
            'farm-animal-reproduction-history': 'animal-reproducao-historico',
            'farm-animal-health-history': 'animal-saude-historico',
            'farm-animal-health-treatments': 'animal-saude-tratamentos',
            'farm-animal-health-treatment-detail': 'animal-saude-tratamento-detalhe',
            'farm-animal-health-mastitis': 'animal-saude-mastite',
            'farm-animal-health-mastitis-dynamics': 'animal-saude-mastite-dinamica',
            'farm-animal-health-gynecologic': 'animal-saude-ginecologico',
            'farm-animal-milk-control': 'animal-leite-controle',
            'farm-animal-milk-control-analysis': 'animal-leite-controle-analise',
            'farm-animal-milk-control-quality': 'animal-leite-controle-qualidade',
            'farm-animal-milk-control-production': 'animal-leite-controle-producao',
            'farm-animal-milk-control-production-curve': 'animal-leite-controle-producao-curva',
            'farm-animal-milk-control-production-accumulated': 'animal-leite-controle-producao-acumulado',
            'farm-animal-milk-control-production-average': 'animal-leite-controle-producao-media',
            'farm-animal-milk-control-production-per-days': 'animal-leite-controle-producao-por-dias',
            'farm-animal-milk-control-production-per-dim': 'animal-leite-controle-producao-por-dim',
            'farm-animal-cowcomfort': 'animal-cowcomfort',
            'farm-animal-cowcomfort-wellness': 'animal-cowcomfort-bem-estar',
            'farm-animal-cowcomfort-stress': 'animal-cowcomfort-estresse',
            'farm-animal-cowcomfort-thermal': 'animal-cowcomfort-termico',
            'farm-animal-cowcomfort-temperature': 'animal-cowcomfort-temperatura',
            'farm-animal-cowcomfort-humidity': 'animal-cowcomfort-umidade',
            'farm-animal-cowcomfort-variability': 'animal-cowcomfort-variabilidade',
            'farm-animal-cowcomfort-rest': 'animal-cowcomfort-descanso',
            'farm-animal-cowcomfort-breathing': 'animal-cowcomfort-respiracao',
            'farm-animal-cowcomfort-movements': 'animal-cowcomfort-movimentos',
            'farm-animal-cowcomfort-analysis': 'animal-cowcomfort-analise',
            'farm-animal-cowcomfort-points': 'animal-cowcomfort-pontos',
            'farm-animal-cowcomfort-hospital': 'animal-cowcomfort-hospital',
            'farm-animal-cowcomfort-recovered': 'animal-cowcomfort-recuperado',
            'farm-animal-cowcomfort-critical': 'animal-cowcomfort-critico',
            'farm-animal-cowcomfort-suspected': 'animal-cowcomfort-suspeito',
            'farm-animal-cowcomfort-detected': 'animal-cowcomfort-detectado',
            'farm-animal-cowcomfort-diagnosed': 'animal-cowcomfort-diagnosticado',
            'farm-animal-cowcomfort-confirmed': 'animal-cowcomfort-confirmado',
            'farm-animal-cowcomfort-open': 'animal-cowcomfort-abrir',
            'farm-animal-cowcomfort-performed': 'animal-cowcomfort-realizado',
            'farm-animal-cowcomfort-return': 'animal-cowcomfort-retorno',
            'farm-animal-cowcomfort-departure': 'animal-cowcomfort-saida',
            'farm-animal-cowcomfort-stock': 'animal-cowcomfort-estoque',
            'farm-animal-cowcomfort-units': 'animal-cowcomfort-unidades',
            'farm-animal-cowcomfort-network': 'animal-cowcomfort-rede',
            'farm-animal-cowcomfort-tokens': 'animal-cowcomfort-tokens',
            'farm-animal-cowcomfort-tokenized': 'animal-cowcomfort-tokenizados',
            'farm-animal-cowcomfort-tokenized-animals': 'animal-cowcomfort-tokenizados-animais',
            'farm-animal-birth-index': 'animal-nascimento-indice',
            'farm-animal-birth': 'animal-nascimento',
            'farm-animal-birth-history': 'animal-nascimento-historico',
            'farm-animal-birth-weight': 'animal-nascimento-peso',
            'farm-animal-baby': 'animal-bebe',
            'farm-animal-baby-weaning': 'animal-bebe-desmame',
            'farm-animal-baby-weighing': 'animal-bebe-pesagem',
            'farm-animal-baby-growth': 'animal-bebe-crescimento',
            'farm-animal-rearing': 'animal-cria',
            'farm-rearing-dashboard': 'cria-painel',
            'farm-rearing-batches': 'cria-lotes',
            'farm-rearing-batch-detail': 'cria-lote-detalhe',
            'farm-rearing-mortality': 'cria-mortalidade',
            'farm-rearing-weaning': 'cria-desmame',
            'farm-rearing-weighing': 'cria-pesagem',
            'farm-rearing-growth': 'cria-crescimento',
            'farm-rearing-events': 'cria-eventos',
            'farm-rearing-event-detail': 'cria-evento-detalhe',
            'farm-rearing-management': 'cria-gestao',
            'farm-rearing-stock': 'cria-estoque',
            'farm-rearing-milk': 'cria-leite',
            'farm-rearing-milk-discard': 'cria-leite-descarte',
            'farm-rearing-milk-discard-control': 'cria-leite-descarte-controle',
            'farm-rearing-milk-discard-control-dashboard': 'cria-leite-descarte-controle-painel',
            'farm-rearing-milk-discard-control-analysis': 'cria-leite-descarte-controle-analise',
            'farm-rearing-milk-discard-control-quality': 'cria-leite-descarte-controle-qualidade',
            'farm-rearing-milk-discard-control-production': 'cria-leite-descarte-controle-producao',
            'farm-rearing-milk-discard-control-production-per-animal': 'cria-leite-descarte-controle-producao-por-animal',
            'farm-rearing-milk-discard-control-production-per-batch': 'cria-leite-descarte-controle-producao-por-lote',
            'farm-rearing-milk-discard-control-induced-marked': 'cria-leite-descarte-controle-induzido-marcados',
            'farm-rearing-milk-discard-control-induced-realized': 'cria-leite-descarte-controle-induzido-realizado',
            'farm-reproduction-dashboard': 'reproducao-painel',
            'farm-reproduction-heat-alerts-tabs-animals': 'reproducao-alertas-de-cio-abas-animais',
            'farm-reproduction-heat-alerts-animals': 'reproducao-alertas-de-cio-animais',
            'farm-reproduction-heat-detail-animals': 'reproducao-cio-detalhe-animais',
            'farm-reproduction-inseminations-animals': 'reproducao-inseminacoes-animais',
            'farm-reproduction-protocols-animals': 'reproducao-protocolos-animais',
            'farm-reproduction-pregnancy-confirmation-animals': 'reproducao-confirmacao-de-gestacao-animais',
            'farm-reproduction-calving-scheduled-animals': 'reproducao-parto-agendado-animais',
            'farm-reproduction-pre-calving-animals': 'reproducao-pre-parto-animais',
            'farm-reproduction-eligible-animals': 'reproducao-elegivel-animais',
            'farm-reproduction-heat-alerts-marked': 'reproducao-alertas-de-cio-marcados',
            'farm-reproduction-inseminations-marked': 'reproducao-inseminacoes-marcados',
            'farm-reproduction-protocols-marked': 'reproducao-protocolos-marcados',
            'farm-reproduction-pregnancy-confirmation-marked': 'reproducao-confirmacao-de-gestacao-marcados',
            'farm-reproduction-calving-scheduled-marked': 'reproducao-parto-agendado-marcados',
            'farm-reproduction-pre-calving-marked': 'reproducao-pre-parto-marcados',
            'farm-reproduction-conception-rate': 'reproducao-taxa-concepcao',
            'farm-reproduction-interval': 'reproducao-intervalo',
            'farm-reproduction-losses': 'reproducao-perdas',
            'farm-reproduction-gestational-losses': 'reproducao-gestacional-perdas',
            'farm-reproduction-cycles': 'reproducao-ciclos',
            'farm-reproduction-mating': 'reproducao-acasalamento',
            'farm-reproduction-embryo': 'reproducao-embriao',
            'farm-reproduction-donor': 'reproducao-doadora',
            'farm-reproduction-inseminator': 'reproducao-inseminador',
            'farm-reproduction-projection': 'reproducao-projecao',
            'farm-reproduction-projection-monthly': 'reproducao-projecao-mensal',
            'farm-reproduction-projection-yearly': 'reproducao-projecao-anual',
            'farm-reproduction-predictions': 'reproducao-predicoes',
            'farm-reproduction-predictions-model': 'reproducao-predicoes-modelo',
            'farm-reproduction-predictions-models': 'reproducao-predicoes-modelos',
            'farm-reproduction-hormone': 'reproducao-hormonio',
            'farm-reproduction-induction': 'reproducao-inducao',
            'farm-reproduction-induction-executed': 'reproducao-inducao-executado',
            'farm-reproduction-induction-expired': 'reproducao-inducao-expirado',
            'farm-reproduction-protocol-detail': 'reproducao-detalhe-do-protocolo',
            'farm-reproduction-protocol-detail-tabs': 'reproducao-detalhe-do-protocolo-abas',
            'farm-reproduction-protocol-detail-markings': 'reproducao-detalhe-do-protocolo-marcacoes',
            'farm-reproduction-protocol-detail-performed': 'reproducao-detalhe-do-protocolo-realizado',
            'farm-reproduction-protocol-detail-check2': 'reproducao-detalhe-do-protocolo-check2',
            'farm-reproduction-protocol-detail-check3': 'reproducao-detalhe-do-protocolo-check3',
            'farm-settings': 'configuracoes',
            'farm-settings-roles': 'configuracoes-papeis',
            'farm-settings-users': 'configuracoes-usuarios',
            'farm-settings-permissions': 'configuracoes-permissoes',
            'farm-monitoring': 'monitoramento',
            'farm-monitoring-tokenized-animals': 'monitoramento-tokenizados-animais',
            'farm-monitoring-c-com': 'monitoramento-c-com',
            'farm-cow-analytics-health-mastitis-dynamics': 'vaca-analises-saude-mastite-dinamica',
        }

    def transform_users_active(self, df: DataFrame) -> DataFrame:
        """
        Adds 'page_traduzida' column to users_active data.

        Args:
            df: DataFrame with users_active data

        Returns:
            Transformed DataFrame
        """
        logger.info("Transforming users_active data")
        
        # Debug: Log input DataFrame info
        input_count = df.count()
        logger.info(f"Input DataFrame row count: {input_count}")
        logger.info(f"Input DataFrame schema: {df.schema}")
        
        # Show sample of input data
        if input_count > 0:
            logger.info("Sample input data (first 5 rows):")
            df.show(5, truncate=False)
            
            # Show unique page values
            unique_pages = df.select("page").distinct().count()
            logger.info(f"Number of unique page values: {unique_pages}")
            df.select("page").distinct().show(20, truncate=False)

        # Build the case_when expression properly
        case_when = None
        for en, pt in self.page_ptbr_map.items():
            if case_when is None:
                case_when = F.when(F.col("page") == en, F.lit(pt))
            else:
                case_when = case_when.when(F.col("page") == en, F.lit(pt))
        
        # Add the page_traduzida column
        if case_when is not None:
            df_transformed = df.withColumn("page_traduzida", case_when.otherwise(F.col("page")))
        else:
            # If no mappings exist, just copy the page column
            logger.warning("No page mappings found! Using original page column")
            df_transformed = df.withColumn("page_traduzida", F.col("page"))

        # Debug: Log output DataFrame info
        output_count = df_transformed.count()
        logger.info(f"Output DataFrame row count: {output_count}")
        logger.info(f"Output DataFrame schema: {df_transformed.schema}")
        
        # Show sample of output data
        if output_count > 0:
            logger.info("Sample output data with page_traduzida (first 5 rows):")
            df_transformed.show(5, truncate=False)
        else:
            logger.error("⚠️ WARNING: Output DataFrame is EMPTY!")

        logger.info("Finished transforming users_active data")

        return df_transformed
