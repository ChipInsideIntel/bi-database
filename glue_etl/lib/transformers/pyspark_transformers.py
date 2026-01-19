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
            'farm-animal-batch-history': 'animais-animal-historico-de-lotes',
            'farm-animal-birth-index': 'animais-animal-indice-de-parto',
            'farm-animal-breathing-index': 'animais-animal-indice-de-respiracao',
            'farm-animal-collar-history': 'animais-animal-historico-de-coleiras',
            'farm-animal-daily-monitoring': 'animais-animal-monitoramento-dia',
            'farm-animal-health': 'animais-animal-saude',
            'farm-animal-health-index': 'animais-animal-indice-de-saude',
            'farm-animal-heat-index': 'animais-animal-indice-de-cio',
            'farm-animal-hourly-monitoring': 'animais-animal-monitoramento-hora',
            'farm-animal-notes': 'animais-animal-notas',
            'farm-animal-production': 'animais-animal-producao',
            'farm-animal-reproduction': 'animais-animal-reproducao',
            'farm-animal-weighing': 'animais-animal-pesagem',
            'farm-batch-accumulated': 'rebanho-lote-acumulado',
            'farm-batch-diet': 'rebanho-lote-dieta-do-lote',
            'farm-batch-monitoring': 'rebanho-lote-monitoramento-do-lote',
            'farm-batch-profile-consumption': 'rebanho-lote-perfil-e-consumo',
            'farm-batch-review-animals': 'rebanho-lote-animais-para-revisao',
            'farm-batch-variability': 'rebanho-lote-variabilidade',
            'farm-cow-analytics': 'cow-analytics',
            'farm-cow-analytics-conception-per-bull-semen': 'cow-analytics-concepcao-por-touro-semen',
            'farm-cow-analytics-conception-per-days-in-lactation': 'cow-analytics-concepcao-por-del',
            'farm-cow-analytics-conception-per-inseminator': 'cow-analytics-concepcao-por-inseminador',
            'farm-cow-analytics-conception-per-number-of-services': 'cow-analytics-concepcao-por-numero-de-servicos',
            'farm-cow-analytics-conception-per-service-type': 'cow-analytics-concepcao-por-tipo-de-servico',
            'farm-cow-analytics-conception-rate': 'cow-analytics-concepcao-taxa-de-concepcao',
            'farm-cow-analytics-discard-animal-departure': 'cow-analytics-descarte-de-animais-saida-de-animais',
            'farm-cow-analytics-discard-discarded-animals': 'cow-analytics-descarte-de-animais-animais-descartados',
            'farm-cow-analytics-discard-marked-for-discard': 'cow-analytics-descarte-de-animais-marcada-para-descarte',
            'farm-cow-analytics-discard-moment': 'cow-analytics-descarte-de-animais-momento-do-descarte',
            'farm-cow-analytics-discard-rate': 'cow-analytics-descarte-de-animais-taxa-de-descarte',
            'farm-cow-analytics-discard-reason': 'cow-analytics-descarte-de-animais-motivo-de-descarte',
            'farm-cow-analytics-discard-total': 'cow-analytics-descarte-de-animais-total-de-descarte',
            'farm-cow-analytics-eligible-cows': 'cow-analytics-vacas-aptas',
            'farm-cow-analytics-gestational-loss-rate': 'cow-analytics-perdas-gestacionais-taxa-de-perdas',
            'farm-cow-analytics-gestational-losses-by-dim': 'cow-analytics-perdas-gestacionais-perdas-por-del',
            'farm-cow-analytics-gestational-losses-per-bull': 'cow-analytics-perdas-gestacionais-perdas-por-touro',
            'farm-cow-analytics-gestational-monthly-losses': 'cow-analytics-perdas-gestacionais-perdas-mensais',
            'farm-cow-analytics-gestational-period-losses': 'cow-analytics-perdas-gestacionais-perdas-por-periodo',
            'farm-cow-analytics-gestational-registered-losses': 'cow-analytics-perdas-gestacionais-perdas-registradas',
            'farm-cow-analytics-health-alert': 'cow-analytics-saude-alerta-de-saude',
            'farm-cow-analytics-health-alert-count': 'cow-analytics-saude-contagem-de-alertas',
            'farm-cow-analytics-health-diagnosed': 'cow-analytics-saude-animais-diagnosticados',
            'farm-cow-analytics-health-history': 'cow-analytics-saude-historico-de-casos',
            'farm-cow-analytics-health-hospital-time': 'cow-analytics-saude-tempo-de-enfermaria',
            'farm-cow-analytics-health-mastitis': 'cow-analytics-saude-mastite',
            'farm-cow-analytics-health-mastitis-dynamics': 'cow-analytics-saude-dinamica-de-mastite',
            'farm-cow-analytics-health-occurrences': 'cow-analytics-saude-total-de-ocorrencias',
            'farm-cow-analytics-heat-return': 'cow-analytics-retorno-de-cio',
            'farm-cow-analytics-herd-average-dim': 'cow-analytics-rebanho-del-medio',
            'farm-cow-analytics-herd-general': 'cow-analytics-rebanho-rebanho-geral',
            'farm-cow-analytics-herd-lactating-animals': 'cow-analytics-rebanho-animais-em-lactacao',
            'farm-cow-analytics-herd-lactating-projection': 'cow-analytics-rebanho-projecao-de-vacas-em-lactacao',
            'farm-cow-analytics-herd-replacement-rate': 'cow-analytics-rebanho-taxa-de-reposicao',
            'farm-cow-analytics-herd-reproductive-distribution': 'cow-analytics-rebanho-distribuicao-reprodutiva',
            'farm-cow-analytics-marked-all-markings': 'cow-analytics-marcadas-como-todas-marcacoes',
            'farm-cow-analytics-marked-for-discard': 'cow-analytics-marcadas-como-para-descarte',
            'farm-cow-analytics-marked-for-lactation-induction': 'cow-analytics-marcadas-como-para-inducao-de-lactacao',
            'farm-cow-analytics-marked-for-protocol': 'cow-analytics-marcadas-como-para-protocolo',
            'farm-cow-analytics-marked-unfit-for-reproduction': 'cow-analytics-marcadas-como-inapta-para-reproducao',
            'farm-cow-analytics-no-service': 'cow-analytics-sem-servico',
            'farm-cow-analytics-problem-cows-critical-dim': 'cow-analytics-vacas-problema-del-alto-sem-prenhez',
            'farm-cow-analytics-rearing-average-birth-weight': 'cow-analytics-cria-recria-peso-medio-nascimento',
            'farm-cow-analytics-rearing-average-daily-gain': 'cow-analytics-cria-recria-ganho-medio-diario',
            'farm-cow-analytics-rearing-average-weaning-weight': 'cow-analytics-cria-recria-peso-medio-ao-desaleitamento',
            'farm-cow-analytics-rearing-growth-curve': 'cow-analytics-cria-recria-curva-de-crescimento',
            'farm-cow-analytics-rearing-health': 'cow-analytics-cria-recria-saude',
            'farm-cow-analytics-rearing-mortality': 'cow-analytics-cria-recria-mortalidade',
            'farm-cow-analytics-rearing-open': 'cow-analytics-cria-recria-vazias-aos-15-meses',
            'farm-cow-analytics-services-cycles': 'cow-analytics-servicos-ciclos-de-servicos',
            'farm-cow-analytics-services-dim-first-service': 'cow-analytics-servicos-del-no-1-servico',
            'farm-cow-analytics-services-heat-detected': 'cow-analytics-servicos-cios-detectados',
            'farm-cow-analytics-services-interval': 'cow-analytics-servicos-intervalo-entre-servicos',
            'farm-cow-analytics-services-monthly': 'cow-analytics-servicos-servicos-mensais',
            'farm-cow-analytics-services-performed': 'cow-analytics-servicos-realizados',
            'farm-cow-analytics-services-rate': 'cow-analytics-servicos-taxa-de-servico',
            'farm-cow-analytics-transition-challenged': 'cow-analytics-transicao-ppi-desafiadas',
            'farm-cow-analytics-transition-ppi': 'cow-analytics-transicao-ppi-do-rebanho',
            'farm-cow-analytics-wellness-nutrition-and-intake': 'cow-analytics-bem-estar-geral-nutricao-e-consumo',
            'farm-cow-analytics-wellness-thermal-comfort-and-rest': 'cow-analytics-bem-estar-geral-conforto-termico-e-descanso',
            'farm-dashboard': 'dashboard',
            'farm-export': 'exportar-dados',
            'farm-handling-events-record': 'eventos-de-manejo-registro-de-manejo',
            'farm-health-applications-performed': 'saude-aplicacoes-realizadas',
            'farm-health-applications-scheduled': 'saude-aplicacoes-programadas',
            'farm-health-challenge-animals': 'saude-animais-em-desafio',
            'farm-health-event-history': 'saude-historico-de-eventos',
            'farm-health-infirmary-baby-diagnosis': 'saude-enfermaria-baby-com-diagnostico',
            'farm-health-infirmary-baby-no-diagnosis': 'saude-enfermaria-baby-sem-diagnostico',
            'farm-health-infirmary-baby-recovered': 'saude-enfermaria-baby-recuperados',
            'farm-health-protocol-detail-applications': 'saude-protocolos-sanitarios-protocolo-aplicacoes',
            'farm-health-protocol-detail-behavior': 'saude-protocolos-sanitarios-protocolo-comportamento-animal',
            'farm-health-protocol-models': 'saude-protocolos-sanitarios-modelos-de-protocolo',
            'farm-health-protocols': 'saude-protocolos-sanitarios-meus-protocolos',
            'farm-health-withdrawal-meat': 'saude-animais-em-carencia-carencia-de-carne',
            'farm-health-withdrawal-milk': 'saude-animais-em-carencia-carencia-de-leite',
            'farm-herd-animals': 'rebanho-animais',
            'farm-herd-batches': 'rebanho-lotes',
            'farm-herd-divergence': 'rebanho-divergencia-de-lotes',
            'farm-herd-manage-batches': 'rebanho-troca-de-lotes',
            'farm-import-files': 'importar-dados-arquivos',
            'farm-import-imports': 'importar-dados-importacoes',
            'farm-infirmary-no-diagnosis': 'enfermaria-sem-diagnostico',
            'farm-infirmary-recovered': 'enfermaria-recuperados',
            'farm-infirmary-with-diagnosis': 'enfermaria-com-diagnostico',
            'farm-milk-control-analysis': 'controle-leiteiro-geral',
            'farm-milk-control-dashboard': 'controle-leiteiro-dashboard',
            'farm-milk-control-induced-marked': 'controle-leiteiro-inducao-de-lactacao-marcados-para-inducao',
            'farm-milk-control-induced-realized': 'controle-leiteiro-inducao-de-lactacao-inducoes-realizadas',
            'farm-milk-control-production': 'controle-leiteiro-registro-da-producao',
            'farm-milk-control-production-average-per-batch': 'controle-leiteiro-registro-da-producao-media-de-lotes',
            'farm-milk-control-production-per-animal': 'controle-leiteiro-registro-da-producao-producao-individual',
            'farm-milk-control-production-per-batch': 'controle-leiteiro-registro-da-producao-media-de-lotes',
            'farm-milk-control-quality': 'controle-leiteiro-qualidade-do-leite',
            'farm-monitoring-animals': 'monitoramento-animais-monitorados',
            'farm-monitoring-c-com': 'monitoramento-gestao-de-antenas',
            'farm-monitoring-c-tech-all': 'monitoramento-gestao-de-coleira-todas',
            'farm-monitoring-c-tech-animal-history': 'monitoramento-gestao-de-coleira-historico-de-animais',
            'farm-monitoring-c-tech-baby-expired': 'monitoramento-gestao-de-coleira-baby-expirada',
            'farm-monitoring-c-tech-collar-report-map': 'monitoramento-gestao-de-coleira-mapa-de-report-de-coleira',
            'farm-monitoring-c-tech-on-farm': 'monitoramento-gestao-de-coleira-revisao-na-fazenda',
            'farm-monitoring-c-tech-replacements': 'monitoramento-gestao-de-coleira-trocas',
            'farm-monitoring-c-tech-technical-review': 'monitoramento-gestao-de-coleira-revisao-tecnica',
            'farm-monitoring-c-tech-waiting-bind': 'monitoramento-gestao-de-coleira-aguardando-vinculo',
            'farm-monitoring-cowcomfort': 'monitoramento-gestao-de-cowcomfort-cowcomfort',
            'farm-monitoring-cowcomfort-points': 'monitoramento-gestao-de-cowcomfort-ponto-de-monitoramento',
            'farm-monitoring-overview': 'monitoramento-visao-geral',
            'farm-monitoring-tokenized-animals': 'monitoramento-animais-tokenizados',
            'farm-notifications': 'notificacoes',
            'farm-notifications-herd': 'notificacoes-rebanho',
            'farm-notifications-system': 'notificacoes-sistema',
            'farm-nutrition-diet-catalog': 'nutricao-cadastro-de-dieta',
            'farm-nutrition-ingredient-management': 'nutricao-gestao-de-insumos',
            'farm-nutrition-monitored-batches': 'nutricao-lotes-monitorados',
            'farm-rearing-weaning-executed': 'cria-recria-desaleitamento-realizados',
            'farm-rearing-weaning-scheduled': 'cria-recria-desaleitamento-programados',
            'farm-reproduction-bull-semen': 'reproducao-touro-semen',
            'farm-reproduction-bull-semen-history': 'reproducao-touro-semen-historico-de-uso',
            'farm-reproduction-bull-semen-stock': 'reproducao-touro-semen-estoque-de-semen',
            'farm-reproduction-calving-iep': 'reproducao-parto-iep',
            'farm-reproduction-calving-performed': 'reproducao-parto-realizados',
            'farm-reproduction-calving-projection': 'reproducao-parto-projecao-de-partos',
            'farm-reproduction-calving-scheduled': 'reproducao-parto-programados',
            'farm-reproduction-drying-performed': 'reproducao-secagem-realizados',
            'farm-reproduction-drying-projection': 'reproducao-secagem-projecao-de-secagens',
            'farm-reproduction-drying-scheduled': 'reproducao-secagem-programados',
            'farm-reproduction-embryo-donor': 'reproducao-doadoras',
            'farm-reproduction-gynecologic-exam': 'reproducao-exame-ginecologico',
            'farm-reproduction-heat-alerts': 'reproducao-alertas-de-cio-alertados',
            'farm-reproduction-heat-alerts-tabs': 'reproducao-alertas-de-cio-tabs',
            'farm-reproduction-heat-confirmed': 'reproducao-alertas-de-cio-confirmados',
            'farm-reproduction-heat-detail': 'reproducao-alertas-de-cio-detalhes',
            'farm-reproduction-inseminator': 'reproducao-inseminador',
            'farm-reproduction-mating': 'reproducao-acasalamento',
            'farm-reproduction-pre-calving': 'reproducao-pre-parto',
            'farm-reproduction-pregnancy-check2': 'reproducao-diagnostico-de-prenhez-2a-confirmacao',
            'farm-reproduction-pregnancy-check3': 'reproducao-diagnostico-de-prenhez-3a-confirmacao',
            'farm-reproduction-pregnancy-confirmation': 'reproducao-diagnostico-de-prenhez-confirmacao',
            'farm-reproduction-pregnancy-rate': 'reproducao-diagnostico-de-prenhez-taxa-de-prenhez',
            'farm-reproduction-pregnancy-reproductive-evaluation': 'reproducao-diagnostico-de-prenhez-avaliacao-reprodutiva',
            'farm-reproduction-pregnancy-suspected-losses': 'reproducao-diagnostico-de-prenhez-suspeita-de-perdas',
            'farm-reproduction-protocol-detail-animals': 'reproducao-protocolos-protocolo-animais-protocolados',
            'farm-reproduction-protocol-detail-infos': 'reproducao-protocolos-protocolo-informacoes',
            'farm-reproduction-protocol-detail-results': 'reproducao-protocolos-protocolo-analise-de-resultados',
            'farm-reproduction-protocol-detail-schedule': 'reproducao-protocolos-protocolo-programacao-do-protocolo',
            'farm-reproduction-protocol-model': 'reproducao-protocolos-modelo-de-protocolo',
            'farm-reproduction-protocol-removed-animals': 'reproducao-protocolos-animais-removidos',
            'farm-reproduction-protocols': 'reproducao-protocolos-meus-protocolos',
            'farm-settings-handling': 'configuracoes-manejo-da-fazenda',
            'farm-settings-network': 'configuracoes-rede',
            'farm-settings-okr': 'configuracoes-okr',
            'farm-settings-overview': 'configuracoes-visao-geral',
            'farm-settings-roles': 'configuracoes-papeis',
            'farm-settings-tokens': 'configuracoes-tokens',
            'farm-settings-units': 'configuracoes-unidades',
            'farm-settings-users': 'configuracoes-usuarios',
            'farm-settings-vic': 'configuracoes-vic',
            'farm-thermal-stress-hourly': 'estresse-termico-hora-hora',
            'farm-thermal-stress-overview': 'estresse-termico-visao-geral',
            'farm-thermal-stress-real-time': 'estresse-termico-tempo-real',
            'system-bull-semen': 'touro-semen',
            'unknown': 'nao-identificado'
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
