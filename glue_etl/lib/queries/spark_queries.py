# lib/queries/spark_queries.py
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from lib.utils.logger import Logger

logger = Logger()

class SparkQueries:
    def __init__(self, spark, dfs: dict):
        self.spark = spark
        self.dfs = dfs
        # convenience
        for k, v in dfs.items():
            # register temp views so we can use SQL if desired
            v.createOrReplaceTempView(k)

    def build_coleiras_reportando(self) -> DataFrame:
        """
        Recreates COLEIRAS_REPORTANDO_QUERY_ATHENA logic:
        Query 1: Validador - Collar reporting status
        """
        logger.info("Starting Coleiras Reportando DataFrame")

        c = self.dfs["collars"].alias("c")
        ac = self.dfs["animal_collar"].alias("ac")
        a = self.dfs["animals"].alias("a")
        f = self.dfs["farms"].alias("f")

        # Subquery for active collar assignments
        ac_subquery = ac.join(a, F.col("ac.animal_id") == F.col("a.id")) \
            .filter(F.col("ac.removal").isNull()) \
            .select(
                F.col("ac.collar_id"),
                F.col("a.farm_id"),
                F.col("ac.animal_id")
            ).alias("ac_sub")

        # Main query
        result = c.join(ac_subquery, F.col("c.id") == F.col("ac_sub.collar_id"), how="left") \
            .join(f, F.col("f.id") == F.col("ac_sub.farm_id"), how="left") \
            .select(
                F.col("c.id").alias("collar_id"),
                F.col("c.code").alias("serial_coleira"),
                F.col("c.last_read").cast("timestamp").alias("ultima_leitura"),
                F.when(
                    F.col("c.last_read") >= F.expr("CURRENT_TIMESTAMP - INTERVAL '48' HOUR"),
                    F.lit("ativa")
                ).otherwise(F.lit("inativa")).alias("status_coleira"),
                F.when(
                    F.col("ac_sub.collar_id").isNull(),
                    F.lit("sem_vinculo")
                ).otherwise(F.lit("com_vinculo")).alias("tem_vinculo"),
                F.col("f.code").alias("farm_code"),
                F.col("ac_sub.animal_id"),
                F.date_format(F.current_date(), "yyyy-MM-dd").alias("extraction_date")
            )

        logger.info("Finished building Coleiras Reportando DataFrame")
        return result

    def build_coleiras_vinculadas(self) -> DataFrame:
        """
        Recreates COLEIRAS_VINCULADAS_QUERY_ATHENA logic:
        Query 2: Alertas - Linked collars with animal info
        """
        logger.info("Starting Coleiras Vinculadas DataFrame")

        ac = self.dfs["animal_collar"].alias("ac")
        a = self.dfs["animals"].alias("a")
        c = self.dfs["collars"].alias("c")
        f = self.dfs["farms"].alias("f")
        ab = self.dfs["animal_breed"].alias("ab")

        # Animal breed with ROW_NUMBER (DISTINCT ON replacement)
        w_ab = Window.partitionBy("animal_id").orderBy(F.col("numerator").desc())
        ab_ranked = ab.withColumn("rn", F.row_number().over(w_ab)) \
            .filter(F.col("rn") == 1) \
            .select("animal_id", "breed", "numerator") \
            .alias("ab_ranked")

        # Main query
        result = ac.join(a, F.col("ac.animal_id") == F.col("a.id")) \
            .join(c, F.col("ac.collar_id") == F.col("c.id")) \
            .join(f, F.col("a.farm_id") == F.col("f.id")) \
            .join(ab_ranked, F.col("a.id") == F.col("ab_ranked.animal_id"), how="left") \
            .select(
                F.col("ac.animal_id"),
                F.col("ac.collar_id"),
                F.col("ac.created_at").cast("timestamp").alias("data_vinculacao"),
                F.col("ac.removal").cast("timestamp").alias("data_remocao"),
                F.col("a.earring").alias("brinco"),
                F.col("f.code").alias("farm"),
                F.col("c.code").alias("serial_coleira"),
                F.col("c.last_read").cast("timestamp").alias("ultima_leitura"),
                F.col("ab_ranked.breed"),
                F.when(
                    F.col("ac.removal").isNull(),
                    F.lit("ativo")
                ).otherwise(F.lit("inativo")).alias("status_vinculo"),
                F.when(
                    F.col("c.last_read") >= F.expr("CURRENT_TIMESTAMP - INTERVAL '48' HOUR"),
                    F.lit("ativa")
                ).otherwise(F.lit("inativa")).alias("status_coleira"),
                F.date_format(F.current_date(), "yyyy-MM-dd").alias("extraction_date")
            )

        logger.info("Finished building Coleiras Vinculadas DataFrame")
        return result

    def build_animals(self) -> DataFrame:
        """
        Recreates ANIMALS_QUERY_ATHENA logic:
        Query 3: IA - Animals data with breed info
        """
        logger.info("Starting Animals DataFrame")

        a = self.dfs["animals"].alias("a")
        f = self.dfs["farms"].alias("f")
        ab = self.dfs["animal_breed"].alias("ab")

        # Animal breed with ROW_NUMBER (DISTINCT ON replacement)
        w_ab = Window.partitionBy("animal_id").orderBy(F.col("numerator").desc())
        ab_ranked = ab.withColumn("rn", F.row_number().over(w_ab)) \
            .filter(F.col("rn") == 1) \
            .select("animal_id", "breed", "numerator") \
            .alias("ab_ranked")

        # Main query
        result = a.join(f, F.col("f.id") == F.col("a.farm_id")) \
            .join(ab_ranked, F.col("a.id") == F.col("ab_ranked.animal_id"), how="left") \
            .filter(F.col("f.demo").cast("boolean") == F.lit(False)) \
            .select(
                F.col("f.code").alias("farm"),
                F.col("a.id").alias("animal_id"),
                F.col("a.earring").alias("brinco"),
                F.col("a.birth").cast("timestamp").alias("data_nascimento"),
                F.col("a.last_delivery").cast("timestamp").alias("ultima_ia"),
                F.col("a.status"),
                F.col("a.born_at_farm"),
                F.col("a.mother_id"),
                F.col("a.father_id"),
                F.col("a.deleted_at").cast("timestamp"),
                F.col("a.created_at").cast("timestamp"),
                F.col("a.collar_id"),
                F.col("a.health_status"),
                F.col("a.last_reproduction_method"),
                F.col("a.reproduction_methods_since_last_conception").alias("qtd_ia_pp"),
                F.col("a.is_female"),
                F.col("a.production_status"),
                F.col("a.total_deliveries"),
                F.col("ab_ranked.breed"),
                F.date_format(F.current_date(), "yyyy-MM-dd").alias("extraction_date")
            )

        logger.info("Finished building Animals DataFrame")
        return result

    def build_users_active(self) -> DataFrame:
        """
        Recreates USERS_ACTIVE_QUERY_ATHENA logic:
        Query 4: User Activities - User access data from 2025-01-01 onwards
        """
        logger.info("Starting Users Active DataFrame")

        ua = self.dfs["user_activities"].alias("ua")
        u = self.dfs["users"].alias("u")
        f = self.dfs["farms"].alias("f")
        fu = self.dfs["farm_user"].alias("fu")

        # Main query
        result = ua.join(u, F.col("u.id") == F.col("ua.user_id")) \
            .join(f, F.col("f.id") == F.col("ua.farm_id")) \
            .join(fu, (F.col("fu.user_id") == F.col("u.id")) & (F.col("fu.farm_id") == F.col("f.id"))) \
            .filter(F.col("ua.timestamp").cast("timestamp") >= F.lit("2025-01-01 00:00:00").cast("timestamp")) \
            .select(
                F.col("ua.timestamp").cast("timestamp").alias("acesso_timestamp"),
                F.col("ua.user_id"),
                F.col("f.code").alias("farm_code"),
                F.col("ua.page"),
                F.date_format(F.current_date(), "yyyy-MM-dd").alias("extraction_date")
            )

        logger.info("Finished building Users Active DataFrame")
        return result

    def build_farm_states(self) -> DataFrame:
        """
        Recreates FARM_STATE_QUERY_ATHENA logic:
        Query 5: Farm State - Comprehensive farm information with collar and animal statistics
        """
        logger.info("Starting Farm States DataFrame")

        # Aliases for all tables
        f = self.dfs["farms"].alias("f")
        fa = self.dfs["farm_addresses"].alias("fa")
        c = self.dfs["cities"].alias("c")
        s = self.dfs["states"].alias("s")
        co = self.dfs["countries"].alias("co")
        cf_table = self.dfs["company_farm"].alias("cf")
        comp = self.dfs["companies"].alias("comp")
        cpf = self.dfs["company_portfolio_farm"].alias("cpf")
        cp = self.dfs["company_portfolios"].alias("cp")
        u = self.dfs["users"].alias("u")
        collars = self.dfs["collars"].alias("collars")
        collar_farm = self.dfs["collar_farm"].alias("collar_farm")
        animals = self.dfs["animals"].alias("animals")

        # CTE 1: farms_filtered
        farms_filtered = f.filter(F.col("f.code") > 0).alias("farms_filtered")

        # CTE 2: collares_farm - count distinct collars per farm
        collares_farm = collars.groupBy("farm_id") \
            .agg(F.countDistinct("id").alias("colares_na_fazenda")) \
            .alias("cfarm")

        # CTE 3: collares_enviados - count distinct collar_id from collar_farm
        collares_enviados = collar_farm.groupBy("farm_id") \
            .agg(F.countDistinct("collar_id").alias("colares_enviados")) \
            .alias("cenv")

        # CTE 4: collares_animais - count distinct collar_id from animals where not null
        collares_animais = animals.filter(F.col("collar_id").isNotNull()) \
            .groupBy("farm_id") \
            .agg(F.countDistinct("collar_id").alias("colares_animais")) \
            .alias("canm")

        # CTE 5: collares_reportando - count collars reporting in last 48 hours
        collares_reportando = collars.filter(
            F.col("last_read") >= F.expr("CURRENT_TIMESTAMP - INTERVAL '48' HOUR")
        ).groupBy("farm_id") \
            .agg(F.countDistinct("id").alias("colares_reportando")) \
            .alias("crep")

        # CTE 6: status_animais - count animals by different statuses
        status_animais = animals.groupBy("farm_id") \
            .agg(
                F.count(F.when(F.col("status") == "pregnant", 1)).alias("vacas_prenhe"),
                F.count(F.when(F.col("status") == "pregnant-pre-delivery", 1)).alias("vacas_pre_parto"),
                F.count(F.when(F.col("production_status") == "dry", 1)).alias("vacas_secas"),
                F.count(F.when(F.col("production_status") == "lactating", 1)).alias("vacas_lactacao"),
                F.count(F.when(F.col("total_deliveries").isNull(), 1)).alias("vacas_novilhas")
            ).alias("sa")

        # Main query - join all CTEs
        result = farms_filtered \
            .join(fa, F.col("farms_filtered.address_id") == F.col("fa.id")) \
            .join(c, F.col("c.id") == F.col("fa.city_id")) \
            .join(s, F.col("s.id") == F.col("c.state_id")) \
            .join(co, F.col("co.id") == F.col("s.country_id")) \
            .join(cf_table, F.col("cf.farm_id") == F.col("farms_filtered.id"), how="left") \
            .join(comp, F.col("comp.id") == F.col("cf.company_id"), how="left") \
            .join(cpf, F.col("cpf.farm_id") == F.col("farms_filtered.id"), how="left") \
            .join(cp, F.col("cp.id") == F.col("cpf.company_portfolio_id"), how="left") \
            .join(u, F.col("u.id") == F.col("cp.responsible_id"), how="left") \
            .join(collares_farm, F.col("cfarm.farm_id") == F.col("farms_filtered.id"), how="left") \
            .join(collares_enviados, F.col("cenv.farm_id") == F.col("farms_filtered.id"), how="left") \
            .join(collares_animais, F.col("canm.farm_id") == F.col("farms_filtered.id"), how="left") \
            .join(collares_reportando, F.col("crep.farm_id") == F.col("farms_filtered.id"), how="left") \
            .join(status_animais, F.col("sa.farm_id") == F.col("farms_filtered.id"), how="left") \
            .select(
                F.col("farms_filtered.code"),
                F.col("farms_filtered.id"),
                F.col("farms_filtered.contract_status"),
                F.col("farms_filtered.deleted_at"),
                F.col("farms_filtered.contract_collars").alias("colares_contratados"),
                F.col("farms_filtered.contract_cow_baby_collars").alias("cowbaby_contratados"),
                F.col("farms_filtered.contract_c_envs").alias("cenvs_contradados"),
                F.col("farms_filtered.plan"),
                F.col("farms_filtered.ventilation_system").alias("sistema_ventilacao"),
                F.col("farms_filtered.system"),
                F.col("c.name").alias("city"),
                F.col("s.name").alias("state"),
                F.col("s.abbreviation").alias("state_abbreviation"),
                F.col("co.code").alias("country"),
                F.col("comp.slug").alias("company_slug"),
                F.col("cp.name").alias("portfolio_name"),
                F.col("u.name").alias("manager"),
                F.col("u.id").alias("manager_user_id"),
                F.coalesce(F.col("cfarm.colares_na_fazenda"), F.lit(0)).alias("colares_na_fazenda"),
                F.coalesce(F.col("cenv.colares_enviados"), F.lit(0)).alias("colares_enviados"),
                F.coalesce(F.col("canm.colares_animais"), F.lit(0)).alias("colares_animais"),
                F.coalesce(F.col("crep.colares_reportando"), F.lit(0)).alias("colares_reportando"),
                F.coalesce(F.col("sa.vacas_prenhe"), F.lit(0)).alias("vacas_prenhe"),
                F.coalesce(F.col("sa.vacas_pre_parto"), F.lit(0)).alias("vacas_pre_parto"),
                F.coalesce(F.col("sa.vacas_secas"), F.lit(0)).alias("vacas_secas"),
                F.coalesce(F.col("sa.vacas_lactacao"), F.lit(0)).alias("vacas_lactacao"),
                F.coalesce(F.col("sa.vacas_novilhas"), F.lit(0)).alias("vacas_novilhas")
            )

        logger.info("Finished building Farm States DataFrame")
        return result

    def build_users(self) -> DataFrame:
        """
        Recreates users query logic:
        Query 6: Users - User information with farm and company associations
        """
        logger.info("Starting Users DataFrame")

        u = self.dfs["users"].alias("u")
        fu = self.dfs["farm_user"].alias("fu")
        cu = self.dfs["company_user"].alias("cu")

        # DISTINCT ON (u.id) replacement using Window function
        # Join users with farm_user and company_user
        result_with_duplicates = u \
            .join(fu, F.col("fu.user_id") == F.col("u.id"), how="left") \
            .join(cu, F.col("cu.user_id") == F.col("u.id"), how="left") \
            .filter(F.col("u.deleted_at").isNull())

        # Use Window function to get distinct users (first row per user_id)
        w_user = Window.partitionBy("u.id").orderBy(F.col("u.id"))
        
        result = result_with_duplicates \
            .withColumn("rn", F.row_number().over(w_user)) \
            .filter(F.col("rn") == 1) \
            .select(
                F.col("u.id").alias("user_id"),
                F.col("u.name"),
                F.col("fu.farm_id"),
                F.col("fu.through_system"),
                F.col("fu.active"),
                F.col("cu.company_id")
            )

        logger.info("Finished building Users DataFrame")
        return result
