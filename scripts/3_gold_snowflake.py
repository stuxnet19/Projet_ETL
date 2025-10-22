# scripts/3_gold_snowflake.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg

# Définition des variables
SILVER_TABLE = "taxi_project.silver.trips_clean"
SECRET_SCOPE = "snowflake_taxi_creds"
SNOWFLAKE_TABLE_NAME = "GOLD_PAYMENT_SUMMARY" # Nom de la table dans Snowflake
PROJECT_NAME = "NYC Taxi ETL"

def get_snowflake_options(dbutils) -> dict:
    """
    Récupère les identifiants Snowflake depuis Databricks Secrets.
    """
    print("Récupération des identifiants Snowflake...")
    return {
        "sfUrl": dbutils.secrets.get(SECRET_SCOPE, "url"),
        "sfUser": dbutils.secrets.get(SECRET_SCOPE, "user"),
        "sfPassword": dbutils.secrets.get(SECRET_SCOPE, "password"),
        "sfDatabase": dbutils.secrets.get(SECRET_SCOPE, "database"),
        "sfSchema": dbutils.secrets.get(SECRET_SCOPE, "schema"),
        "sfWarehouse": dbutils.secrets.get(SECRET_SCOPE, "warehouse")
    }


def process_gold_and_load(spark, silver_table: str, snowflake_table: str, sf_options: dict):
    """
    Agrège les données Silver et les charge dans Snowflake.
    """
    print(f"Lecture des données Silver depuis {silver_table}")
    df_silver = spark.read.table(silver_table)

    # Logique métier (Gold) : Agréger les métriques par type de paiement
    df_gold = (df_silver
        .groupBy("payment_type")
        .agg(
            count("*").alias("total_trips"),
            sum("total_amount").alias("total_revenue"),
            avg("tip_amount").alias("average_tip")
        )
        .orderBy("payment_type")
    )
    
    print("Agrégat Gold calculé. Affichage d'un échantillon :")
    df_gold.show()

    # Récupérer les options de connexion
    try:
        sf_options = get_snowflake_options(dbutils)
    except Exception as e:
        print(f"ERREUR: Impossible de récupérer les secrets. Vérifiez le nom du scope '{SECRET_SCOPE}'")
        raise e

    print(f"Chargement de l'agrégat Gold dans Snowflake : {snowflake_table}")

    # Écriture dans Snowflake
    (df_gold.write
        .format("snowflake") # Utilise le connecteur Spark-Snowflake
        .options(**sf_options)
        .option("dbtable", snowflake_table)
        .mode("overwrite") # Remplace la table à chaque exécution
        .save()
    )
    
    print("Chargement Gold vers Snowflake terminé avec succès.")

if __name__ == "__main__":


    print("Démarrage du script Gold (version corrigée)...")
    print(f"Récupération des secrets depuis le scope: {SECRET_SCOPE}")
    sf_options = get_snowflake_options(dbutils) 

    # 2. Lancer la fonction principale avec les vrais secrets
    process_gold_and_load(spark, SILVER_TABLE, SNOWFLAKE_TABLE_NAME, sf_options)

    print("Script Gold terminé.")