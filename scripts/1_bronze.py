# scripts/1_bronze.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit


SOURCE_PATH = "/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-01.csv.gz"
BRONZE_TABLE = "taxi_project.bronze.trips"
PROJECT_NAME = "NYC Taxi ETL"

def ingest_to_bronze(spark: SparkSession, source_path: str, table_name: str):
    """
    Ingère les données CSV sources vers une table Delta Bronze managée par UC.
    """
    print(f"Démarrage de l'ingestion depuis {source_path}")

    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true") 
        .load(source_path)
    )

    df_with_audit = (
        df.withColumn("_ingest_timestamp", current_timestamp())
        .withColumn("_source_file", lit(source_path))
    )

    print(f"Écriture des données dans {table_name}")
    (
        df_with_audit.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(table_name)
    )
    print(f"Ingestion Bronze terminée avec succès pour {table_name}.")


if __name__ == "__main__":
    ingest_to_bronze(spark, SOURCE_PATH, BRONZE_TABLE)