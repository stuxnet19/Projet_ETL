# scripts/2_silver.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month
from pyspark.sql.types import TimestampType, IntegerType, DoubleType

# Définition des variables
BRONZE_TABLE = "taxi_project.bronze.trips"
SILVER_TABLE = "taxi_project.silver.trips_clean"
PROJECT_NAME = "NYC Taxi ETL"

def transform_to_silver(spark: SparkSession, bronze_table: str, silver_table: str):
    """
    Nettoie, type et enrichit les données de Bronze vers Silver.
    """
    print(f"Lecture des données depuis {bronze_table}")
    df_bronze = spark.read.table(bronze_table)

    # Logique de transformation
    # 1. Sélectionner les colonnes utiles
    # 2. Renommer pour la clarté (snake_case)
    # 3. Caster vers les bons types de données
    # 4. Filtrer les données aberrantes
    df_silver = (df_bronze
        .select(
            col("VendorID").alias("vendor_id").cast(IntegerType()),
            col("tpep_pickup_datetime").alias("pickup_datetime").cast(TimestampType()),
            col("tpep_dropoff_datetime").alias("dropoff_datetime").cast(TimestampType()),
            col("passenger_count").cast(IntegerType()),
            col("trip_distance").cast(DoubleType()),
            col("RatecodeID").alias("rate_code_id").cast(IntegerType()),
            col("PULocationID").alias("pickup_location_id").cast(IntegerType()),
            col("DOLocationID").alias("dropoff_location_id").cast(IntegerType()),
            col("payment_type").cast(IntegerType()),
            col("fare_amount").cast(DoubleType()),
            col("tip_amount").cast(DoubleType()),
            col("total_amount").cast(DoubleType())
        )
        .filter(
            (col("passenger_count") > 0) &
            (col("trip_distance") > 0.0) &
            (col("total_amount") > 0)
        )
    )
    
    # Enrichissement : partitionner par année/mois pour la performance
    df_final = (df_silver
        .withColumn("pickup_year", year(col("pickup_datetime")))
        .withColumn("pickup_month", month(col("pickup_datetime")))
    )

    print(f"Écriture des données nettoyées dans {silver_table}")
    (df_final.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("pickup_year", "pickup_month") # Optimisation des requêtes
        .saveAsTable(silver_table)
    )
    
    print(f"Transformation Silver terminée avec succès pour {silver_table}.")

if __name__ == "__main__":
    spark = SparkSession.builder.appName(PROJECT_NAME + " - Silver").getOrCreate()
    transform_to_silver(spark, BRONZE_TABLE, SILVER_TABLE)