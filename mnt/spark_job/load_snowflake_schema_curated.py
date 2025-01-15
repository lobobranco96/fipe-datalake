import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import pyspark
from pyspark.sql.functions import monotonically_increasing_id, col

load_dotenv(dotenv_path="/spark_job/.env")

GOOGLE_KEY_ID = os.getenv("GOOGLE_KEY_ID")
GOOGLE_PRIVATE_KEY = os.getenv("GOOGLE_PRIVATE_KEY")
GOOGLE_ACCOUNT_EMAIL = os.getenv("GOOGLE_ACCOUNT_EMAIL")

def create_spark_session():

    conf = (
    pyspark.SparkConf()
    .set("spark.master", "spark://spark-master:7077")
    )
    spark = SparkSession.builder \
    .appName("GCS Integration with PySpark") \
    .config(conf=conf) \
    .config("fs.gs.auth.service.account.private.key.id", GOOGLE_KEY_ID) \
    .config("fs.gs.auth.service.account.private.key", GOOGLE_PRIVATE_KEY) \
    .config("fs.gs.auth.service.account.email", GOOGLE_ACCOUNT_EMAIL) \
    .getOrCreate()

    return spark

def snowflake_schema_curated(spark):

    full_path = "gs://lobobranco-datalake/processed/vehicle_fipe_data" 
    data_from_processed = spark.read.parquet(full_path)
    data_from_processed.printSchema()
   # Criar tabela brand_dim
    brand_dim = (
        data_from_processed.select("brand")
        .distinct()
        .withColumn("id_brand", monotonically_increasing_id())
        .select("id_brand", "brand")
    )

    # Criar tabela model_vehicle_dim
    model_vehicle_dim = (
        data_from_processed.join(brand_dim, "brand")
        .select("id_brand", "model", "car_detail", "cambio", "cilindrada")
        .distinct()
        .withColumn("id_model_vehicle", monotonically_increasing_id())
        .select("id_model_vehicle", "id_brand", "model", "car_detail", "cambio", "cilindrada")
    )

    # Criar tabela year_dim
    df_with_ids = (
        data_from_processed.join(brand_dim, "brand")
        .join(model_vehicle_dim, ["id_brand", "model", "car_detail", "cambio", "cilindrada"])
    )

    year_dim = (
        df_with_ids
        .select("id_model_vehicle", "year")
        .distinct()
        .withColumn("id_year", monotonically_increasing_id())
        .select("id_year", "id_model_vehicle", "year")
    )

    # Criar tabela fipe_price_fact
    fipe_price_fact = (
        df_with_ids.join(year_dim, ["id_model_vehicle", "year"])
        .select("id_year", "id_model_vehicle", "fipe_price")
        .withColumn("id_fipe_price", monotonically_increasing_id())
        .select("id_fipe_price", "id_year", "id_model_vehicle", "fipe_price")
    )

    output_path = "gs://lobobranco-datalake/curated"

    brand_dim.write.mode("overwrite").parquet(f"{output_path}/brand_dim")
    model_vehicle_dim.write.mode("overwrite").parquet(f"{output_path}/model_vehicle_dim")
    year_dim.write.mode("overwrite").parquet(f"{output_path}/year_dim")
    fipe_price_fact.write.mode("overwrite").parquet(f"{output_path}/fipe_price_fact")


if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    snowflake_schema_curated(spark)

    spark.stop()
