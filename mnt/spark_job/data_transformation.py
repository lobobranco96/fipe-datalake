import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import pyspark
from pyspark.sql.functions import regexp_extract, regexp_replace, col, trim
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col, concat, lit, substring

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

def data_transformation(spark):
    bucket_path = "gs://lobobranco-datalake/raw/"
    csv_file = "*.csv"
    full_path = f"{bucket_path}{csv_file}"

    data = spark.read.csv(full_path, header="true")

    """ Transformando a coluna car_detail """
    data_transformation = data.withColumn("car_detail", regexp_replace(col("car_detail"), r'\s+\w+$', ''))
    data_transformation = data_transformation.withColumn("car_detail", regexp_replace(col("car_detail"), r'-', ' ')) # Substituir os traços por espaços

    """ Criando uma nova coluna "cambio" e extraindo o cambio do veiculo para a coluna"""
    data_transformation = data_transformation.withColumn("cambio", regexp_extract(col("car_detail"), r'(\w+)$', 1))
    data_transformation = data_transformation.withColumn("cambio", regexp_replace(col("cambio"), "automatizado", "automatico")) # trocando automatizado por automatico

    data_transformation = data_transformation.withColumn("car_detail", regexp_replace(col("car_detail"), r'\s+\w+$', ''))# Remover a última palavra da coluna "car_detail"

    data_transformation = data_transformation.drop("_c0") # removendo a coluna _c0
    data_transformation = data_transformation.withColumn("year", col("year").cast("int")) #mudando o datatype da coluna year de String para Integer

    """ Transformando a coluna fipe_price """
    data_transformation = data_transformation.withColumn("fipe_price", regexp_replace(col("fipe_price"), r'R\$', ''))  # Remove "R$"
    data_transformation = data_transformation.withColumn("fipe_price", regexp_replace(col("fipe_price"), r'\s+', ''))  # Remove espaços extras
    data_transformation = data_transformation.withColumn("fipe_price", regexp_replace(col("fipe_price"), r'\.', ''))  # Substitui a vírgula por ponto "" se remover essa linha ou a de baixo quebra a coluna""
    data_transformation = data_transformation.withColumn("fipe_price", regexp_replace(col("fipe_price"), r',', '.'))  # Substitui a vírgula por ponto "" Nao remover""
    data_transformation = data_transformation.withColumn("fipe_price", regexp_replace(data_transformation["fipe_price"], r'[^0-9.]', ''))
    data_transformation = data_transformation.withColumn("fipe_price", data_transformation["fipe_price"].cast(FloatType())) # Modificando o data type para Float

    # Criar a coluna 'cilindrada' com os dois primeiros dígitos
    data_transformation = data_transformation.withColumn("cilindrada", regexp_extract(col("car_detail"), r'^(\d{1,2})', 1))

    # Remover a parte da cilindrada de 'car_detail'
    data_transformation = data_transformation.withColumn("car_detail", regexp_replace(col("car_detail"), r'^\d{1,2} ', ''))
    data_transformation = data_transformation.withColumn("cilindrada", concat(substring(col("cilindrada"), 1, 1), lit("."), substring(col("cilindrada"), 2, 2))) # coloca '.' entre os numeros #1.2 2.0 1.4

    # Mudando a ordem das colunas
    data_transformation = data_transformation.select("brand", "model", "year", "car_detail", "cambio", "cilindrada", "url", "fipe_price")
    data_transformation.show(truncate=True)

    bucket = "gs://lobobranco-datalake"
    processed_layer = "processed"
    file_name = "vehicle_fipe_data"

    full_path = f"{bucket}/{processed_layer}/{file_name}"
    return data_transformation.write \
        .mode("overwrite") \
        .parquet(full_path)
    
if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    data_transformation(spark)
    spark.stop()
