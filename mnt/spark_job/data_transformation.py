from pyspark.sql import SparkSession
import pyspark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

if __name__ == "__main__":
    # Cria a sessão do Spark
    conf = (
    pyspark.SparkConf()
    .set("spark.master", "spark://spark-master:7077")
    )

    spark = SparkSession.builder \
    .appName("GCS Integration") \
    .config(conf=conf) \
    .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

   # Defina o esquema
    schema = StructType([
        StructField("brand", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("car_detail", StringType(), True),
        StructField("url", StringType(), True),
        StructField("fipe_price", StringType(), True)
    ])
    #print(spark)
    df = spark.read.option("header", True).csv("gs://lobobranco-datalake/raw/acura_fipe.csv")

    # Mostra o DataFrame
    #df.show()
    #df = spark.read.csv("gs://lobobranco-datalake/raw/acura_fipe.csv", header=True, inferSchema=True)

    #processed_layer = "gs://lobobranco-datalake/processed"

# Escreve o DataFrame como um arquivo Parquet
    #df.write \
     #   .mode("overwrite") \
      #  .parquet(processed_layer + "/fipe_data")
    
    spark.stop()
