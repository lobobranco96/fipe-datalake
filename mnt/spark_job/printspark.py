from pyspark.sql import SparkSession
import pyspark

if __name__ == "__main__":
    # Cria a sessão do Spark
    MASTER = "spark://spark-master:7077"
    conf = (
    pyspark.SparkConf()
    .setAppName('WordCount')  
    .set("spark.master", MASTER)
    )

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    print(spark)
    # Arquivo de entrada (pode ser alterado)
    #input_path = "/spark-job/input.txt"
    #output_path = "output.txt"

    # Lê o arquivo de entrada
    #text_file = spark.read.text(input_path)
    
    # Conta as palavras
    #word_counts = (
     #   text_file.rdd.flatMap(lambda line: line[0].split())
      #  .map(lambda word: (word, 1))
       # .reduceByKey(lambda a, b: a + b)
    #)
    
    # Converte para DataFrame
    #word_counts_df = word_counts.toDF(["word", "count"])
    
    # Salva a saída
    #word_counts_df.write.mode("overwrite").csv(output_path)

    # Para o Spark
    spark.stop()
