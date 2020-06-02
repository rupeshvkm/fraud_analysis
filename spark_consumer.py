from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

spark = SparkSession.builder.master("local[5]").appName("stream_processor").getOrCreate()
# sc = SparkContext(appName="PythonStreamingKafkaWordCount")
# ssc = StreamingContext(sc, 10)

linesDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "fraud_log") \
    .option("startingOffsets", "earliest") \
    .load()

query = linesDF.writeStream.outputMode("append").format("console").start()
query.awaitTermination()