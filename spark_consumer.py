from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = SparkSession.builder.master("local[5]").appName("stream_processor").getOrCreate()
# sc = SparkContext(appName="PythonStreamingKafkaWordCount")
# ssc = StreamingContext(sc, 10)

linesDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "fraud.logs") \
    .load()
    #.option("startingOffsets", "earliest") \

schema = StructType([
    StructField("step", StringType(), True),
    StructField("type", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("nameOrig", StringType(), True),
    StructField("oldbalanceOrg", StringType(), True),
    StructField("newbalanceOrig", StringType(), True),
    StructField("nameDest", StringType(), True),
    StructField("oldbalanceDest", StringType(), True),
    StructField("newbalanceDest", StringType(), True),
    StructField("isFraud", StringType(), True),
    StructField("isFlaggedFraud", StringType(), True)
])

rowDF = linesDF.withColumn('value',from_json(linesDF['value'].cast('string'),schema))\
    .select('value.step','value.type','value.amount','value.nameOrig','value.oldbalanceOrg','value.newbalanceOrig','value.nameDest','value.oldbalanceDest','value.newbalanceDest','value.isFraud','value.isFlaggedFraud')

query = rowDF.writeStream\
    .outputMode("append")\
    .format("csv")\
    .option("path","C:/Users/mrupv/bits/spa/Assignment2/project/data/")\
    .option("checkpointLocation", "C:/Users/mrupv/bits/spa/Assignment2/project/checkpoint_path/")\
    .start()
query.awaitTermination()