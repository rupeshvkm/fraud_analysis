from pyspark.ml.classification import LogisticRegressionModel
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local[5]').appName('predictor').getOrCreate()
model = PipelineModel.load('model/')

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "fraud.test.logs") \
    .load()

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

rowDF = df.withColumn('value',from_json(df['value'].cast('string'),schema)) \
    .select('value.step','value.type','value.amount','value.nameOrig','value.oldbalanceOrg','value.newbalanceOrig','value.nameDest','value.oldbalanceDest','value.newbalanceDest','value.isFraud','value.isFlaggedFraud')

prediction = model.transform(rowDF)
result = prediction.select("features", "label", "myProbability", "prediction")

fraud_records = result.where(result['prdeiction'] == 1)
good_records = result.where(result['prdeiction'] != 1)
