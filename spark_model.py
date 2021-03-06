from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer,VectorAssembler
from pyspark.ml.classification import LogisticRegression

from pyspark.sql.functions import *

spark = SparkSession.builder.master('local[5]').appName('fraud-analysis').getOrCreate()

data = spark.read.option('header','true').csv('C:/Users/mrupv/bits/spa/Assignment2/paysim1/train.csv')

df = data.withColumn("oldbalanceOrg",data["oldbalanceOrg"].cast("double"))\
    .withColumn("newbalanceOrig",data["newbalanceOrig"].cast("double"))\
    .withColumn("oldbalanceDest",data["oldbalanceDest"].cast("double"))\
    .withColumn("newbalanceDest",data["newbalanceDest"].cast("double")) \
    .withColumn("step",data["step"].cast("int")) \
    .withColumn("amount",data["amount"].cast("double")) \
    .withColumn("isFraud",data["isFraud"].cast("int"))

type_indexer = StringIndexer(inputCol='type',outputCol='type_index')
orig_indexer = StringIndexer(inputCol='nameOrig',outputCol='nameOrig_index')
dest_indexer = StringIndexer(inputCol='nameDest',outputCol='nameDest_index')
assembler = VectorAssembler(inputCols=['step','type_index','amount','nameOrig_index','oldbalanceOrg','newbalanceOrig','nameDest_index','oldbalanceDest','newbalanceDest'],
                            outputCol='features')

model = LogisticRegression(featuresCol='features',labelCol='isFraud')

pipeline = Pipeline(stages=[type_indexer,orig_indexer,dest_indexer,assembler,model])

model = pipeline.fit(df)

#output_df = model.transform(df)

model.save('model/')

