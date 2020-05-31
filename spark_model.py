from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer,VectorAssembler
from pyspark.ml.classification import LogisticRegression

from pyspark.sql.functions import *

spark = SparkSession.builder.master('local[5]').appName('fraud-analysis').getOrCreate()

df = spark.read.option('header','true').csv('C:/Users/mrupv/bits/spa/Assignment2/paysim1/PS_20174392719_1491204439457_log.csv')

type_indexer = StringIndexer(inputCol='type',outputCol='type_index')
orig_indexer = StringIndexer(inputCol='nameOrig',outputCol='nameOrig_index')
dest_indexer = StringIndexer(inputCol='nameDest',outputCol='nameDest_index')
assembler = VectorAssembler(inputCols=['step','type_index','amount','nameOrig_index','oldbalanceOrg','newbalanceOrig','nameDest_index','oldbalanceDest','newbalanceDest'],
                            outputCol='feature_vector')

model = LogisticRegression(featuresCol='features',labelCol='isFraud')

pipeline = Pipeline(stages=[type_indexer,orig_indexer,dest_indexer,assembler,model])

model = pipeline.fit(df)

#output_df = model.transform(df)

model.save('model/')

