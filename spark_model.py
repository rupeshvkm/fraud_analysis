from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[5]').appName('fraud-analysis').getOrCreate()

df = spark.read.option('header','true').csv('C:/Users/mrupv/bits/spa/Assignment2/paysim1/PS_20174392719_1491204439457_log.csv')

df.show()