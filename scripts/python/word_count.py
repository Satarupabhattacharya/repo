from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark=SparkSession.builder.appName("ACF").getOrCreate()

df=spark.read.text("C:\\Users\satar\OneDrive\Desktop\source\wordcount\wiki_msd.txt")

df=df.withColumnRenamed("value","sentence")
df=df.withColumn("word_list", split("sentence"," ")).withColumn("word",explode("word_list"))
df1=df.filter('word = "worked"').groupby("word").count()

df.show()
df1.show()