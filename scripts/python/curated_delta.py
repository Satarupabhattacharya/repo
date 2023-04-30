
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sys import *
from load_customer_config import src_dir,target_dir


spark=SparkSession.builder.master("local[1]").appName("app1")\
        .config("spark.sql.sources.partitionOverwriteMode","dynamic").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
df=spark.read.parquet("C:\\Users\satar\OneDrive\Desktop\\target\curated_layer\customer_dim")
df.show()
df1=df.select("YYYYMM").distinct()
df1.show()
df1.withColumn()