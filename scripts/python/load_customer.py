import sys

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sys import *
from load_customer_config import src_dir,target_dir


try:
    spark=SparkSession.builder.master("local[1]").appName("app1")\
        .config("spark.sql.sources.partitionOverwriteMode","dynamic").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    print(sys.argv)

    #var=sys.argv[0]
    var = "202304"
    src_dir_ym=src_dir+str(var)+'\\'


    #src_dir_ym="C:\\Users\satar\OneDrive\Desktop\source\customer\\202303\\"
    """
    cust_schema = StructType([
         StructField ("cust_id",IntegerType(),True),
         StructField("cust_name", StringType(), True),
         StructField("cust_country", StringType(), True),
         StructField("cust_state", StringType(), True),
         StructField("YYYYMM", StringType(), True),
      ])"""

    print(f"arguement passed: {var}")
    print(f"src path : {src_dir_ym}")

    df=spark.read.option("delimiter", "|").csv(src_dir_ym,inferSchema=True, header=True)
    df.printSchema()
    df1=df.distinct()
    df1=df1.dropDuplicates(["cust_id","cust_country"])
    df1.show()
    df1.withColumn("YYYYMM",lit(var)).write.parquet(target_dir,mode="overwrite",partitionBy=["YYYYMM","cust_country","cust_state"])

except Exception as e:
   # raise
    print(str(e))
