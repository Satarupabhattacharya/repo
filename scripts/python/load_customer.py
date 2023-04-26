from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *


try:
    spark=SparkSession.builder.master("local[1]").appName("app1").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    df=spark.read.option("delimiter", "|").csv("C:\\Users\satar\OneDrive\Desktop\source\\202304\customer.csv",inferSchema=True, header=True)
    df.show()

    df1=df.dropDuplicates(["cust_id","cust_country"])
    df1.show()
    df1.withColumn("YYYYMM",lit('202304')).write.parquet("C:\\Users\satar\OneDrive\Desktop\\target\cust_dim",mode="overwrite",partitionBy=["YYYYMM","cust_country","cust_state"])

except Exception as e:
   # raise
    print(str(e))
