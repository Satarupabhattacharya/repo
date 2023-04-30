from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sys import *
from load_customer_config import src_dir,target_dir
from pyspark.sql.window import Window
from load_customer_config import delta_src_dir


try:

    spark=SparkSession.builder.master("local[1]").appName("app1")\
        .config("spark.sql.sources.partitionOverwriteMode","dynamic").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    df = spark.read.option("delimiter","|").csv("C:\\Users\satar\OneDrive\Desktop\source\\transaction",inferSchema=True,header=True)
    df_trx=df.groupby("cust_id").agg(max(to_date("tran_date")).alias("max_tran_date"))\
        .withColumn("cutoff_date", add_months(current_date(),-12)).filter("max_tran_date<=cutoff_date").drop("cutoff_date")
    df_trx.show()




    df_cust = spark.read.parquet("C:\\Users\satar\OneDrive\Desktop\\target\curated_layer\customer_dim\YYYYMM=202303").distinct().sort("cust_id")

    df_join=df_trx.join(df_cust, df_trx.cust_id==df_cust.cust_id,"leftouter")
    df_join.show()

   # df_cust_id_distinct=df_join.filter.distinct()
   # df1.filter(df1.cust_id not in [cust_id_distinct]).show()


except Exception as e:
        #raise
        print(str(e))

