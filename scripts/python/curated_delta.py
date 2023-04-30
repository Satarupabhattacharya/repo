
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

    df = spark.read.parquet("C:\\Users\satar\OneDrive\Desktop\\target\curated_layer\customer_dim")
    df1 = df.select("YYYYMM").distinct()

    windowSpec = Window.orderBy("YYYYMM")
    curr_yrmonth=df1.withColumn("row_number",row_number().over(windowSpec)).filter(col("row_number")==2).collect()[0][0]
    prev_yrmonth=df1.withColumn("row_number", row_number().over(windowSpec)).filter(col("row_number")==1).collect()[0][0]
    curr_src_path=delta_src_dir+str(curr_yrmonth)
    prev_src_path=delta_src_dir+str(prev_yrmonth)

    df_curr=spark.read.parquet(curr_src_path,inferSchema=True,header=True)
    df_prev=spark.read.parquet(prev_src_path,inferSchema=True,header=True)

    df_curr_new=df_curr.withColumn("MD5", md5(concat_ws(" ", df_curr.cust_id,df_curr.cust_name,df_curr.cust_country,df_curr.cust_state)))
    df_prev_new=df_prev.withColumn("MD5",md5(concat_ws(" ", df_prev.cust_id, df_prev.cust_name, df_prev.cust_country,df_prev.cust_state)))

    df_curr_new.show()
    df_prev_new.show()


    df_join=df_curr_new.join(df_prev_new,df_curr_new.cust_id==df_prev_new.cust_id,"fullouter")
    df_join.show()
    df2_join=df_join.withColumn("flag", when((df_curr_new.cust_id.isNull()),'D')
                                        .when((df_prev_new.cust_id.isNull()),"I")
                                        .when(~ df_curr_new.cust_id.isNull() & ~ df_prev_new.cust_id.isNull()
                                              & (df_curr_new.MD5==df_prev_new.MD5),"FU")
                                        .otherwise("TU")
                                   )


    df3=  df2_join.select((df_curr_new.cust_id,df_curr_new.cust_name,df_curr_new.cust_country,df_curr_new.cust_state,df2_join.flag)\
        .where((df2_join.flag =="TU") | (df2_join.flag =="FU")|(df2_join.flag =="I")))

    # df2_join.selectExpr("case when ")





except Exception as e:
        #raise
        print(str(e))
