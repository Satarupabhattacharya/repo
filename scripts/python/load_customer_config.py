
src_dir="C:\\Users\satar\OneDrive\Desktop\source\customer\\"
target_dir="C:\\Users\satar\OneDrive\Desktop\\target\curated_layer\customer_dim"

cust_schema = "StructType([\
    StructField('cust_id', IntegerType(), True),\
    StructField('cust_name', StringType(), True),\
    StructField('cust_country', StringType(), True),\
    StructField('cust_state', StringType(), True),\
    StructField('YYYYMM', StringType(), True),\
])"