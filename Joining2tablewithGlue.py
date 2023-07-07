import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *


glueContext = GlueContext(SparkContext.getOrCreate())

table1 = glueContext.create_dynamic_frame.from_catalog(database="cust_db", table_name="customer")
table2 = glueContext.create_dynamic_frame.from_catalog(database="cust_db", table_name="product")

joined_table = Join.apply(frame1=table1, frame2=table2, keys1=["product_id"], keys2=["product_id"], transformation_ctx="joined_table")

filter_data = joined_table.filter(f= lambda x: x["amt"] >= "15000") 

transformed_data = ApplyMapping.apply(frame=filter_data, mappings =
[ ("product_id","long","product_id","int"),

        ("product_name", "string", "product_name", "string"),
        ("company", "string", "company", "string"),
        ("cust_name", "string", "cust_name", "string"),
        
        ("cust_id", "long", "cust_id", "int"),
        
        ("amt","long","amt","int")

])

#partitionby = Windows.partitionBy("country").orderBy("salary")
#df = transformed_data.select(transformed_data.state).collect()

glueContext.write_dynamic_frame.from_options(frame=transformed_data, connection_type="s3", connection_options={"path": "s3://awsnewbuck123/output_custdata/"}, format="parquet")