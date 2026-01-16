from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField('customer_id', StringType(), True),
    StructField('customer_unique_id', StringType(), True),
    StructField('customer_zip_code_prefix', IntegerType(), True),
    StructField('customer_city', StringType(), True),
    StructField('customer_state', StringType(), True)
])

dp.create_streaming_table('ecommerce.bronze.customers_bronze', schema=schema)

@dp.append_flow('ecommerce.bronze.customers_bronze')
def load_customer_raw():
    # The function must return a Spark Streaming DataFrame (eg., Kaka)
    # Can enhance this by adding checkpointing.
    return (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", 'true')
        .schema(schema)
        .load("/Volumes/ecommerce/bronze/ingestion/customers/"))
    