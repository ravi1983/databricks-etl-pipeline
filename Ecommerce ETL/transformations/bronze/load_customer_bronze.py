from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField('customer_id', StringType(), True),
    StructField('customer_unique_id', StringType(), True),
    StructField('customer_zip_code_prefix', IntegerType(), True),
    StructField('customer_city', StringType(), True),
    StructField('customer_state', StringType(), True)
])

@dp.table(
    name='customer_bronze'
)
def load_customer_bronze():
    # The function must return a Spark Streaming DataFrame (eg., Kafka)
    # Can enhance this by adding checkpointing.
    return (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", 'true')
        .schema(schema)
        .load("/Volumes/ecommerce/transactionsdb/ingestion/customers/"))