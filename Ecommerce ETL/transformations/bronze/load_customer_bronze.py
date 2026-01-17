from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField('customer_id', StringType(), True),
    StructField('customer_unique_id', StringType(), True),
    StructField('customer_zip_code_prefix', IntegerType(), True),
    StructField('customer_city', StringType(), True),
    StructField('customer_state', StringType(), True)
])

@dp.table( # Creates streaming table ONLY if spark streaming is returned from function.
    name='customer_bronze'
)
def load_customer_bronze():
    # The function must return a Spark Streaming DataFrame
    return (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", 'true')
        .schema(schema)
        .load("/Volumes/ecommerce/transactionsdb/ingestion/customers/"))