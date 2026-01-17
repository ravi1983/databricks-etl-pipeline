from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

schema = StructType([
    StructField('order_id', StringType(), True),
    StructField('customer_id', StringType(), True),
    StructField('order_status', StringType(), True),
    StructField('order_purchase_timestamp', TimestampType(), True),
    StructField('order_approved_at', TimestampType(), True),
    StructField('order_delivered_carrier_date', TimestampType(), True),
    StructField('order_delivered_customer_date', TimestampType(), True),
    StructField('order_estimated_delivery_date', TimestampType(), True)
])

@dp.table(
    name='orders_bronze'
)
def load_orders_bronze():
    return (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", 'true')
        .schema(schema)
        .load("/Volumes/ecommerce/transactionsdb/ingestion/orders/"))