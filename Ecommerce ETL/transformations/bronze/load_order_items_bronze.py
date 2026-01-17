from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

schema = StructType([
    StructField('order_id', StringType(), True),
    StructField('order_item_id', IntegerType(), True),
    StructField('product_id', StringType(), True),
    StructField('seller_id', StringType(), True),
    StructField('shipping_limit_date', TimestampType(), True),
    StructField('price', StringType(), True),
    StructField('freight_value', StringType(), True)
])

@dp.table(
    name='order_items_bronze',
)
def load_order_items_bronze():
    return (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", 'true')
        .schema(schema)
        .load("/Volumes/ecommerce/transactionsdb/ingestion/order_items/"))