from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.types import DecimalType

# This view hold the transformed data, and used as the source for both gold and silver
@dp.view(
    name='order_items_view'
)
def orders_item_view():
  return (spark.readStream.table('order_items_bronze')
            .withColumn("price", F.col("price").cast(DecimalType(10, 2)))
            .withColumn("freight_value", F.col("freight_value").cast(DecimalType(10, 2)))
            .withColumn('created_on', F.current_timestamp()))
  

dp.create_streaming_table(
  name='order_items_silver'
)
dp.create_auto_cdc_flow(
  target = "order_items_silver",
  source = "order_items_view",
  keys = ["order_id", "order_item_id"],
  sequence_by = F.col("created_on"),
  stored_as_scd_type = 1
)