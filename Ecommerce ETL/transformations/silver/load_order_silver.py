from pyspark import pipelines as dp
import pyspark.sql.functions as F

# This view hold the transformed data, and used as source for both gold and silver
@dp.view(
    name='orders_view'
)
def orders_view():
  return (spark.readStream.table('orders_bronze')
            .withColumn('created_on', F.current_timestamp()))
  
# Creating deduped silver table with CDC type1
dp.create_streaming_table(
  name='orders_silver'
)
dp.create_auto_cdc_flow(
  target = "orders_silver",
  source = "orders_view",
  keys = ["order_id"],
  sequence_by = F.col("order_purchase_timestamp"),
  stored_as_scd_type = 1
)