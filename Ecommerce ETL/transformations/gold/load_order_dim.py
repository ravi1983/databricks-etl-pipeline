from pyspark import pipelines as dp
import pyspark.sql.functions as F
  
dp.create_streaming_table(
  name='orders_dim'
)
dp.create_auto_cdc_flow(
  target = "orders_dim",
  source = "orders_view",
  keys = ["order_id"],
  sequence_by = F.col("order_purchase_timestamp"),
  stored_as_scd_type = 1
)