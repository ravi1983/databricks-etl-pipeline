from pyspark import pipelines as dp
import pyspark.sql.functions as F
  
dp.create_streaming_table(
  name='customers_dim'
)
dp.create_auto_cdc_flow(
  target = "customers_dim",
  source = "customers_dedup__view",
  keys = ["customer_unique_id"],
  sequence_by = F.col("created_on"),
  stored_as_scd_type = 2
)