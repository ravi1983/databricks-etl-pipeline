from pyspark import pipelines as dp
import pyspark.sql.functions as F
  
dp.create_streaming_table(
  name='customers_dim'
)
dp.create_auto_cdc_flow(
  target = "customers_dim",
  source = "customers_transformed_view", # Using the view instead of silver streaming table
  keys = ["customer_id", "customer_unique_id"],
  sequence_by = F.col("created_on"),
  stored_as_scd_type = 2 # CDC type2 - Will keep track of customer attribute changes
)