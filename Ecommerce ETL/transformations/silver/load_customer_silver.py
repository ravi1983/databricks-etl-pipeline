from pyspark import pipelines as dp
import pyspark.sql.functions as F

# This view holds the transformed data, and used as the source for both gold and silver
@dp.view(
    name='customers_transformed_view'
)
def customers_silver_view():
  return (spark.readStream.table('customer_bronze')
            .withColumn('created_on', F.current_timestamp()))

dp.create_streaming_table(
  name='customers_silver',
  expect_all_or_drop = { # Data quality checks
    "customer_not_null": "customer_id != null"
  }
)
dp.create_auto_cdc_flow( # Abstracts the CDC logic
  target = "customers_silver",
  source = "customers_transformed_view",
  keys = ["customer_id", "customer_unique_id"],
  sequence_by = F.col("created_on"),
  stored_as_scd_type = 1 # CDC type1 - overwrites data
)