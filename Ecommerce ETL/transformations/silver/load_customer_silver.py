from pyspark import pipelines as dp
import pyspark.sql.functions as F

# This view hold the transformed data, and used as source for both gold and silver
@dp.view(
    name='customers_dedup__view'
)
def customers_silver_view():
  # Find unique entries by customer_unique_id, and if there are multiple use the 1st one
  return (spark.readStream.table('customer_bronze')
            .dropDuplicates(['customer_unique_id'])
            .withColumn('created_on', F.current_timestamp()))
  
# Creating deduped silver table with CDC type1
dp.create_streaming_table(
  name='customers_silver'
)
dp.create_auto_cdc_flow(
  target = "customers_silver",
  source = "customers_dedup__view",
  keys = ["customer_unique_id"],
  sequence_by = F.col("created_on"),
  stored_as_scd_type = 1
)