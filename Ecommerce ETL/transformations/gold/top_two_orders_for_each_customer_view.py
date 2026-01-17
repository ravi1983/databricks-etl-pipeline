from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.window import Window

@dp.materialized_view(
    name = "top_two_orders_for_each_customer_view",
    comment = "Top two orders for EACH customer by order value"
)
def top_two_orders_each_customer():
    # Aggregate Order Totals (order_totals CTE)
    order_items = dp.read("ecommerce.transactionsdb.order_items_fact")
    order_totals = order_items.groupBy("order_id", "customer_unique_id").agg(
        F.sum("price").alias("total_order_price"),
        F.sum("freight_value").alias("total_order_freight")
    )

    # Rank Orders (ranked_orders CTE)
    window_spec = Window.partitionBy("customer_unique_id").orderBy(F.col("total_order_price").desc())
    ranked_orders = order_totals.withColumn("order_rank", F.row_number().over(window_spec))

    # Join with Customers and Filter (top_two_orders CTE)
    customers_dim = dp.read("ecommerce.transactionsdb.customers_dim")
    active_customers = customers_dim.filter(F.col("__END_AT").isNull())

    result_df = (ranked_orders.alias("ro")
        .join(
            active_customers.alias("cd"), 
            on = F.col("ro.customer_unique_id") == F.col("cd.customer_unique_id"), 
            how = "inner"
        )
        .filter(F.col("ro.order_rank") <= 2)
        .select(
            "ro.order_id",
            "ro.customer_unique_id",
            F.col("cd.customer_zip_code_prefix").alias("customer_zipcode"),
            F.col("cd.customer_city").alias("customer_city"),
            F.col("cd.customer_state").alias("customer_state"),
            F.col("ro.total_order_price").alias("order_price"),
            F.col("ro.total_order_freight").alias("order_freight_value"),
            "ro.order_rank"
        )
        .orderBy("customer_unique_id", "order_rank"))

    return result_df