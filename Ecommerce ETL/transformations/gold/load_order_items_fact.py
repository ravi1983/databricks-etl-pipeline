from pyspark import pipelines as dp

@dp.table(
    name = "order_items_fact"
)
def load_order_items_fact():
    orders_df = dp.read("orders_dim")
    customer_df = dp.read("customers_dim")
    items_df = dp.read_stream("order_items_silver") # This created streaming table

    # 1. First, join orders and customers to get a complete order profile
    orders_with_customers = orders_df.join(
        customer_df, 
        on = "customer_id", 
        how = "inner"
    )

    # 2. Join the items to that profile
    # We use a select to pick specific columns and avoid name collisions
    return (items_df.join(
        orders_with_customers, 
        on = "order_id", 
        how = "inner"
    ).select(
        items_df["order_id"],
        items_df["order_item_id"],
        orders_with_customers["customer_id"],
        customer_df["customer_unique_id"],
        items_df["price"],
        items_df["freight_value"],
        orders_with_customers["order_purchase_timestamp"]
    ))
