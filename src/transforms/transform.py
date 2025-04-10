from pyspark.sql import DataFrame
from pyspark.sql.functions import col, split, month, round, to_timestamp, date_format, split, trim, sum as _sum,min , max, round, count, avg
from pyspark import StorageLevel

def group_and_aggregate(
    df: DataFrame,
    group_cols: list[str],
    agg_col: str,
    alias: str,
    agg_func: str = "sum",
    round_result: bool = True,
    limit: int = None
) -> DataFrame:
    agg_expr = {
    "sum": _sum(col(agg_col)),
    "count": count(col(agg_col)),
    "avg": avg(col(agg_col)),
    "min": min(col(agg_col)),
    "max": max(col(agg_col))
}.get(agg_func)

    if agg_expr is None:
        raise ValueError(f"Unsupported aggregation function: '{agg_func}'.")

    if agg_func == "sum" or agg_func == "avg" and round_result:
        agg_expr = round(agg_expr, 2)

    result = df.groupBy(*group_cols).agg(agg_expr.alias(alias)).orderBy(col(alias).desc())

    if limit:
        result = result.limit(limit)

    return result

def obtain_metrics(df: DataFrame) -> dict[str, DataFrame]:
    metrics ={
       "monthly_sales" : group_and_aggregate(df, ["month_date"], "total_price", "sales", "sum", True ),
       "top_selling_citys_states" : group_and_aggregate(df, ["city", "state"], "total_price", "sales", "sum"),
       "most_sold_products" : group_and_aggregate(df, ["product"], "quantity", "quantity", "sum", False),
       "best_hours" : group_and_aggregate(df, ["order_hour"], "order_id", "quantity","count"),
       "avg_sales_by_city_state" : group_and_aggregate(df, ["city", "state"], "total_price", "avg_sales", "avg", True),
       "avg_quantity_per_product" : group_and_aggregate( df, ["product"], "quantity", "avg_quantity", "avg", True),
       "avg_sales_by_month" : group_and_aggregate(df, ["month_date"], "total_price", "avg_sales", "avg", True),
       "most_sold_products_by_month" : group_and_aggregate(df, ["month_date", "product"], "quantity", "total_quantity", "sum", False)
    }
    
    return metrics
def transform_data(df: DataFrame):
  
    return df.na.replace(["", " "], None) \
       .withColumnRenamed("Order ID", "order_id") \
       .withColumnRenamed("Quantity Ordered", "quantity") \
       .withColumnRenamed("Price Each", "price_each") \
       .withColumnRenamed("Purchase Address", "purchase_address") \
       .withColumnRenamed("Product", "product") \
       .withColumn("order_date", to_timestamp("Order Date", "MM/dd/yy HH:mm")) \
       .withColumn("month_date", month("order_date")) \
       .withColumn("order_hour", date_format("order_date", "HH:mm")) \
       .withColumn("street", trim(split(col("purchase_address"), ",")[0])) \
       .withColumn("city", trim(split(col("purchase_address"), ",")[1])) \
       .withColumn("state", trim(split(split(col("purchase_address"), ",")[2], " ")[1])) \
       .withColumn("zip_code", trim(split(split(col("purchase_address"), ",")[2], " ")[2])) \
       .withColumn("total_price", col("price_each")*col("quantity")) \
       .drop("Order Date") \
       .filter(col("order_id").isNotNull()) \
       .persist(StorageLevel.MEMORY_AND_DISK)
