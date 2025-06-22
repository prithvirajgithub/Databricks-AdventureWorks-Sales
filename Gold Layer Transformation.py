# Databricks notebook source
sales_combined_df = spark.table("silver.factsalescombined")
total_revenue_per_customer_df = spark.table("silver.facttotalrevenuepercustomer")
customer_segment_df = spark.table("silver.factcustomersegment")
monthly_sales_df = spark.table("silver.factmonthlysales")
fact_top_products_df = spark.table("silver.facttopproducts")
fact_yoy_growth_df = spark.table("silver.factyoysalesgrowth")

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

sales_combined_df.write \
    .mode("overwrite") \
    .format("delta") \
    .partitionBy("SalesTerritoryKey") \
    .saveAsTable("gold.sales_analytics")

spark.sql("OPTIMIZE gold.sales_analytics ZORDER BY (CustomerKey)")

# COMMAND ----------

from pyspark.sql.functions import count, sum, round, when, col

cltv_df = (
    sales_combined_df
    .groupBy("CustomerKey")
    .agg(
        sum("Sales_Amount").alias("TotalSpend"),
        count("SalesOrderLineKey").alias("TotalPurchases"),
    )
    .withColumn("AvgPurchaseValue", round(col("TotalSpend") / col("TotalPurchases"), 2))
    .withColumn("LoyaltyScore",
        when(col("TotalSpend") > 100000, "Platinum")
        .when(col("TotalSpend") > 50000, "Gold")
        .when(col("TotalSpend") > 20000, "Silver")
        .otherwise("Bronze")
    )
)

cltv_df.write.mode("overwrite").format("delta").saveAsTable("gold.customer_ltv")

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank

regional_sales_df = (
    sales_combined_df
    .groupBy("Region", "SalesTerritoryKey")
    .agg(sum("Sales_Amount").alias("TotalSales"))
    .withColumn("Rank", rank().over(Window.partitionBy("Region", "SalesTerritoryKey").orderBy(col("TotalSales").desc())))
)

regional_sales_df.write.mode("overwrite").format("delta").saveAsTable("gold.regional_sales")

spark.sql("OPTIMIZE gold.regional_sales ZORDER BY (SalesTerritoryKey)")

# COMMAND ----------

from pyspark.sql.functions import lag

monthly_growth_df = (
    monthly_sales_df
    .withColumn("PreviousMonthSales", lag("TotalSales").over(Window.partitionBy("YearMonth").orderBy("YearMonth")))
    .withColumn("MoM_GrowthRate",
        round(100 * (col("TotalSales") - col("PreviousMonthSales")) / col("PreviousMonthSales"), 2)
    )
    .filter(col("PreviousMonthSales").isNotNull())
)

monthly_growth_df.write.mode("overwrite").format("delta").saveAsTable("gold.monthly_growth")

# COMMAND ----------

print(f"Total Customers in LTV Table: {cltv_df.count()}")
print(f"Total Records in Regional Sales Table: {regional_sales_df.count()}")

negative_sales_check = sales_combined_df.filter(col("Sales_Amount") < 0).count()

if negative_sales_check == 0:
    print("Business Logic Validation Passed: No negative sales values.")
else:
    print("Warning: Business Logic Validation Failed! Negative sales values exist.")

# COMMAND ----------

print(dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get())