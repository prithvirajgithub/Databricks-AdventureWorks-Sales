# Databricks notebook source
# MAGIC %md
# MAGIC ##### sales_analytics

# COMMAND ----------

# MAGIC %sql
# MAGIC Drop table if exists Gold.sales_analytics

# COMMAND ----------

sales_combined_df = spark.sql(""" Select * from silver.factsalescombined """)
sales_combined_df.write.format("delta").mode("overwrite") \
  .partitionBy("SalesTerritoryKey") \
  .saveAsTable("Gold.sales_analytics")

spark.sql("""
    OPTIMIZE Gold.sales_analytics
    ZORDER BY (CustomerKey)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### customer_ltv

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Gold.customer_ltv

# COMMAND ----------

customer_ltv_df = spark.sql("""
WITH agg AS (
  SELECT 
    Customer,
    CustomerKey,
    SUM(Sales_Amount) AS TotalSpend,
    COUNT(DISTINCT ProductKey) AS TotalPurchases,
    AVG(Sales_Amount) AS AvgPurchaseValue
  FROM silver.FactSalesCombined
  GROUP BY Customer, CustomerKey
)

SELECT 
  Customer,
  CustomerKey,
  TotalSpend AS TotalSpend,
  TotalPurchases AS TotalPurchases,
  AvgPurchaseValue AS AvgPurchaseValue,
  CASE 
    WHEN TotalSpend > 100000 THEN 'Platinum'
    WHEN TotalSpend > 50000 THEN 'Gold'
    WHEN TotalSpend > 20000 THEN 'Silver'
    ELSE 'Bronze'
  END AS LoyaltyScore
FROM agg
""")
# customer_ltv_df.write.format("delta").mode("overwrite").partitionBy("Customer").saveAsTable("workspace.Gold.customer_ltv")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### regional_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Gold.regional_sales

# COMMAND ----------

regional_sales_df = spark.sql("""
Select Region, SalesTerritoryKey, SUM(Sales_Amount) AS TotalSales, DENSE_RANK() OVER (ORDER BY SUM(Sales_Amount) DESC) AS SalesRank
FROM silver.FactSalesCombined
GROUP BY Region, SalesTerritoryKey  """)

regional_sales_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("Gold.regional_sales")

spark.sql("""
    OPTIMIZE Gold.regional_sales
    ZORDER BY (SalesTerritoryKey)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### monthly_growth

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Gold.monthly_growth

# COMMAND ----------

df = spark.sql("""
WITH MonthlySales AS (
    SELECT
        DATE_FORMAT(s.Date, 'yyyy-MM') AS Month,
        SUM(s.Sales_Amount) AS TotalSales
    FROM silver.FactSalesCombined s
    GROUP BY DATE_FORMAT(s.Date, 'yyyy-MM')
),
MoMChange AS (
   SELECT
        Month,
        TotalSales,
        LAG(TotalSales) OVER (ORDER BY Month) AS PreviousMonthSales,
        ROUND(
            100.0 * (TotalSales - LAG(TotalSales) OVER (ORDER BY Month)) / NULLIF(LAG(TotalSales) OVER (ORDER BY Month), 0),
            2
        ) AS MoM_Percent_Change
    FROM MonthlySales
)
SELECT *
FROM MoMChange
ORDER BY Month; """)

df.write.format("delta").mode("overwrite").saveAsTable("Gold.monthly_growth")

# COMMAND ----------

df.explain("formatted")

# COMMAND ----------

# %sql select * from Gold.customer_ltv

# COMMAND ----------

df = spark.sql(""" select sum(TotalPurchases) from Gold.customer_ltv where CustomerKey = 29079""")

# COMMAND ----------

from pyspark.sql.functions import col

print(f"Total Customers in LTV Table: {customer_ltv_df.count()}")
print(f"Total Records in Regional Sales Table: {regional_sales_df.count()}")
negative_sales_check = sales_combined_df.filter(col("Sales_Amount") < 0).count()
if negative_sales_check == 0:
    print("Business Logic Validation Passed: No negative sales values.")
else:
    print("Warning: Business Logic Validation Failed! Negative sales values exist.")

# COMMAND ----------


print(dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get())