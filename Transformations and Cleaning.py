# Databricks notebook source
spark.sql("USE CATALOG workspace")

# COMMAND ----------

tables_df = spark.sql("SHOW TABLES IN Bronze")
bronze_tables = [row['tableName'] for row in tables_df.collect()]

def update_column_names(df):
    for col_name in df.columns:
        new_col_name = col_name.replace(" ", "_")
        df = df.withColumnRenamed(col_name, new_col_name)
    return df

bronze_dfs = {}
for table in bronze_tables:
    df = spark.read.table(f"Bronze.{table}")
    df_cleaned = update_column_names(df)
    bronze_dfs[table] = df_cleaned
    print(f"Updated column names for table: {table}")

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col

# COMMAND ----------

sales_df = bronze_dfs["factsales"]

sales_df = sales_df.withColumn("Unit_Price", regexp_replace(col("Unit_Price"), "[$,]", "").cast("double")) \
                   .withColumn("Extended_Amount", regexp_replace(col("Extended_Amount"), "[$,]", "").cast("double")) \
                   .withColumn("Product_Standard_Cost", regexp_replace(col("Product_Standard_Cost"), "[$,]", "").cast("double")) \
                   .withColumn("Total_Product_Cost", regexp_replace(col("Total_Product_Cost"), "[$,]", "").cast("double")) \
                   .withColumn("Sales_Amount", regexp_replace(col("Sales_Amount"), "[$,]", "").cast("double"))

bronze_dfs["factsales"] = sales_df

# COMMAND ----------

from pyspark.sql.types import StringType, IntegerType, DoubleType, FloatType, LongType, BooleanType

def missingornull(df):
    default_values = {}
    for field in df.schema.fields:
        dtype = field.dataType
        if isinstance(dtype, StringType):
            default_values[field.name] = "null"
        elif isinstance(dtype, BooleanType):
            default_values[field.name] = False
        elif isinstance(dtype, (IntegerType, LongType)):
            default_values[field.name] = 0
        elif isinstance(dtype, (DoubleType, FloatType)):
            default_values[field.name] = 0.0
        else:
            default_values[field.name] = None
    return df.fillna(default_values)

customer_df = bronze_dfs["dimcustomer"]
sales_df = bronze_dfs["factsales"]

customer_df = missingornull(customer_df)
sales_df = missingornull(sales_df)

bronze_dfs["dimcustomer"] = customer_df
bronze_dfs["factsales"] = sales_df


# COMMAND ----------

from pyspark.sql.functions import to_date
date_df = bronze_dfs["dimdate"]
date_df = date_df.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))
bronze_dfs["dimdate"] = date_df

# COMMAND ----------

from pyspark.sql.functions import lit
df = bronze_dfs["factsales"].withColumn("Currency", lit("USD"))
bronze_dfs["factsales"] = df

# COMMAND ----------

date_df         = bronze_dfs["dimdate"]
sales_df        = bronze_dfs["factsales"]
product_df      = bronze_dfs["dimproduct"]
customer_df     = bronze_dfs["dimcustomer"]
reseller_df     = bronze_dfs["dimreseller"]
order_df        = bronze_dfs["factsalesorderdetails"]
territory_df    = bronze_dfs["factsalesterritorydetails"]

salescombined_df = (
    sales_df
      .join(order_df,on="SalesOrderLineKey",how="inner")
      .join(territory_df,on="SalesTerritoryKey",how="inner")
      .join(customer_df,on="CustomerKey",how="inner")
      .join(product_df,on="ProductKey",how="inner")
      .join(date_df,sales_df["OrderDateKey"] == date_df["DateKey"], how="inner")
      .drop(date_df["DateKey"])
)

bronze_dfs["salescombined"] = salescombined_df

# COMMAND ----------

from pyspark.sql import functions as F
total_revenue_per_customer_df = (salescombined_df.groupBy("CustomerKey").agg(F.sum("Sales_Amount").alias("TotalRevenue")))
bronze_dfs["totalrevenuepercustomer"] = total_revenue_per_customer_df

# COMMAND ----------

from pyspark.sql.window import Window
windowbyyear = Window.orderBy("Fiscal_Year")
sales_growth_df = (
    salescombined_df
      .groupBy("Fiscal_Year")
      .agg(F.sum("Sales_Amount").alias("TotalSales"))
      .withColumn("PreviousYearSales", F.lag("TotalSales").over(windowbyyear))
      .withColumn(
          "YoY_GrowthRate",
          F.round(100 * (F.col("TotalSales") - F.col("PreviousYearSales")) / F.col("PreviousYearSales"), 2)
      )
      .filter(F.col("PreviousYearSales").isNotNull())
)
 
bronze_dfs["salesgrowth"] = sales_growth_df

# COMMAND ----------

customer_segment_df = (
    total_revenue_per_customer_df
      .withColumn(
          "Segment",
          F.when(F.col("TotalRevenue") > 50000, "High_Value")
           .when(F.col("TotalRevenue") > 20000, "Medium_Value")
           .otherwise("Low_Value")
      )
)
bronze_dfs["CustomerSegment"] = customer_segment_df

# COMMAND ----------

top_products_df = (
    salescombined_df
      .groupBy("ProductKey", "Product")
      .agg(F.sum("Sales_Amount").alias("TotalSales"))
      .orderBy(F.col("TotalSales").desc())
      .limit(10)
)
bronze_dfs["TopProducts"] = top_products_df

# COMMAND ----------

monthly_sales_df = (
    salescombined_df
      .withColumn("YearMonth", F.date_format("Date", "yyyy-MM"))
      .groupBy("YearMonth")
      .agg(F.sum("Sales_Amount").alias("TotalSales"))
      .orderBy("YearMonth")
)
bronze_dfs["MonthlySales"] = monthly_sales_df

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
bronze_dfs["salescombined"].write.mode("overwrite").format("delta").saveAsTable("silver.factsalescombined")
bronze_dfs["totalrevenuepercustomer"].write.mode("overwrite").format("delta").saveAsTable("silver.facttotalrevenuepercustomer")
bronze_dfs["CustomerSegment"].write.mode("overwrite").format("delta").saveAsTable("silver.factcustomersegment")
bronze_dfs["TopProducts"].write.mode("overwrite").format("delta").saveAsTable("silver.facttopproducts")
bronze_dfs["MonthlySales"].write.mode("overwrite").format("delta").saveAsTable("silver.factmonthlysales")

# COMMAND ----------

bronze_dfs["salescombined"].write.mode("overwrite").format("delta").option("overwriteSchema", "true").partitionBy("SalesTerritoryKey").saveAsTable("silver.factsalescombined")

# COMMAND ----------

spark.sql("""OPTIMIZE silver.factsalescombined ZORDER BY (CustomerKey)""")

# COMMAND ----------

row_count = salescombined_df.count()
print(f"Total Records in Sales Combined Data: {row_count}")
row_count = total_revenue_per_customer_df.count()
print(f"Total Records in Revenue per customer Data: {row_count}")
row_count = customer_segment_df.count()
print(f"Total Records in Customer Segmentation Data: {row_count}")
row_count = top_products_df.count()
print(f"Total Records in Top Products Data: {row_count}")
row_count = monthly_sales_df.count()
print(f"Total Records in MonthlySales Data: {row_count}")

# COMMAND ----------

business_logic_check = salescombined_df.filter(col("Sales_Amount") < 0).count()
if business_logic_check == 0:
    print("Business Logic Validation Passed: No negative TotalAmount values.")
else:
    print("Warning: Business Logic Validation Failed! Negative TotalAmount values exist.")

# COMMAND ----------

current_username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
current_username