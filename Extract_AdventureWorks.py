# Databricks notebook source
spark.sql("USE CATALOG workspace")
tables_df = spark.sql("SHOW TABLES IN bronze")
tables_df.display()

# COMMAND ----------

tables = [row['tableName'] for row in tables_df.collect()]
row_counts = []
 
for table in tables:
    count_df = spark.sql(f"SELECT COUNT(*) as row_count FROM bronze.{table}")
    row_count = count_df.collect()[0]['row_count']
    row_counts.append((table, row_count))
 
row_counts_df = spark.createDataFrame(row_counts, ["table_name", "row_count"])
display(row_counts_df)

# COMMAND ----------

current_username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
current_username

# COMMAND ----------

