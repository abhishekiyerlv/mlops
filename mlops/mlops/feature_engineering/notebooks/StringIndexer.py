# Databricks notebook source
from pyspark.ml.feature import StringIndexer
import mlflow
mlflow.autolog(disable=True)

# COMMAND ----------

dbutils.widgets.text("temp_view", "")
dbutils.widgets.text("column", "")

# COMMAND ----------

temp_view = dbutils.widgets.get("temp_view")
column = dbutils.widgets.get("column")

df = spark.sql(f"SELECT * FROM global_temp.{temp_view}")

# COMMAND ----------

cols = [c.strip() for c in column.split(',')]
indexed_cols = [f"transformed_{col}" for col in cols] 
indexer = StringIndexer(inputCols=cols, outputCols=indexed_cols)
df_indexed = indexer.fit(df).transform(df)
display(df_indexed)

# COMMAND ----------

first_column = df_indexed.columns[0]
cols = [first_column] + indexed_cols
final_df = df_indexed.select(cols)

# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.createOrReplaceGlobalTempView("transformed_df")
