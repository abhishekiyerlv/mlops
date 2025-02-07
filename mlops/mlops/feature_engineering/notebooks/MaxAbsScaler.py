# Databricks notebook source
from pyspark.ml.feature import MaxAbsScaler, VectorAssembler
import mlflow
mlflow.autolog(disable=True)

# COMMAND ----------

dbutils.widgets.text("temp_view", "")
dbutils.widgets.text("column", "")

# COMMAND ----------

temp_view = dbutils.widgets.get("temp_view")
column = dbutils.widgets.get("column")
transformation = dbutils.widgets.get("transformation")
df = spark.sql(f"SELECT * FROM global_temp.{temp_view}")

# COMMAND ----------

assembler = VectorAssembler(inputCols=[column], outputCol='vector_{}'.format(column))
vectorized_df = assembler.transform(df)

# COMMAND ----------

scaler = MaxAbsScaler(inputCol='vector_{}'.format(column), outputCol="scaled_{}".format(column))
scaled_df = scaler.fit(vectorized_df).transform(vectorized_df)

# COMMAND ----------

display(scaled_df)
