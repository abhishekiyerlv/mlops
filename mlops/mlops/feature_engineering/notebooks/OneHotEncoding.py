# Databricks notebook source
from pyspark.ml.feature import StringIndexer, OneHotEncoder
import mlflow
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, DoubleType, FloatType
from pyspark.ml.linalg import SparseVector
mlflow.autolog(disable=True)

# COMMAND ----------

display(dbutils.fs.ls('/databricks-datasets'))

# COMMAND ----------

file_path = "dbfs:/databricks-datasets/flights/"
df = spark.read.format("csv").option("header", "true").load(file_path)
df = df.limit(500)
display(df)


# COMMAND ----------

df.createOrReplaceGlobalTempView("temp_view")

# COMMAND ----------

dbutils.widgets.text("temp_view", "")
dbutils.widgets.text("column", "")

# COMMAND ----------

temp_view = dbutils.widgets.get("temp_view")
column = dbutils.widgets.get("column")

df = spark.sql(f"SELECT * FROM global_temp.{temp_view}")

# COMMAND ----------

cols = [c.strip() for c in column.split(',')]
indexed_cols = [f"{col}_indexed" for col in cols] 
indexer = StringIndexer(inputCols=cols, outputCols=indexed_cols)
df_indexed = indexer.fit(df).transform(df)
display(df_indexed)

# COMMAND ----------

encoded_cols = [f"{col.split('_')[0]}_encoded" for col in indexed_cols]
encoder = OneHotEncoder(inputCols=indexed_cols, outputCols=encoded_cols)
df_encoded = encoder.fit(df_indexed).transform(df_indexed)

# COMMAND ----------

display(df_encoded)

# COMMAND ----------



# COMMAND ----------

scaled_df = df_encoded.withColumn("encoded_array", vector_to_list_udf(df_encoded["origin_encoded"]))
cols = ["encoded_{}".format(c) for c in cols]
for i, col_name in enumerate(cols):
    scaled_df = scaled_df.withColumn(col_name, scaled_df["scaled_array"].getItem(i))

first_column = scaled_df.columns[0]
cols = [first_column] + cols
final_df = scaled_df.select(cols)

# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.createOrReplaceGlobalTempView("transformed_df")
