# Databricks notebook source
from pyspark.ml.feature import StandardScaler, VectorAssembler
import mlflow
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, DoubleType
mlflow.autolog(disable=True)

# COMMAND ----------

dbutils.widgets.text("temp_view", "")
dbutils.widgets.text("column", "")

# COMMAND ----------

temp_view = dbutils.widgets.get("temp_view")
column = dbutils.widgets.get("column")

df = spark.sql(f"SELECT * FROM global_temp.{temp_view}")

# COMMAND ----------

#Outliers - Robust scaling
#Normal distribution - Standard Scaling
#Distance based models SVM and KNN - Normalization  
#Neural network based models - Min Max Normalization
#Large differences between points - Log normalization

# COMMAND ----------

cols = [c.strip() for c in column.split(',')]
assembler = VectorAssembler(inputCols=cols, outputCol='features')
assembled_df = assembler.transform(df)


# COMMAND ----------


scaler = StandardScaler(inputCol='features', outputCol="scaled_features", withStd=True, withMean=False)
scaled_df = scaler.fit(assembled_df).transform(assembled_df)

# COMMAND ----------

display(scaled_df)

# COMMAND ----------

def vector_to_list(vector):
    return vector.toArray().tolist()

# COMMAND ----------

vector_to_list_udf = udf(vector_to_list, ArrayType(DoubleType()))

# COMMAND ----------

scaled_df = scaled_df.withColumn("scaled_array", vector_to_list_udf(scaled_df["scaled_features"]))
cols = ["transformed_{}".format(c) for c in cols]
for i, col_name in enumerate(cols):
    scaled_df = scaled_df.withColumn(col_name, scaled_df["scaled_array"].getItem(i))

first_column = scaled_df.columns[0]
cols = [first_column] + cols
final_df = scaled_df.select(cols)

# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.createOrReplaceGlobalTempView("transformed_df")

# COMMAND ----------


