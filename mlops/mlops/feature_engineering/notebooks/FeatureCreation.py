# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# COMMAND ----------

# # Add a widget to select a column
# dbutils.widgets.text("column_name", "")
# selected_column = dbutils.widgets.get("column_name")

# COMMAND ----------

from pyspark.sql.functions import col, expr

# Add widgets to select columns and operation
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("column_name_1", "")
dbutils.widgets.text("column_name_2", "")
dbutils.widgets.dropdown("operation", "addition", ["addition", "multiplication", "division"])

selected_table = dbutils.widgets.get("table_name")
selected_column_1 = dbutils.widgets.get("column_name_1")
selected_column_2 = dbutils.widgets.get("column_name_2")
selected_operation = dbutils.widgets.get("operation")

# Load data from the selected table
df = spark.table(selected_table)

# Display the initial DataFrame
# display(df)


# Check if the selected columns are provided and valid
if selected_column_1 in df.columns and selected_column_2 in df.columns:
    if selected_operation == "addition":
        # Addition
        df_result = df.withColumn("result", col(selected_column_1) + col(selected_column_2))
    elif selected_operation == "multiplication":
        # Multiplication
        df_result = df.withColumn("result", col(selected_column_1) * col(selected_column_2))
    elif selected_operation == "division":
        # Division (handling division by zero)
        df_result = df.withColumn(
            "result",
            expr(f"CASE WHEN {selected_column_2} != 0 THEN {selected_column_1} / {selected_column_2} ELSE NULL END")
        )
    else:
        print("Invalid operation selected.")
        df_result = df

    # Display the final DataFrame with the result
    display(df_result)
else:
    print(f"Please provide valid column names using the widgets. Available columns: {', '.join(df.columns)}")
