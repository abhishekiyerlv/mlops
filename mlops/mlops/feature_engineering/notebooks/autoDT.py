# Databricks notebook source
import mlflow
mlflow.autolog(disable=True)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import rand
from pyspark.sql.types import StringType

# Initialize Spark session
spark = SparkSession.builder.appName("SyntheticDataset").getOrCreate()

# Generate synthetic data
data = (
    spark.range(0, 500)
    .withColumn("age", (rand() * 52 + 18).cast("int"))  # Age: Random between 18 and 70
    .withColumn("gender", (rand() * 2).cast("int"))  # Gender: 0 or 1
    .withColumn("city", ((rand() * 4).cast("int")).cast(StringType()))  # City: 0 to 3 as strings
    .withColumn("salary", (rand() * 90000 + 30000).cast("int"))  # Salary: Random between 30k and 120k
)

# Cast 'gender' column to StringType
data = data.withColumn("gender", data["gender"].cast(StringType()))

# Define replacement mappings
gender_map = {"0": "Male", "1": "Female"}
city_map = {"0": "New York", "1": "Los Angeles", "2": "Chicago", "3": "Miami"}

# Perform the replacements
data = (
    data.replace(to_replace=gender_map, subset=["gender"])
        .replace(to_replace=city_map, subset=["city"])
)

# Display the dataset
data.show()

# Check the shape of the dataset
num_rows = data.count()
num_columns = len(data.columns)
print(f"Shape of the dataset: ({num_rows}, {num_columns})")


# COMMAND ----------

data.show(6)

# COMMAND ----------

from pyspark.sql.functions import col

# Split the data into training and test sets
train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

# Define the feature columns and the target column
feature_columns = ["age", "gender", "city", "salary"]
target_column = "salary"

# Select features and target for training and test sets
X_train = train_data.select([col(c) for c in feature_columns if c != target_column])
y_train = train_data.select(col(target_column))
X_test = test_data.select([col(c) for c in feature_columns if c != target_column])
y_test = test_data.select(col(target_column))

# Display the training and test sets
display(X_train.count())
display(y_train.count())
display(X_test.count())
display(y_test.count())

# COMMAND ----------

#Scaling Notebook
from pyspark.sql.functions import col, udf
from pyspark.ml.linalg import Vectors
from pyspark.sql.types import ArrayType, DoubleType
from pyspark.ml.feature import VectorAssembler, StandardScaler, MinMaxScaler, RobustScaler
from pyspark.ml import Pipeline

# UDF to create a feature vector
def assemble_features(*args):
    return Vectors.dense(args)

# Register UDF
assemble_features_udf = udf(assemble_features, ArrayType(DoubleType()))

# Manually create feature vector
def apply_scaling(df, scale_method="standard"):
    # Select numeric columns
    numeric_columns = [col for col, dtype in df.dtypes if dtype in ['int', 'double']]

    # Create a new column by applying UDF to assemble features
    #df = df.withColumn("features", assemble_features_udf(*[col(c) for c in numeric_columns]))
    assembler = VectorAssembler(inputCols=numeric_columns, outputCol="features")

    # Choose scaling method (StandardScaler, MinMaxScaler, or RobustScaler)
    if scale_method == "standard":
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)
    elif scale_method == "minmax":
        scaler = MinMaxScaler(inputCol="features", outputCol="scaled_features")
    elif scale_method == "robust":
        scaler = RobustScaler(inputCol="features", outputCol="scaled_features")
    else:
        raise ValueError(f"Unknown scaling method: {scale_method}")

    # Apply the scaling using Pipeline
    pipeline = Pipeline(stages=[assembler, scaler])
    model = pipeline.fit(df)
    df_scaled = model.transform(df)
    
    # Convert the scaled features into individual columns (optional)
    df_scaled = df_scaled.drop("features").withColumnRenamed("scaled_features", "features")
    
    return df_scaled



# COMMAND ----------

numeric_columns = [col for col, dtype in data.dtypes if dtype in ['int', 'double']]
categorical_columns = [col for col, dtype in data.dtypes if dtype == 'str']
display(numeric_columns)
display(categorical_columns)

# COMMAND ----------

scaled_df = apply_scaling(data)
display(scaled_df)

# COMMAND ----------

#Encoding Notebook
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.sql import functions as F

def apply_encoding(df, encoding_method="onehot", categorical_columns=None, target_column=None):
    if categorical_columns is None:
        # If no categorical columns specified, select columns of string type
        categorical_columns = [col for col, dtype in df.dtypes if dtype == 'string']
    
    if encoding_method == "onehot":
        # One-Hot Encoding
        stages = []
        for col in categorical_columns:
            # StringIndexer to convert category labels to numeric indices
            indexer = StringIndexer(inputCol=col, outputCol=col + "_index")
            encoder = OneHotEncoder(inputCol=col + "_index", outputCol=col + "_onehot")
            stages += [indexer, encoder]
        
        # Create a pipeline with indexer and encoder stages
        pipeline = Pipeline(stages=stages)
        model = pipeline.fit(df)
        df_encoded = model.transform(df)
        
        # Drop the temporary index columns after encoding
        df_encoded = df_encoded.drop(*[col + "_index" for col in categorical_columns])
    
    elif encoding_method == "label":
        # Label Encoding
        stages = []
        for col in categorical_columns:
            # StringIndexer for label encoding
            indexer = StringIndexer(inputCol=col, outputCol=col + "_encoded")
            stages.append(indexer)
        
        # Create a pipeline with indexers
        pipeline = Pipeline(stages=stages)
        model = pipeline.fit(df)
        df_encoded = model.transform(df)
    
    elif encoding_method == "target":
        # Target Encoding
        if target_column is None:
            raise ValueError("Target column must be specified for target encoding.")
        
        for col in categorical_columns:
            # Calculate the mean of the target column for each category
            target_mean = df.groupBy(col).agg(F.mean(target_column).alias(col + "_encoded"))
            df_encoded = df.join(target_mean, on=col, how='left')
    
    else:
        raise ValueError(f"Unknown encoding method: {encoding_method}")
    
    return df_encoded


# COMMAND ----------

# Feature Extraction Notebook
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr

# Multiplication: Sales = No. of units sold * cost price per unit
def multiply_features(df: DataFrame, column1: str, column2: str, new_feature_name: str) -> DataFrame:
    return df.withColumn(new_feature_name, col(column1) * col(column2))

# Division: Sales / No. of units sold = cost price per unit
def divide_features(df: DataFrame, column1: str, column2: str, new_feature_name: str) -> DataFrame:
    return df.withColumn(new_feature_name, col(column1) / col(column2))

# Subtraction: Revenue - COGS = Profit
def subtract_features(df: DataFrame, column1: str, column2: str, new_feature_name: str) -> DataFrame:
    return df.withColumn(new_feature_name, col(column1) - col(column2))

# Addition: Revenue = Profit + COGS
def add_features(df: DataFrame, column1: str, column2: str, new_feature_name: str) -> DataFrame:
    return df.withColumn(new_feature_name, col(column1) + col(column2))

# Grouping and Aggregation: Revenue per employee
def group_by_features(
    df: DataFrame, 
    group_by_column: str, 
    aggregate_column: str, 
    aggregate_function, 
    new_feature_name: str
) -> DataFrame:
    # Perform groupBy and aggregation
    df_grouped = df.groupBy(group_by_column).agg(aggregate_function(col(aggregate_column)).alias(new_feature_name))
    # Join aggregated data back to the original DataFrame
    return df.join(df_grouped, on=group_by_column, how="left")


# COMMAND ----------

# Test extraction notebook
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

# Initialize Spark session
spark = SparkSession.builder.appName("FeatureCreation").getOrCreate()

# Sample DataFrame
tdata = [
    (1, 10, 5, 2),
    (2, 15, 6, 3),
    (3, 20, 7, 4)
]
columns = ["id", "units_sold", "cost_per_unit", "revenue"]

tdf = spark.createDataFrame(tdata, columns)

tdf = multiply_features(tdf, "units_sold", "cost_per_unit", "sales")
tdf = divide_features(tdf, "sales", "units_sold", "price_per_unit")
tdf = subtract_features(tdf, "revenue", "sales", "profit")
tdf = add_features(tdf, "profit", "sales", "total_revenue")
tdf = group_by_features(tdf, "id", "revenue", sum, "total_revenue_per_id")

tdf.show()


# COMMAND ----------

# # Creating new features notebook

# # Sales = No. of units sold * cost price per unit
# def multiplied_features(df, column1, column2, new_feature_name=None):
#     if new_feature_name is None:
#         return df[f"{column1}"]*df[f"{column2}"]
#     df[f"{new_feature_name}"] = df[f"{column1}"]*df[f"{column2}"]
#     print(f"Created new feature: {new_feature_name}")
#     return None

# # Sales / No. of units sold = cost price per unit
# def div_col1_by_col2_features(df, column1, column2, new_feature_name=None):
#     if new_feature_name is None:
#         return df[f"{column1}"]/df[f"{column2}"]
#     df[f"{new_feature_name}"] = df[f"{column1}"]/df[f"{column2}"]
#     print(f"Created new feature: {new_feature_name}")
#     return None

# # Revenue - COGS = Profit
# def diff_col2_from_col1_features(df, column1, column2, new_feature_name=None):
#     if new_feature_name is None:
#         return df[f"{column1}"]-df[f"{column2}"]
#     df[f"{new_feature_name}"] = df[f"{column1}"]-df[f"{column2}"]
#     print(f"Created new feature: {new_feature_name}")
#     return None

# # Revenue = Profit + COGS
# def add_features(df, column1, column2, new_feature_name=None):
#     if new_feature_name is None:
#         return df[f"{column1}"]+df[f"{column2}"]
#     df[f"{new_feature_name}"] = df[f"{column1}"]+df[f"{column2}"]
#     print(f"Created new feature: {new_feature_name}")
#     return None

# # Revenue per employee
# def group_by_features(df, group_by_column, aggregate_column, aggregate_function, new_feature_name=None):
#     df_grouped = df.groupBy(group_by_column).agg(aggregate_function(aggregate_column).alias(new_feature_name))
#     df = df.join(df_grouped, on=group_by_column, how='left')
#     return df

# COMMAND ----------

print('hey')

# COMMAND ----------

# Apply One-Hot Encoding
# df_encoded_onehot = apply_encoding(data)
# df_encoded_onehot.show()

# Apply Label Encoding
# df_encoded_label = apply_encoding(data, encoding_method="label")
# df_encoded_label.show()

# Apply Target Encoding (provide target column)
df_encoded_target = apply_encoding(scaled_df, encoding_method="target", target_column="salary")
df_encoded_target.show()

# COMMAND ----------

spark.sql('SELECT t.* FROM transformation t JOIN dataset_column_transformation dt ON t.id = dt.transformation_id')

# COMMAND ----------

import pyspark
print(f"PySpark Version: {pyspark.__version__}")

# COMMAND ----------


