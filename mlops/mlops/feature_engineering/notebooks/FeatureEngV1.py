# Databricks notebook source
from pyspark.sql.functions import  col
from databricks.feature_store import FeatureStoreClient

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ds_training_1.mlops.dataset

# COMMAND ----------

config = spark.sql("""
SELECT ds_training_1.mlops.dataset_column.dataset_id, ds_training_1.mlops.dataset_column_transformation.transformation_id,ds_training_1.mlops.dataset_column_transformation.column_id, ds_training_1.mlops.dataset_column.column_name, ds_training_1.mlops.transformation.name as transformation_name, ds_training_1.mlops.dataset_column_transformation.input_paramter, ds_training_1.mlops.transformation.notebook_name as notebook_name, ds_training_1.mlops.transformation.notebook_path, 
ds_training_1.mlops.dataset.location as dataset_location,
ds_training_1.mlops.dataset.primary_key as primary_key,
ds_training_1.mlops.dataset.target as target,
ds_training_1.mlops.dataset.name as dataset_name,
ds_training_1.mlops.dataset_column_transformation.status, 
ds_training_1.mlops.dataset_column_transformation.data_update_mode
FROM ds_training_1.mlops.dataset_column_transformation 
INNER JOIN ds_training_1.mlops.transformation
ON ds_training_1.mlops.dataset_column_transformation.transformation_id = ds_training_1.mlops.transformation.id 
INNER JOIN ds_training_1.mlops.dataset_column
ON ds_training_1.mlops.dataset_column.id = ds_training_1.mlops.dataset_column_transformation.column_id
JOIN ds_training_1.mlops.dataset
ON ds_training_1.mlops.dataset_column.dataset_id = ds_training_1.mlops.dataset.id
WHERE status = 'not completed'
""")
display(config)

# COMMAND ----------

#fix the audit and run!!!

# COMMAND ----------

def get_dataset(id, mode, location):
    last_processed_row = spark.sql("SELECT * FROM ds_training_1.mlops.transformation_audit WHERE dataset_id = {} ORDER BY `timestamp` DESC LIMIT 1".format(id))
    dataset = "SELECT * FROM {}".format(location)
    dataset = spark.sql(dataset)
    count = dataset.count()
    if last_processed_row.count() == 0 or mode == "overwrite":
        return count, dataset
    else:
        count = int(last_processed_row.select("last_processed_row").collect()[0][0])
        dataset = dataset.subtract(dataset.limit(count))
        return count, dataset

# COMMAND ----------

dataset_ids = config.select("dataset_id").distinct().collect()
dataset_ids = [row.dataset_id for row in dataset_ids]
final_ids = {}
for id in dataset_ids:
    final_df = None
    filtered_df = config.filter(col("dataset_id") == id)
    trans_ids = filtered_df.select("transformation_id").distinct().collect()
    trans_ids = [row.transformation_id for row in trans_ids]
    dataset_location = filtered_df.select("dataset_location").collect()[0][0]
    mode = filtered_df.select("data_update_mode").collect()[0][0]
    count, dataset = get_dataset(id, mode, dataset_location)
    dataset.createOrReplaceGlobalTempView("temp_view")
    num = int(filtered_df.select("primary_key").collect()[0][0])
    num2 = int(filtered_df.select("target").collect()[0][0])
    primary_key = dataset.schema.names[num]
    target = dataset.schema.names[num2]
    for trans_id in trans_ids:
        transformation_df = filtered_df.filter(col("transformation_id") == trans_id)
        column = transformation_df.select("column_name").rdd.flatMap(lambda x: x).collect()
        column = ",".join(column)
        column_id = transformation_df.select("column_id").rdd.flatMap(lambda x: x).collect()
        final_ids[trans_id] = column_id
        notebook_path =  transformation_df.select("notebook_path").collect()[0][0]
        result = dbutils.notebook.run(notebook_path,  timeout_seconds=3600, arguments = {"temp_view": "temp_view","column": column})
        transformed_df = spark.sql("SELECT * FROM global_temp.transformed_df")
        if final_df is None:
            final_df = transformed_df
        else:
            final_df = final_df.join(transformed_df, on=primary_key, how="inner")
    transformed_columns = [col for col in final_df.columns if col != primary_key]
    non_transformed_columns = [col for col in dataset.columns if col != primary_key and f'transformed_{col}' not in transformed_columns and col != target]
    #print("The transformed columns are " )
    #print(transformed_columns)
    #print("----------")
    #print("The non transformed columns are ")
    #print(non_transformed_columns)
    non_transformed_dataset = dataset.select([primary_key] + non_transformed_columns)
    print(non_transformed_columns)
    if len(non_transformed_columns) > 0:
        final_df = final_df.join(non_transformed_dataset, on=primary_key, how='left')
    #print("----------")
    #print("The final transformed dataframe")
    display(final_df)

    fs = FeatureStoreClient()
    dataset_name = transformation_df.select("dataset_name").collect()[0][0]
    table_name = "ds_training_1.feature_store.{}".format(dataset_name)
    try:
        fs.get_table(table_name)
        if mode == "append":
            fs.ingest(table_name, final_df, mode="append")
            row_count = final_df.count() + count
        else: 
            fs.create_table(name=table_name, primary_keys=[primary_key], df=final_df)
            row_count = final_df.count()
    except:
        fs.create_table(name=table_name, primary_keys=[primary_key], df=final_df)
        row_count = count
    for transformation_id, column_ids in final_ids.items():
        for column_id in column_ids:
            query = f"UPDATE ds_training_1.mlops.dataset_column_transformation SET status = 'completed' WHERE column_id = {column_id} AND transformation_id = {transformation_id};"
            query2 = f"INSERT INTO ds_training_1.mlops.transformation_audit  (transformation_id, status, timestamp, last_processed_row, dataset_id) VALUES ({transformation_id}, 'success', current_timestamp, {row_count}, {id})"
            spark.sql(query)
            spark.sql(query2)

# COMMAND ----------

display(config)

# COMMAND ----------

# MAGIC %sql
# MAGIC --UPDATE ds_training_1.mlops.dataset_column_transformation
# MAGIC --SET status = 'completed'
# MAGIC --WHERE status = "not completed"

# COMMAND ----------

display(config)
