# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE ds_training_1.mlops.dataset ( 
# MAGIC   id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC   name STRING,
# MAGIC   description STRING,
# MAGIC   created_by STRING,
# MAGIC   created_timestamp STRING 
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO ds_training_1.mlops.dataset (name, description, created_by, created_timestamp)
# MAGIC VALUES 
# MAGIC ('observe_ai.default.access_audit', 'access audit dataset', 'brindavivek.kotha@latentviewo365.onmicrosoft.com', '2024-11-11, 15:42:39'),
# MAGIC ('ds_training_1.ds_bronze.job_cost_details', 'Job Cost details', 'jeno.allwyn@latentviewo365.onmicrosoft.com', '2024-11-15, 15:44:52');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ds_training_1.mlops.dataset

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE ds_training_1.mlops.dataset_column (
# MAGIC   id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC   dataset_id INT, 
# MAGIC   column_name STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO ds_training_1.mlops.dataset_column (dataset_id, column_name)
# MAGIC VALUES 
# MAGIC (1, 'action_type'),
# MAGIC (1, 'user_type'),
# MAGIC (2, 'email'),
# MAGIC (2, 'cost_usd');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE ds_training_1.mlops.transformation (
# MAGIC   id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC   name STRING,
# MAGIC   notebook_name STRING,
# MAGIC   notebook_path STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO ds_training_1.mlops.transformation (name, notebook_name, notebook_path)
# MAGIC VALUES 
# MAGIC ('Scaling', 'Scaling', '/Workspace/Users/abhishek.iyer@latentviewo365.onmicrosoft.com/MLOps and feature engineering/Feature_Engineering/Scaling');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE ds_training_1.mlops.dataset_column_transformation (
# MAGIC   column_id INT,
# MAGIC   transformation_id INT,
# MAGIC   sequence_id INT,
# MAGIC   input_paramter STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC Insert into ds_training_1.mlops.dataset_column_transformation (column_id, transformation_id, sequence_id, input_paramter) VALUES (1,4,1,1)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE ds_training_1.mlops.transformation_audit (
# MAGIC   id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC   transformation_id INT,
# MAGIC   status STRING,
# MAGIC   timestamp TIMESTAMP
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO ds_training_1.mlops.transformation_audit(transformation_id, status, timestamp)
# MAGIC VALUES (1, 'In progress', CURRENT_TIMESTAMP())
