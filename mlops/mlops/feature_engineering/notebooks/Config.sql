-- Databricks notebook source
SELECT * FROM ds_training_1.mlops.dataset

-- COMMAND ----------

SELECT * FROM ds_training_1.dataset.bike_sharing

-- COMMAND ----------

INSERT INTO ds_training_1.mlops.dataset (name, description, created_by, created_timestamp, location)  VALUES ("bikesharing", "Rental Bike Sharing Dataset", "abhishek.iyer@latentviewo365.onmicrosoft.com",current_timestamp(), "ds_training_1.dataset.bike_sharing");
INSERT INTO ds_training_1.mlops.dataset (name, description, created_by, created_timestamp, location)  VALUES ("visa", "Status of Visa approval", "abhishek.iyer@latentviewo365.onmicrosoft.com",current_timestamp(), "ds_training_1.dataset.visadataset");

-- COMMAND ----------

SELECT * FROM ds_training_1.mlops.dataset

-- COMMAND ----------

SELECT * FROM ds_training_1.dataset.privacy

-- COMMAND ----------

SELECT * FROM ds_training_1.mlops.dataset_column

-- COMMAND ----------

INSERT INTO ds_training_1.mlops.dataset_column (dataset_id, column_name) VALUES 
(5, "case_id"), 
(5, "continent"),
(5, "education_of_employee"),
(5, "has_job_experience"), 
(5, "requires_job_training"), 
(5, "no_of_employees"),
(5, "yr_of_estab"),
(5, "region_of_employment"),
(5, "prevailing_wage"),
(5, "unit_of_wage"),
(5, "full_time_position"),
(5, "case_status")

-- COMMAND ----------

SELECT * FROM ds_training_1.mlops.dataset_column

-- COMMAND ----------

SELECT * FROM ds_training_1.mlops.transformation

-- COMMAND ----------

INSERT INTO ds_training_1.mlops.transformation (name, notebook_name, notebook_path) VALUES 
("Standard Scaler", "StandardScaler", "/Workspace/Users/abhishek.iyer@latentviewo365.onmicrosoft.com/MLOps and feature engineering/Feature_Engineering/StandardScaler"),
("Min Max Scaler", "MinMaxScaler", "/Workspace/Users/abhishek.iyer@latentviewo365.onmicrosoft.com/MLOps and feature engineering/Feature_Engineering/MinMaxScaler"),
("Max Absolute Scaler", "MaxAbsScaler", "/Workspace/Users/abhishek.iyer@latentviewo365.onmicrosoft.com/MLOps and feature engineering/Feature_Engineering/MaxAbsScaler"),
("One Hot Encoding", "OneHotEncoding", "/Workspace/Users/abhishek.iyer@latentviewo365.onmicrosoft.com/MLOps and feature engineering/Feature_Engineering/OneHotEncoding"),
("Label Encoding", "String Indexer", "/Workspace/Users/abhishek.iyer@latentviewo365.onmicrosoft.com/MLOps and feature engineering/Feature_Engineering/StringIndexer")

-- COMMAND ----------

INSERT INTO ds_training_1.mlops.dataset_column_transformation (column_id, transformation_id, sequence_id, input_paramter) VALUES 
(25, 6, 10, null),
(18, 6, 10, null),
(30, 3, 20, null),
(31, 3, 20, null),
(34, 6, 10, null), 
(35, 6, 10, null),
(36, 6, 10, null),
(37, 6, 10, null),
(38, 2, 20, null),
(39, 6, 10, null),
(40, 6, 10, null),
(41, 2, 20, null),
(43, 6, 10, null),
(44, 6, 10, null),
(45, 6, 10, null)

-- COMMAND ----------

SELECT * FROM ds_training_1.mlops.dataset_column_transformation

-- COMMAND ----------

ALTER TABLE ds_training_1.mlops.dataset_column_transformation ADD COLUMNS (status STRING)


-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC # Step 1: Extract data from the table using Spark SQL for the past 7 days
-- MAGIC audit_df = spark.sql("""
-- MAGIC SELECT *
-- MAGIC FROM observe_ai.default.access_audit
-- MAGIC """)
-- MAGIC
-- MAGIC # Step 2: Convert to Pandas DataFrame for further analysis
-- MAGIC df = audit_df.toPandas()
-- MAGIC
-- MAGIC # Step 3: Convert 'time_of_access' to datetime format if it's not already
-- MAGIC df['time_of_access'] = pd.to_datetime(df['time_of_access'])
-- MAGIC
-- MAGIC # Step 4: Feature engineering - Compute the features needed for anomaly detection
-- MAGIC df['hour_of_day'] = df['time_of_access'].dt.hour  # Extract hour of the day
-- MAGIC
-- MAGIC # Aggregate data for the daily features (e.g., distinct action types, failure ratio)
-- MAGIC df_features = (
-- MAGIC     df.groupby(['user', 'date_of_access'])
-- MAGIC     .agg(
-- MAGIC         daily_access_count=('action', 'size'),  # Count of actions
-- MAGIC         distinct_action_types=('action_type', 'nunique'),  # Count of distinct action types
-- MAGIC         failure_ratio=('status', lambda x: (x != "200").mean() * 100),  # Failure ratio
-- MAGIC         unique_tables_accessed=('table', 'nunique'),  # Distinct tables accessed
-- MAGIC         avg_hour_of_access=('hour_of_day', 'mean'),  # Avg hour of access
-- MAGIC         count_403_errors=('status', lambda x: (x == '403').sum()),  # Count of 403 errors
-- MAGIC         count_404_errors=('status', lambda x: (x == '404').sum()),  # Count of 404 errors
-- MAGIC         count_500_errors=('status', lambda x: (x == '500').sum()),  # Count of 500 errors
-- MAGIC         weekend_access_count=('time_of_access', lambda x: x.dt.dayofweek.isin([5, 6]).sum())  # Weekend access count
-- MAGIC     )
-- MAGIC     .reset_index()
-- MAGIC )
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC csv_file_path = "/Volumes/ds_training_2/bronze/source/task_2/BikeSharingraw.csv"
-- MAGIC df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
-- MAGIC df.write.saveAsTable("ds_training_1.dataset.bike_sharing")

-- COMMAND ----------

SELECT * FROM ds_training_1.dataset.bike_sharing

-- COMMAND ----------

SELECT * FROM ds_training_1.mlops.dataset_column_transformation

-- COMMAND ----------

UPDATE ds_training_1.mlops.dataset_column_transformation SET status = 'not completed' WHERE column_id IN (25, 18, 30, 31) and transformation_id IN (6,3)

-- COMMAND ----------

SELECT * FROM ds_training_1.mlops.dataset_column_transformation

-- COMMAND ----------

SELECT * FROM ds_training_1.mlops.dataset

-- COMMAND ----------

INSERT INTO ds_training_1.mlops.dataset (name, description, created_by, created_timestamp, location)  VALUES ("carprice", "Features of the car and their respective prices", "abhishek.iyer@latentviewo365.onmicrosoft.com",current_timestamp(), "ds_training_1.dataset.car_price");

-- COMMAND ----------

SELECT * FROM ds_training_1.mlops.dataset

-- COMMAND ----------

SELECT * FROM ds_training_1.dataset.car_price

-- COMMAND ----------

INSERT INTO ds_training_1.mlops.dataset_column (dataset_id, column_name) VALUES 
(6, "car_ID"), 
(6, "symboling"),
(6, "CarName"),
(6, "fueltype"), 
(6, "aspiration"), 
(6, "doornumber"),
(6, "carbody"),
(6, "drivewheel"),
(6, "enginelocation"),
(6, "wheelbase"),
(6, "carlength"),
(6, "carwidth"),
(6, "carheight"),
(6, "curbweight"),
(6, "enginetype"),
(6, "cylindernumber"),
(6, "enginesize"),
(6, "fuelsystem"),
(6, "boreratio"),
(6, "stroke"),
(6, "compressionratio"),
(6, "horsepower"),
(6, "peakrpm"),
(6, "citympg"),
(6, "highwaympg"),
(6, "price")

-- COMMAND ----------

SELECT * FROM ds_training_1.mlops.transformation

-- COMMAND ----------

SELECT * FROM ds_training_1.mlops.dataset_column WHERE dataset_id = 6 ORDER BY id;

-- COMMAND ----------

SELECT * FROM ds_training_1.mlops.dataset_column_transformation


-- COMMAND ----------

INSERT INTO ds_training_1.mlops.dataset_column_transformation VALUES
(48, 6, 10, null, "not completed"),
(49, 6, 10, null, "not completed"),
(50, 6, 10, null, "not completed"),
(51, 6, 10, null, "not completed"),
(52, 6, 10, null, "not completed"),
(53, 6, 10, null, "not completed"),
(54, 6, 10, null, "not completed"),
(55, 2, 20, null, "not completed"),
(56, 2, 20, null, "not completed"),
(57, 2, 20, null, "not completed"),
(58, 2, 20, null, "not completed"),
(59, 2, 20, null, "not completed"),
(60, 6, 10, null, "not completed"),
(61, 6, 10, null, "not completed"),
(62, 2, 20, null, "not completed"),
(63, 6, 10, null, "not completed"),
(64, 2, 20, null, "not completed"),
(65, 2, 20, null, "not completed"),
(66, 2, 20, null, "not completed"),
(67, 2, 20, null, "not completed"),
(68, 2, 20, null, "not completed"),
(69, 2, 20, null, "not completed"),
(70, 2, 20, null, "not completed")

-- COMMAND ----------

ALTER TABLE ds_training_1.mlops.transformation_audit 
ADD COLUMNS (dataset_id int);

-- COMMAND ----------


