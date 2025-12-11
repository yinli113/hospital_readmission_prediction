# Databricks notebook source
# MAGIC %run ./3_build_gold

# COMMAND ----------

# MAGIC %md
# MAGIC **![Average Time in Hospital by Age Group](path)**

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# By age group
age_in_hospital = spark.sql("""
SELECT age, ROUND(AVG(time_in_hospital), 2) AS avg_stay
FROM gold_patient_features
GROUP BY age
ORDER BY avg_stay
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC **![Average Time in Hospital by Diagnosis](path)**

# COMMAND ----------

# By primary diagnosis (top 10 frequent)
diagnosis_in_hospital = spark.sql("""
SELECT diag_1, COUNT(*) AS total, ROUND(AVG(time_in_hospital), 2) AS avg_stay
FROM gold_patient_features
GROUP BY diag_1
ORDER BY avg_stay DESC
LIMIT 10
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC **![ Diagnoses with High Readmission Rates](path)**

# COMMAND ----------

# Diagnoses with High Readmission Rates
spark.sql("""
SELECT diag_1, COUNT(*) AS total_cases,
       SUM(readmission_flag) AS readmissions,
       ROUND(SUM(readmission_flag) / COUNT(*), 3) AS readmission_rate
FROM gold_patient_features
GROUP BY diag_1
HAVING COUNT(*) > 100  -- filter out low volume diagnoses
ORDER BY readmission_rate DESC
LIMIT 10
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC **![Relationship Between Insulin / A1C and Readmission](path)**

# COMMAND ----------

# A1Cresult vs readmission
spark.sql("""
SELECT A1Cresult, COUNT(*) AS total,
       SUM(readmission_flag) AS readmissions,
       ROUND(SUM(readmission_flag) / COUNT(*), 3) AS readmission_rate
FROM gold_patient_features
GROUP BY A1Cresult
ORDER BY readmission_rate
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC **![Relationship Between Insulin / A1C and Readmission](path)**

# COMMAND ----------

# Insulin vs readmission
spark.sql("""
SELECT insulin, COUNT(*) AS total,
       SUM(readmission_flag) AS readmissions,
       ROUND(SUM(readmission_flag) / COUNT(*), 3) AS readmission_rate
FROM gold_patient_features
GROUP BY insulin
ORDER BY readmission_rate
""").show()

