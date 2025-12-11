# Databricks notebook source
# MAGIC %run ./1_build_bronze

# COMMAND ----------

# SILVER LAYER MODELING (Mentor-Specified Columns)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

class BuildSilver:
    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()

    def clean_silver_tables(self):
        tables_to_drop = ["dim_patient", "fact_patient_encounter"]
        for table in tables_to_drop:
            self.spark.sql(f"DROP TABLE IF EXISTS {table}")
            try:
                from pyspark.dbutils import DBUtils
                dbutils = DBUtils(self.spark)
                dbutils.fs.rm(f"dbfs:/user/hive/warehouse/{table}", recurse=True)
            except Exception:
                print(f"⚠️ Skipping file delete for {table}")
        print("🧹 Silver layer tables cleaned.")

    def read_bronze(self):
        return self.spark.table("bronze_health")

    def build_silver_tables(self):
        df = self.read_bronze()

        # Create dim_patient with selected demographic fields
        dim_patient = df.select("patient_nbr", "age", "gender", "race").dropDuplicates(["patient_nbr"])
        dim_patient.write.format("delta").mode("overwrite").saveAsTable("dim_patient")

        # Select mentor-requested fields for the fact table
        fact_cols = [
            "encounter_id", "patient_nbr",
            "admission_type_id", "discharge_disposition_id", "admission_source_id",
            "time_in_hospital", "num_lab_procedures", "num_procedures", "num_medications",
            "number_outpatient", "number_emergency", "number_inpatient",
            "diag_1", "diag_2", "diag_3",
            "readmitted", "change", "diabetesMed", "insulin", "A1Cresult"
        ]

        fact_df = df.select([c for c in fact_cols if c in df.columns])
        fact_df.write.format("delta").mode("overwrite").saveAsTable("fact_patient_encounter")

        print("✅ Silver layer tables (dim_patient and fact_patient_encounter) created.")

    def run(self):
        self.clean_silver_tables()
        self.build_silver_tables()
if __name__ == "__main__":
    silver = BuildSilver()
    silver.run()





# COMMAND ----------

class TestSilverLayer:
    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()

    def run_tests(self):
        dim = self.spark.table("dim_patient")
        fact = self.spark.table("fact_patient_encounter")

        assert "patient_nbr" in dim.columns, "❌ Missing patient_nbr in dim_patient"
        assert dim.select("patient_nbr").distinct().count() == dim.count(), "❌ Duplicate patient_nbr in dim_patient"

        required_fact_cols = [
            "encounter_id", "patient_nbr",
            "admission_type_id", "discharge_disposition_id", "admission_source_id",
            "time_in_hospital", "num_lab_procedures", "num_procedures", "num_medications",
            "number_outpatient", "number_emergency", "number_inpatient",
            "diag_1", "diag_2", "diag_3",
            "readmitted", "change", "diabetesMed", "insulin", "A1Cresult"
        ]

        for col_name in required_fact_cols:
            assert col_name in fact.columns, f"❌ Missing column in fact table: {col_name}"

        print("✅ Silver layer tests passed.")


if __name__ == "__main__":
    TestSilverLayer().run_tests()

