# Databricks notebook source
# MAGIC %run ./2_build_silver

# COMMAND ----------

# GOLD LAYER FEATURE ENGINEERING (Mentor-Specified Columns Only)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, expr
from pyspark.ml.feature import StringIndexer

class BuildGold:
    def __init__(self, gold_table):
        self.gold_table = gold_table
        self.spark = SparkSession.builder.getOrCreate()

    def clean_gold_tables(self):
        self.spark.sql(f"DROP TABLE IF EXISTS {self.gold_table}")
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(self.spark)
            dbutils.fs.rm(f"dbfs:/user/hive/warehouse/{self.gold_table}", recurse=True)
        except Exception:
            print(f"⚠️ Skipping file delete for {self.gold_table} (likely not in Databricks environment)")
        print("🧹 Gold table cleaned.")

    def transform_features(self, df):
        df = df.filter(col("diag_1").isNotNull())

        # Encode age
        age_buckets = [f"[{10*i}-{10*(i+1)})" for i in range(10)]
        for i, bucket in enumerate(age_buckets, start=1):
            df = df.withColumn("age", when(col("age") == bucket, lit(i)).otherwise(col("age")))
        df = df.withColumn("age", col("age").cast("int"))

        # Binary columns
        df = df.withColumn("gender", when(col("gender") == "Male", lit(1)).otherwise(lit(0)))
        df = df.withColumn("diabetesMed", when(col("diabetesMed") == "Yes", lit(1)).otherwise(lit(0)))
        df = df.withColumn("change", when(col("change") == "Ch", lit(1)).otherwise(lit(0)))

        # Insulin and lab results
        df = df.withColumn("insulin", when(col("insulin").isin("Steady", "Up", "Down"), lit(1)).when(col("insulin") == "No", lit(0)).otherwise(None))
        df = df.withColumn("A1Cresult", when(col("A1Cresult").isin(">7", ">8"), lit(1))
                                        .when(col("A1Cresult") == "Norm", lit(0))
                                        .when(col("A1Cresult").isNull() | (col("A1Cresult") == "None"), lit(-99))
                                        .otherwise(lit(-99)).cast("int"))

        # Add derived columns
        df = df.withColumn("total_visits", expr("number_outpatient + number_emergency + number_inpatient"))
        df = df.withColumn("readmission_flag", when(col("readmitted").isin("<30", ">30"), lit(1)).otherwise(lit(0)))
        
        return df

    def index_categoricals(self, df):
        index_fields = ["race", "diag_1", "diag_2", "diag_3"]
        for col_name in index_fields:
            if col_name in df.columns:
                indexer = StringIndexer(inputCol=col_name, outputCol=f"{col_name}_indexed", handleInvalid="keep")
                df = indexer.fit(df).transform(df)
        return df

    def write_gold(self, df):
        df.write.format("delta").mode("overwrite").partitionBy("age", "readmission_flag").option("overwriteSchema", "true").saveAsTable(self.gold_table)
        print(f"✅ Gold features table `{self.gold_table}` created.")

    def run(self):
        self.clean_gold_tables()
        fact_df = self.spark.table("fact_patient_encounter")
        dim_patient = self.spark.table("dim_patient")
        df = fact_df.join(dim_patient, on="patient_nbr", how="left")

        df = self.transform_features(df)
        df = self.index_categoricals(df)

        # Cache intermediate DataFrame to speed up repeated operations
        df = df.cache()
        self.write_gold(df)

if __name__ == "__main__":
    gold = BuildGold("gold_patient_features")
    gold.run()


# COMMAND ----------

class TestGoldFeatures:
    def __init__(self, gold_table):
        self.gold_table = gold_table
        self.spark = SparkSession.builder.getOrCreate()

    def run_tests(self):
        df = self.spark.table(self.gold_table)

        required_cols = [
            "age", "gender", "race_indexed",
            "admission_type_id", "discharge_disposition_id", "admission_source_id",
            "time_in_hospital", "num_lab_procedures", "num_procedures", "num_medications",
            "number_outpatient", "number_emergency", "number_inpatient",
            "diag_1_indexed", "diag_2_indexed", "diag_3_indexed",
             "readmission_flag", "total_visits", "change", "diabetesMed", "insulin", "A1Cresult"
        ]

        for col_name in required_cols:
            assert col_name in df.columns, f"❌ Missing column: {col_name}"

        assert df.filter(col("diag_1").isNull()).count() == 0, "❌ Nulls found in diag_1"
        print("✅ Gold layer tests passed.")


if __name__ == "__main__":
    TestGoldFeatures("gold_patient_features").run_tests()

