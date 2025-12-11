# Databricks notebook source
from pyspark.sql import SparkSession
import os

class BuildBronze:
    def __init__(self, raw_path = "data/diabetic_data.csv", table_name="bronze_health", infer_schema=True, header=True):
        self.spark = SparkSession.builder.getOrCreate()
        self.raw_path = raw_path
        self.table_name = table_name
        self.infer_schema = infer_schema
        self.header = header

    

    def clean_bronze_tables(self):
        """
        Deletes the Bronze Delta table metadata and data.
        """
        self.spark.sql(f"DROP TABLE IF EXISTS {self.table_name}")
        path = f"dbfs:/user/hive/warehouse/{self.table_name}"
        # dbutils is only available in Databricks. 
        # For local runs, we might want to skip or handle differently.
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(self.spark)
            dbutils.fs.rm(path, recurse=True)
            print("🧹 Bronze table, data cleaned.")
        except ImportError:
            print(f"⚠️ dbutils not available. Skipping DBFS cleanup for {path}")
            # If local, maybe clean up local warehouse dir?
            pass


    def load_csv(self):
        # Resolve absolute path if needed, assuming running from project root
        file_path = os.path.abspath(self.raw_path)
        print(f"Loading data from: {file_path}")
        
        df = (
            self.spark.read
            .option("header", str(self.header).lower())
            .option("inferSchema", str(self.infer_schema).lower())
            .csv(file_path)
        )
        return df

    def write_bronze_table(self, df, mode="overwrite"):
        # You can partition by a column like 'admission_date' if available
        (
            df.write
            .format("delta")
            .mode(mode)
            .saveAsTable(self.table_name)
        )
        print(f"✅ Bronze table `{self.table_name}` created.")

    def run(self):
        self.clean_bronze_tables()
        df = self.load_csv()
        df.printSchema()
        self.write_bronze_table(df)

if __name__ == "__main__":
    # Point to the data folder in the project structure
    builder = BuildBronze("data/diabetic_data.csv")
    builder.run()

