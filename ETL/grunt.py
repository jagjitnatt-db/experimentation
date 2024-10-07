# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

def merge_into_table(table_name:str, source_dataframe:DataFrame, keys:list, values:list = ["*"], except_columns:list = None, optional_when_matched_condition:str = "1=1", optional_when_not_matched_condition:str = "1=1", optional_when_not_matched_by_source_condition:str = "1=1", schema_evolution:bool = False, delete_missing_records:bool = False, soft_delete:bool = False, soft_delete_column:str = "isDeleted"):
  source_dataframe.createOrReplaceTempView(f"{table_name}_src")

  merge_keys = " AND ".join([f"src.{key} = tgt.{key}" for key in keys])

  if "*" not in values:
    tgt_columns = ", ".join(values)
    src_columns = ", ".join([f"src.{value}" for value in values])
    insert_columns = f"({tgt_columns}) VALUES ({src_columns})"
    update_columns = ", ".join([f"tgt.{value} = src.{value}" for value in values])
  else:
    insert_columns = "*"
    update_columns = "*"
  
  if delete_missing_records:
    if soft_delete:
      delete_strategy = f"UPDATE SET {soft_delete_column} = 'Y'"
    else:
      delete_strategy = "DELETE"
    when_not_matched_by_source = f"WHEN NOT MATCHED BY SOURCE AND {optional_when_not_matched_by_source_condition} THEN {delete_strategy}"
  else:
    when_not_matched_by_source = ""

  if schema_evolution:
    schema_evolution_text = "WITH SCHEMA EVOLUTION"
  else:
    schema_evolution_text = ""
  merge_statement = f"""
  MERGE {schema_evolution_text} INTO {table_name} tgt 
  USING {table_name}_src src 
  ON {merge_keys}
  WHEN MATCHED AND {optional_when_matched_condition} THEN
  UPDATE SET {update_columns}
  WHEN NOT MATCHED BY TARGET AND {optional_when_not_matched_condition} THEN
  INSERT {insert_columns}
  {when_not_matched_by_source}
  """
  return merge_statement

# COMMAND ----------

def implement_scd_2(table_name:str, source_view:str, keys:list, order_by_columns:list, scd_columns:tuple = ("Effective_From_Date", "Effective_To_Date"), scd_column_source:str = "current_timestamp()", active_ind_column:str = "", snapshot_mode:bool = True, default_to_date:str='9999-12-31'):
  key_columns = ", ".join(keys)
  from_date_col = scd_columns[0]
  to_date_col = scd_columns[1]
  scd_columns = f"{scd_columns[0]}, {scd_columns[1]}"

  records_from_target = spark.sql(f"select * FROM {table_name}")
  if scd_column_source in scd_columns:
    records_from_source = spark.sql(f"select *, NULL as {to_date_col} from {source_view}")
  else:
    records_from_source = spark.sql(f"select *, {scd_column_source} as {from_date_col}, NULL as {to_date_col} from {source_view}")

  if records_from_target.schema[from_date_col].dataType.typeName() == "timestamp":
    previous_val = " - INTERVAL 1 SECOND"
  elif records_from_target.schema[from_date_col].dataType.typeName() == "date":
    previous_val = " - 1"
  else:
    previous_val = ""
  matched_records = records_from_target.join(records_from_source, on=keys, how="semi")
  combined_records = records_from_source.unionAll(matched_records)
  window = Window.partitionBy(keys).orderBy(order_by_columns)
  ordered_records = combined_records.withColumn(to_date_col, lead(from_date_col, 1, default_to_date).over(window)).withColumn(to_date_col, expr(f"{to_date_col}{previous_val}"))
  return ordered_records



# COMMAND ----------



# COMMAND ----------

df = spark.read.table("jagjitnatt.experimentation.hospitals")
display(df)

# COMMAND ----------

merge_statement = merge_into_table(table_name = "target_table", source_dataframe = df, keys = ["cms_certification_num"], values = ["*"], schema_evolution = True, optional_when_not_matched_condition = "last_updated > '2012-01-01 00:00:00'", delete_missing_records = True, soft_delete=True, soft_delete_column = "del_ind")

# COMMAND ----------

print(merge_statement)

# COMMAND ----------

# SQL code to create a table with SCD Type 2 and insert sample records
sql_query = """
CREATE  TABLE IF NOT EXISTS jagjitnatt.experimentation.employee_scd2 (
    employee_id INT,
    employee_name STRING,
    role STRING,
    salary DECIMAL(10,2),
    start_date DATE,
    end_date DATE
);"""

sql_query2 = """
INSERT INTO jagjitnatt.experimentation.employee_scd2 (employee_id, employee_name, role, salary, start_date, end_date)
SELECT 1, 'John Doe', 'Software Engineer', 70000.00, '2020-01-01', NULL UNION ALL
 SELECT   2, 'Jane Smith', 'Data Scientist', 80000.00, '2020-06-01', NULL UNION ALL
 SELECT   3, 'Mike Brown', 'Product Manager', 90000.00, '2021-01-01', NULL
"""

# Execute the SQL query
spark.sql(sql_query2)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM jagjitnatt.experimentation.employee_scd2

# COMMAND ----------

dummy_data = [
    (4, 'Alice Johnson', 'HR Manager', 75000.00, '2021-05-01', None),
    (5, 'Bob White', 'Marketing Specialist', 68000.00, '2021-07-15', None),
    (6, 'Charlie Green', 'Sales Associate', 62000.00, '2022-01-10', None)
]

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE  TABLE IF NOT EXISTS jagjitnatt.experimentation.employee_staging (
# MAGIC     employee_id INT,
# MAGIC     employee_name STRING,
# MAGIC     role STRING,
# MAGIC     salary DECIMAL(10,2),
# MAGIC     start_date DATE
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO jagjitnatt.experimentation.employee_staging (employee_id, employee_name, role, salary, start_date)
# MAGIC SELECT 4, 'Alice Johnson', 'HR Manager', 75000.00, '2021-05-01'

# COMMAND ----------

src = spark.read.table("jagjitnatt.experimentation.employee_staging")
src.createOrReplaceTempView("employee_staging")
display(src)

# COMMAND ----------

mid_df = implement_scd_2(table_name="jagjitnatt.experimentation.employee_scd2", source_view="employee_staging", keys=["employee_id"], order_by_columns=["start_date"], scd_columns=("start_date", "end_date"), scd_column_source="start_date")

# COMMAND ----------

mid_df.orderBy("employee_id").display()

# COMMAND ----------

mid_df.explain()

# COMMAND ----------


