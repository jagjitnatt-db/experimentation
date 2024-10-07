-- Databricks notebook source
CREATE OR REFRESH STREAMING TABLE merge_dpp
(
  id INT,
  name STRING,
  age INT,
  dt_part DATE,
  last_updated TIMESTAMP
)
PARTITIONED BY(dt_part);

APPLY CHANGES INTO
  live.merge_dpp
FROM
  stream(jagjitnatt.dlt_demo.merge_dpp_src)
KEYS
  (`id`, dt_part)
SEQUENCE BY
  last_updated
STORED AS SCD TYPE 1

-- COMMAND ----------


