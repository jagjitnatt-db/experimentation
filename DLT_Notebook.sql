-- Databricks notebook source
CREATE OR REFRESH LIVE TABLE gsk_lt
AS
SELECT * FROM jagjitnatt.experimentation.hospitals
WHERE 1=1;

-- COMMAND ----------


