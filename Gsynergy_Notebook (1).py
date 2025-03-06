# Databricks notebook source
# DBTITLE 1,JDBC Connection

jdbc_url = "jdbc:sqlserver://gsynergyserver.database.windows.net:1433;database=Gsynergy_Database"
jdbc_properties = {
    "user": "gsynergyadmin",
    "password": "Shra26051998@",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}
 

# COMMAND ----------

# DBTITLE 1,Creating Views of SQL Tables

table_names = [
    "[stg].[fact_averagecosts]",
    "[stg].[fact_transactions]",
    "[stg].[hier_clnd]", 
    "[stg].[hier_hldy]",
    "[stg].[hier_invloc]",
    "[stg].[hier_invstatus]",
    "[stg].[hier_possite]",
    "[stg].[hier_pricestate]",
    "[stg].[hier_prod]", 
    "[stg].[hier_rtlloc]"
]

dfs = {}

for table in table_names:
    df = spark.read.jdbc(url=jdbc_url, table=table, properties=jdbc_properties)
    

    dfs[table] = df
    
    temp_view_name = table.replace(".", "_").replace("[", "").replace("]", "")

    df.createOrReplaceTempView(temp_view_name)
    
    print(f"Loaded and created view: {temp_view_name}")



# COMMAND ----------

# DBTITLE 1,CHECK For NULL
# MAGIC %sql
# MAGIC SELECT 
# MAGIC     CASE WHEN COUNT(*) = COUNT(order_id) THEN 'NOT NULL' ELSE 'NULL' END AS order_id_check,
# MAGIC     CASE WHEN COUNT(*) = COUNT(line_id) THEN 'NOT NULL' ELSE 'NULL' END AS line_id_check,
# MAGIC     CASE WHEN COUNT(*) = COUNT(sku_id) THEN 'NOT NULL' ELSE 'NULL' END AS sku_id_check,
# MAGIC     CASE WHEN COUNT(*) = COUNT(fscldt_id) THEN 'NOT NULL' ELSE 'NULL' END AS fscldt_id_check
# MAGIC FROM stg_fact_transactions;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Uniqueness Check with Unique
# MAGIC %sql
# MAGIC SELECT 
# MAGIC     CASE WHEN MAX(cnt) = 1 THEN 'UNIQUE' ELSE 'DUPLICATE FOUND' END AS primary_key_check
# MAGIC FROM (
# MAGIC     SELECT order_id, line_id, COUNT(*) AS cnt
# MAGIC     FROM stg_fact_transactions
# MAGIC     GROUP BY order_id, line_id
# MAGIC ) t;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC - > # **Foreign Key Constraints**
# MAGIC Check whether all transactions have valid SKU IDs in stg_hier_prod

# COMMAND ----------

# DBTITLE 1,Foreign Key Constraints
# MAGIC %sql
# MAGIC SELECT 
# MAGIC     CASE WHEN COUNT(*) = 0 THEN 'NOT NULL' ELSE 'NULL' END AS sku_fk_check
# MAGIC FROM stg_fact_transactions t
# MAGIC LEFT JOIN stg_hier_prod p ON t.sku_id = p.sku_id
# MAGIC WHERE p.sku_id IS NULL;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Check whether all transactions have valid pos_site_id in stg_hier_possite

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     CASE WHEN COUNT(*) = 0 THEN 'NOT NULL' ELSE 'NULL' END AS pos_site_fk_check
# MAGIC FROM stg_fact_transactions t
# MAGIC LEFT JOIN stg_hier_possite p ON t.pos_site_id = p.site_id
# MAGIC WHERE p.site_id IS NULL;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Normalize Hierarchy Tables
# MAGIC We create separate tables for each level of the hierarchy.
# MAGIC Example: pos_site stores site-related details, while category, subcategory, style, and sku break down the product hierarchy.
# MAGIC This structure avoids data duplication and makes querying easier.

# COMMAND ----------

# DBTITLE 1,Normalize Staging Schema
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS staging.pos_site AS 
# MAGIC SELECT DISTINCT site_id, site_label, subchnl_id, subchnl_label, chnl_id, chnl_label 
# MAGIC FROM stg_hier_possite;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Create Staged Fact Table
# MAGIC The fact_transactions table is filtered to ensure pos_site_id and sku_id exist in the normalized hierarchy tables.
# MAGIC This helps maintain referential integrity, preventing mismatched or missing data.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS staging.category AS 
# MAGIC SELECT DISTINCT cat_id, cat_label FROM stg_hier_prod;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS staging.subcategory AS 
# MAGIC SELECT DISTINCT subcat_id, subcat_label, cat_id FROM stg_hier_prod;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS staging.style AS 
# MAGIC SELECT DISTINCT styl_id, styl_label, subcat_id FROM stg_hier_prod;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS staging.sku AS 
# MAGIC SELECT DISTINCT sku_id, sku_label, stylclr_id, stylclr_label, styl_id, issvc, isasmbly, isnfs 
# MAGIC FROM stg_hier_prod;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Check for Orphaned Records (Optional but recommended)
# MAGIC Before inserting data, we check for missing pos_site_id and sku_id in the fact_transactions table.
# MAGIC This helps identify data issues early, ensuring consistency between the fact and dimension tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS staging.fact_transactions AS 
# MAGIC SELECT * FROM stg_fact_transactions 
# MAGIC WHERE pos_site_id IN (SELECT site_id FROM staging.pos_site) 
# MAGIC AND sku_id IN (SELECT sku_id FROM staging.sku);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from staging.fact_transactions

# COMMAND ----------

# MAGIC %md
# MAGIC # mview_weekly_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 1: Ensure the schema exists
# MAGIC CREATE SCHEMA IF NOT EXISTS refined;
# MAGIC
# MAGIC -- Step 2: Create the table in the schema
# MAGIC CREATE OR REPLACE TABLE refined.mview_weekly_sales (
# MAGIC     pos_site_id BIGINT,
# MAGIC     sku_id BIGINT,
# MAGIC     fsclwk_id BIGINT,
# MAGIC     price_substate_id BIGINT,
# MAGIC     type STRING,
# MAGIC     total_sales_units DOUBLE,
# MAGIC     total_sales_dollars DOUBLE,
# MAGIC     total_discount_dollars DOUBLE
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Step 3: Insert Aggregated Data
# MAGIC INSERT INTO refined.mview_weekly_sales
# MAGIC SELECT 
# MAGIC     TRY_CAST(pos_site_id AS BIGINT) AS pos_site_id,
# MAGIC     TRY_CAST(sku_id AS BIGINT) AS sku_id,
# MAGIC     WEEKOFYEAR(fscldt_id) AS fsclwk_id,  -- Convert date to fiscal week ID
# MAGIC     TRY_CAST(price_substate_id AS BIGINT) AS price_substate_id,
# MAGIC     type,
# MAGIC     SUM(sales_units) AS total_sales_units,
# MAGIC     SUM(sales_dollars) AS total_sales_dollars,
# MAGIC     SUM(discount_dollars) AS total_discount_dollars
# MAGIC FROM stg_fact_transactions
# MAGIC GROUP BY pos_site_id, sku_id, fsclwk_id, price_substate_id, type;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from refined.mview_weekly_sales

# COMMAND ----------

from pyspark.sql import SparkSession

# ✅ Read Data from Databricks Delta Table
final = spark.sql("SELECT * FROM refined.mview_weekly_sales")


# Write Data to SQL Server Table
final.write.jdbc(url=jdbc_url, table="dbo.mview_weekly_sales", mode="append", properties=jdbc_properties)
 
print("Data Successfully Pushed to SQL Server!")