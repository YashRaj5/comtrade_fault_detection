# Databricks notebook source
# MAGIC %md
# MAGIC # Configuration

# COMMAND ----------

# DBTITLE 1,Instantiate config variable
if 'config' not in locals().keys():
    config = {}

# COMMAND ----------

# DBTITLE 1,Database
config['database'] = 'comtrade'

# create database if not exists
_ = spark.sql('create database if not exists {0}'.format(config['database']))

# set current database context
_ = spark.catalog.setCurrentDatabase(config['database'])

# COMMAND ----------

# DBTITLE 1,File Paths
config['temp_point'] = '/tmp/comtrade'
config['source_path'] = "s3://db-gtm-industry-solutions/data/rcg/comtrade/source/"
config['input_path'] = config['temp_point'] + '/input'
config['ouput_path'] = config['temp_point'] + '/output'
dbutils.fs.mkdirs(config['input_path'])
dbutils.fs.mkdirs(config['ouput_path'])

# COMMAND ----------

# DBTITLE 1,Model
config['model_name'] = 'fault_detection'

# COMMAND ----------

# DBTITLE 1,mlflow experiment
import mlflow
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
mlflow.set_experiment('/Users/{}/comtrade'.format(username))
