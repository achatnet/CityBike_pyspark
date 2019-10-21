# Databricks notebook source
"""
Created on 21/10/2019

@author: Djalel
"""

"""
Import packages
----------
"""
from pyspark.sql.functions import col, asc
from pyspark.sql.types import *




"""
Variables
----------
    
n_cluster: int
    user-defined number of clusters

account_name : string
    Storage account name

account_key: string 
    Storage account name
	
container_name: string
    Storage container name (bucket)
	
source_file_path: string
	Source file with the full path storage
"""



def read_source_file(account_name, account_key, container_name, source_file_path):
  """
  Read source file from azure container

  Parameters
  ----------
  account_name : string
      Storage account name

  account_key: string 
      Storage account name

  container_name: string
      Storage container name (bucket)

  source_file_path: string
      Source file with the full path storage

  Returns
  -------
  DataFrame 
  """
  spark.conf.set(
  "fs.azure.account.key." + account_name + ".blob.core.windows.net", 
  account_key)

  data = spark.read.option("multiline", "true").json("wasbs://"+ container_name + "@" + account_name + ".blob.core.windows.net/" + source_file_path)
  return data





def clean_data(data):
  """
  Clean data and return a dataframe cleaned

  Parameters
  ----------
  data : dataframe

  Returns
  -------
  DataFrame 
  """
  
  # Drop columns we don't need
  column_to_keep=["id","latitude","longitude"]
  data = data.select(column_to_keep)

  # Drop None and Null Values
  data = data.dropna()
  data = data.filter("id!=7")

  # Convert latitude and longitude to double
  from pyspark.sql import functions as F
  data = data.select("id",
                  F.col("latitude").cast(DoubleType()),
                  F.col("longitude").cast(DoubleType())
                  )

  return data



def vec_ass(data):
  """
  Assemble latitude and longitude columns in features column

  Parameters
  ----------
  data : dataframe

  Returns
  -------
  DataFrame 
  """

  from pyspark.ml.feature import VectorAssembler

  vecAssembler = VectorAssembler(inputCols=["latitude", "longitude"], outputCol="features")
  df = vecAssembler.transform(data)
  return df


  
  


def kmeans(df, nb_cluster=2, seed=1):
  """
  Applying KMeans Alg with nb_cluster and seed parameters defined by the user

  Parameters
  ----------
  df : DataFrame

  nb_cluster : int
      number of clusters, default value is 2

  seed: int 
      seed parameter, default value is 1

  Returns
  -------
  DataFrame with clusters info column
  """

  from pyspark.ml.clustering import KMeans
  kmeans = KMeans(k=nb_cluster, seed=seed)  
  model = kmeans.fit(df.select('features'))


  transformed = model.transform(df)
  return transformed






"""
Initialize Variables
----------
"""
account_name = <account_name>
account_key = <account_key>
container_name = <container_name> 
source_file_path = <container_name> 


output_file_path = source_file_path.replace("input/","output/").replace(".json",".csv")

# =============================================================================
# main function
# Perform a clustering based on either the location or characteristics of 
# bike stations.
# =============================================================================

data = read_source_file(account_name, account_key, container_name, source_file_path)

data_clean = clean_data(data)

data_vec = vec_ass(data_clean)


transformed = kmeans(df, nb_cluster=2, seed=1)

transformed.select(["id","latitude","longitude"]).write.csv("wasbs://"+ container_name + "@" + account_name + ".blob.core.windows.net/" + output_file_path)


# COMMAND ----------

transformed.show()

# COMMAND ----------


