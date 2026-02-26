import sys
import json
import boto3
import logging
import time
import pandas as pd


from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from datetime import date
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame



## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'TempDir'])
required_args = {'env', 'job_type'}                      
if '--WORKFLOW_RUN_ID' in sys.argv:
    workflow_args = getResolvedOptions(sys.argv, ['WORKFLOW_NAME', 'WORKFLOW_RUN_ID'])
    workflow_name = workflow_args['WORKFLOW_NAME']
    workflow_run_id = workflow_args['WORKFLOW_RUN_ID']
    
    glue_client = boto3.client("glue")
    workflow_params = glue_client.get_workflow_run_properties(Name=workflow_name, RunId=workflow_run_id)["RunProperties"]

    missing_params = required_args - set(workflow_params.keys())
    custom_args = getResolvedOptions(sys.argv, missing_params)
    custom_args.update(workflow_params)
else:
    custom_args = getResolvedOptions(sys.argv, required_args)     

env = custom_args['env']
job_type = custom_args['job_type'].lower()

## Spark connection
sc = SparkContext()
sql_context= SQLContext(sc)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## CONSTANTS ##
etljobName = args['JOB_NAME']


today_date= date.today()
#today_date = "2021-11-10" date format to pass 
print("today is :",today_date,type(today_date))

# Connect to s3 bucket-
s3 = boto3.resource("s3")
s3_bucket = s3.Bucket("s3-use1-ap-da-products-insights-datalake-p")
s3_c = boto3.client('s3')
input_bucket_name = "s3-use1-ap-da-products-insights-datalake-p"


def read_csv():

    df = sql_context.read.format('csv').\
         option('header','true').\
         option('maxColumns','17').\
         option('escape','"').\
         load(f"s3://s3-use1-ap-da-products-insights-datalake-p/product_scrap/books_scrap_report/{today_date}/*")
    df.printSchema()
    df.show(10)
    #csv_df=spark.read.option("header",True).csv(f"s3://s3-use1-ap-da-products-insights-datalake-p/product_scrap/books_scrap_report/{today_date}/*")
    #csv_df=spark.read.option("header",True).csv(f"s3://s3-use1-ap-da-products-insights-datalake-p/product_scrap/books_scrap_report/{'2021-11-10'}/*")
    renamed_df = df.select([F.col(col).alias(col.replace(' ', '_')) for col in df.columns])

    renamed_df.printSchema()
    renamed_df.show(10)
    #view creation and schema change
    renamed_df.createOrReplaceTempView("wiser_view")
    final_df = spark.sql('''SELECT BIGINT(id),title,format,website,vendor,INT(buy_box),INT(rental_availibility),INT(no_of_new_sellers),currency,\
                            FLOAT(current_price),FLOAT(standard_delivery),FLOAT(total_cost),FLOAT(was_price),url_scraped,date_stamp,time_stamp,\
                            INT(status),date('2021-11-11') as Inbound_date FROM wiser_view ''')
 
    final_df.printSchema()
    final_df.show(5)
   
    #stored parquet file in s3 location 
    #final_df.repartition(1).write.mode("append").option("compression","snappy").parquet("s3://s3-use1-ap-da-products-insights-datalake-p/product_scraped_data/books/scraped_processed_data/prod/scraped_inbound_data/")
    final_df.repartition(1).write.mode("append").option("compression","snappy").parquet("s3://s3-use1-ap-da-products-insights-datalake-p/product_scraped_data/books/scraped_processed_data/uat/scraped_inbound_data/")

def initialise():
   
    read_csv() 
    
initialise()
job.commit()  

