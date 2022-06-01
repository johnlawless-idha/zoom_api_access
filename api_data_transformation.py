import data_migration as dm
import df_transformation as dt
import boto3
import pandas as pd
import json

#api_data_collection collects a partially transformed df with participant data as well as raw metric data. This workflow
#should collect those from aws, transform them according to df_transformation, and import completed df to s3 backup and 
#microstrategy

s3 = boto3.client('s3')
bucket = 'lake-idha-analytics-nonprod'

prefix = 'zoom_api/'
is_json = False

#Key chosen from method of saving from api_data_collection workflow
#Partially transformed df
untransformed_df = dm.retrieve_s3_to_csv(Bucket = bucket, Key = 'zoom_api/untransformed_dataframes/untransformed_df.csv')
#Raw meeting metrics
raw_qos_data = dm.retrieve_s3_to_json(Bucket = bucket, Key = 'zoom_api/raw_metrics/meeting_metrics_raw.json')
#Created transformed metrics, all further transformations need to be done from here
df_with_metrics = dt.qos_data(untransformed_df,users = raw_qos_data)
