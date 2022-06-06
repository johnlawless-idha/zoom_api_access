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
df_with_metrics = dt.convert_json_qos_to_df(raw_qos_data, untransformed_df)

#creates disconnect counts
df_with_disconnects = dt.generate_disconnect_cols(df_with_metrics)

#Creates "user_n is host" features
df_with_host_ids = dt.host_locator(df_with_disconnects)

#These generate specific features to identify meetings in which the host and/or attendee have audio or video issues
df_with_located_issues = dt.generate_metric_issue_features(df_with_host_ids)
#This generates the remaining engineered features, including general audio/severe audio and video/severe video issuess
df_final = dt.generate_df_features(df_with_located_issues)

#Finally, upload new df to s3 in prep for migrating to BCH 360. Will make a new subdirectory for this
resp = dm.upload_to_s3(df_final, 'zoom_data_df_transformed', 'zoom_api/transformed_dataframes/', is_json = False)

#Confirm upload was successful
dm.response_check(resp)