#Import functions from these files - ensure that path for import is maintined if these files are moved!
import data_migration as dm
import zoom_api_data_extration as ext
import boto3
import pandas as pd
import json

#This file illustrates the expected workflow for collecting data via the api and storing to AWS S3.
#Refer to those files for any functions

#Key and secret necessary for API access - omitted from git push
#Credentials for test account
test_key = ''
test_secret = ''

#Credentials for main account
prod_key = ''
prod_secret = ''

#This boolean will determine all authenticate calls in the script
is_test = False
#This one will return json data if True, or dataframe data if False
is_json = True

#Authenticates client object to interact with zoom api
client = ext.authenticate()

#Creates a json object linking each zoom account user id to all meeting ids under their account over a date range. Defaults
#currently to datetime.today() as end date and today - 1 for start date (will need to edit this). Also gets participant data
#for those meetings

#Very small base dataframe object - store in base_df directory
meeting_df = ext.get_meetings()
#json data - store in a particiants directory
all_meeting_participants_json = ext.get_participants(meeting_df = meeting_df)

#Creating a larger df object that will eventually be stored in a backup S3, flagged to migrate to microstrategy
df_mid_merge = ext.convert_json_participants_to_csv(all_meeting_participants_json, meeting_df)

#Should return raw json data to upload into raw_qos directory
raw_qos_data = ext.qos_data(meeting_df, return_json = True)

#Now save all data into S3. Talk with Chris about good practice for file naming when automating files
resp = dm.upload_to_s3(all_meeting_participants_json, 'meeting_participants_raw', prefix = 'zoom_api/participants_json/')

#General workflow should check that upload succeeded for each case
dm.response_check(resp)

resp = dm.upload_to_s3(df_mid_merge, 'untransformed_df', prefix = 'zoom_api/untransformed_dataframes/', is_json = False)
dm.response_check(resp)

resp = dm.upload_to_s3(raw_qos_data, 'meeting_metrics_raw', prefix = 'zoom_api/raw_metrics/')
dm.response_check(resp)