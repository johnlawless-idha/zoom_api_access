import pandas as pd
#StringIO is necessary to re-assemble csv files if imported
from io import StringIO
import json
import boto3

######NOTES FOR FURTHER WORK: May need to generate a further algorithm to properly automate the naming/path designation 
# of saved files once that stage has been reached 

#Global definitions that will be used in all functions
s3 = boto3.client('s3')
bucket = 'lake-idha-analytics-nonprod'

prefix = 'zoom_api/'

def get_s3_keys(bucket = bucket, key = None):
    '''
    This function will access the relevant S3 bucket and directory, then retrieves keys to find relevant key
    for import function to retrieve proper file. Enter desired file as a key and this function will return the 
    path to the file. If unknown, run without keys and it will display all keys in the zoom_api directory in S3.
    '''
    obj = s3.list_objects(Bucket=bucket, Prefix = prefix)
    key_list = [key['Key'] for key in obj['Contents']]
    
    #This should return a specific key for retrieve_s3_to_json() to operate for retrieval
    if not key:
        return key_list
    for item in key_list:
        if key in item:
            return item

def retrieve_s3_to_json(bucket, key):
    '''
    This function is designed to take an s3 json file and assign it to a python object. If file name is known (even
    partially), then the full key can be found by entering the name into get_s3_keys(). If not, running get_s3_keys()
    without any file name will return a list of all directories and files in the zoom_api directory.
    '''
    s3_file = s3.get_object(Bucket = bucket, Key = key)
    body = s3_file['Body']
    
    retrieved_file = json.loads(body.read())
    return retrieved_file

def retrieve_s3_to_csv(bucket, key):
    '''
    Retrieves a csv file as a key if it exists in S3. 
    '''
    obj = s3.get_object(Bucket = bucket, Key = key)
    body = obj['Body']
    #Data stored as raw text needs to be read in properly to convert via pandas
    csv_string = body.read().decode('utf-8')
    
    return pd.read_csv(StringIO(csv_string))

def upload_to_s3(data, filename, bucket = bucket, is_json = True):
    '''
    This function takes data that has completed transformation and migrates it to s3. If a json file, it will migrate
    to the zoom_api directory to be retrieved by subsequent functions in data table transformations. If json = false,
    it is assumed that it is instead creating a backup of data expected to 'live' in BCH360 and will migrate to a
    different directory. As this is an upload function, there is no output except for the http status. 
    
    TO UPDATE: This function currently stores all files in the same directory, but needs to be updated to
    differentiate migration locations once they have been properly determined. 

    Args: data - the file to be uploaded. Expecting a dictionary if is_json = True or a dataframe if False
          filename - the name you want the file saved as, minus the extension
          bucket - which s3 bucket to save to - global default is lake-idha-analytics-nonprod
          is_json - bool which determines how the file will be saved (via json.dumps() or StringIO() for a csv)
    '''
    if is_json == True:
        prefix = 'zoom_api/'
        return s3.put_object(Body = json.dumps(data),
                        Bucket = bucket, Key = f'{prefix}{filename}.json')['ResponseMetadata']['HTTPStatusCode']

    
    #Change this when a new directory for backup is needed
    
    prefix = 'zoom_api/'
    #Converts to string for upload, but maintains a csv format
    csv_buffer = StringIO()
    data.to_csv(csv_buffer)
    return s3.put_object(Body = csv_buffer.getvalue(), Bucket = bucket,
                         Key = f'{prefix}{filename}.csv')['ResponseMetadata']['HTTPStatusCode']