import json
import datetime
import numpy as np
import pandas as pd
pd.set_option('display.max_columns', 500)
import time
from espressomaker import Espresso
from zoomus import ZoomClient

#Notes: -json objects should be able to have a common id of meeting_id for storage
    #   - date objects will be needed to pass into get_meetings start and end dates

#Notes: lines 126 - 185, 
# all need to be reviewed after testing response. Rather than
#repetitive try/except uses, check response when authentication is expired, and use an if
#statement instead to save repetetive scripts. 

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
is_json = False

def authenticate(test = is_test):
    '''
    Generates a python object called "client" that interacts with the zoom api
    to make requests for the BCH outpatient account. If test == True, authentication will
    instead occur with a test account in order to assess that output aligns with 
    expectations. 
    '''
    if test:
        key = test_key
        secret = test_secret
        client = ZoomClient(key, secret)
        return client
    key = prod_key
    secret = prod_secret
    client = ZoomClient(key, secret)
    return client


client = authenticate()

def query_date():
    '''
    Placeholder for now - this function will query existing data, identify the correct start_time based on most recently
    acquired data, then return the following day as start_time, and 3 days following that as the end_time for functions below.
    Should return a tuple (start_time, end_time) so that they can be simply called as start_date, end_date = query_date()
    '''
    pass

def generate_user_list(make_df = True):
    '''
    Returns data on all users in the zoom account. Due to number of users on account,
    a while loop runs to gather all users until the data return does not contain
    a next_page_token.
    '''
    user_data = json.loads(client.user.list().content)
    users = user_data['users']
    pages = user_data['page_count']
    
    
    while True:
        #checks if current page is not the final page, if so, continues api requests
        #Both a lack of the key, or an empty response can occur if the return data
        #is the final page
        if 'next_page_token' not in user_data.keys():
            break
        if user_data['next_page_token'] == '':
            break
        next_page = user_data['next_page_token']
        user_data = json.loads(client.user.list(next_page_token = next_page).content)
        
        users.extend(user_data['users'])
        #Prevents possibility of hitting api rate limits per minute
        time.sleep(0.3)
        

    if make_df == False:
        return users
    else:
        return pd.DataFrame(users)
    
def user_list():
    user_df = generate_user_list()
    user_list = user_df.id.unique().tolist()
    
    return user_list

def get_meetings(gen_list = True, user_df = None, end_date = datetime.date.today(), 
                            start_date = None, is_test = is_test, return_json = False):
    '''
    This function cycles through each unique user_id from user_list(). For each id, it
    searches over a given date span and returns a dataframe object of each zoom meeting
    associated with that user_id over the time period. Default time period is current day
    to 30 days prior to current day. Note: you can only search a maximum of 30 days in one
    search, or the api will default back to 30 days. It is only possible to search a 
    maximum of 6 months into the past.

    If gen_list == True, this will first run user_list(), or, you can
    pass an existing user_df in if already created to save additional api calls.

    If return_json == True, returns the data as a list of json objects. If false, it returns
    a dataframe object. Defaulting to false as this is all relational and other functions depend on dataframe changes
    '''
    
    #Build out the zoomclient object 
    client = authenticate(test = is_test)
    count = 0
    fails = []
    

    if gen_list:
        all_user_ids = user_list()
    else:
        all_user_ids = user_df.id.unique().tolist()
    
    
    if start_date == None:
        delta = datetime.timedelta(days = 1)
        start_date = end_date - delta
    #This will house all users' meeting lists as dictionary objects
    meeting_list = []
    

    for user in all_user_ids:
        try:
            data = json.loads(client.report.get_user_report(
                user_id = user, start_time = start_date, 
                end_time = end_date).content)

            temp = data['meetings']
            while True:
                if 'next_page_token' not in data.keys():
                    break
                if data['next_page_token'] == '':
                    break
                try:
                    next_page = data['next_page_token']
                    data = json.loads(client.report.get_user_report(
                        user_id = user, start_time =start_date, 
                        end_time = end_date, next_page_token = next_page).content)
                    temp.extend(data['meetings'])
                    time.sleep(0.3)
                except KeyError:
            #This most likely means that the client access expired and needs to be refreshed

                    client = authenticate(test = is_test)

            meeting_list.append(temp)
            time.sleep(0.3)
        except KeyError:
            #This most likely means that the client access expired and needs to be refreshed

            client = authenticate(test = is_test)

            try:
                data = json.loads(client.report.get_user_report(
                    user_id = user, start_time = start_date, 
                    end_time = end_date).content)
                temp = data['meetings']
                while True:
                    if 'next_page_token' not in data.keys():
                        break
                    if data['next_page_token'] == '':
                        break
                    try:
                        next_page = data['next_page_token']
                        data = json.loads(client.report.get_user_report(
                            user_id = user, start_time = start_date, 
                            end_time = end_date, next_page_token = next_page).content)
                        temp.extend(data['meetings'])
                        time.sleep(0.3)
                    except KeyError:
                        break

                    meeting_list.append(temp)
                    time.sleep(0.3)

            except KeyError:
                #A keyerror means that the given user has no meetings 
                # available in their data for some reason
                count += 1
                fails.append(user)
                #I want to know how many fails occurred 
                continue

    
    ret_list = []
    for i in meeting_list:
        #This removes empty data where user had no meetings
        if len(i) > 0:
            for meeting in i:
                ret_list.append(meeting)
    if return_json == True:
        return ret_list

    ret_df = pd.DataFrame(ret_list)
    ret_df.rename(columns = {'id':'meeting_id'}, inplace = True)
    ret_df['epic_csn'] = ret_df.custom_keys.apply(
        lambda x: int(x[0]['value']) if not pd.isnull(x) else x)
    #These columns do not contain data useful for analysis and/or contain only 1 value
    ret_df.drop(columns = [
                        'uuid',
                        'topic', 
                        'type',
                        'source', 
                        'custom_keys'], inplace = True)
            
    #print(count)
    #print(fails)
    return ret_df

def get_participants(meeting_df = None, return_json = is_json, is_test = is_test):
    '''
    This function gathers unique meeting ids from previous functions and makes zoom
    api requests for each meeting id. If return_json == True, it will return a list of
    json objects with meeting_id is the unique identifier. If false, it will return the data
    as a dataframe object. 
    If meeting_df is not passed, this funciton will run get_meetings() to obtain it. The 
    dataframe object returned will be a merge of meeting_df and the dataframe obtained in this
    function. 
    '''
    if not meeting_df:
        meeting_df = get_meetings()
    m_ids = meeting_df.meeting_id.unique().tolist()
    meet_data = []
    #count is merely a test object to indicate if the api has failed to pull data
    count = 0
    client = authenticate(test = is_test)
    #meet_dict = {}
    
    for m_id in m_ids:
        #Create the meeting_id key so that the data can be located if desired
        meet_dict = {'meeting_id':m_id}
        try:
            p_list = json.loads(client.metric.list_participants(meeting_id = str(m_id), 
                                                type = 'past').content)['participants']
        except KeyError:
            time.sleep(0.3)
            #Refresh authorization
            client = authenticate(test = is_test)
            
            try:
                p_list = json.loads(client.metric.list_participants(meeting_id = str(m_id), 
                                            type = 'past').content)['participants']
            except KeyError:
                meet_data.append(meet_dict)
                count += 1
                continue
        #Define p_list and p_qos once outside per m_id, to reduce server calls

        for ind in range(len(p_list)):
            #this internal loop will grab all participants in that m_id
            #meet_dict[f'participant_{ind + 1}'] = p_list[ind]['user_name']
            for key in p_list[ind].keys():
                meet_dict[f'{key}_{ind}'] = p_list[ind][key]

        #This is done outside of this "inner" loop of participants, 
        # but would be done for each m_id in all m_ids
        meet_data.append(meet_dict)

        time.sleep(0.3)
    if return_json == True:
        #return a list of all json objects for non-relational storage
        ret_dict = {}
        for record in range(len(meet_data)):
            ret_dict[record] = meet_data[record]
        return ret_dict

    #Otherwise, make a dataframe object    
    participant_df = pd.DataFrame(meet_data)
    participant_df.meeting_id = participant_df.meeting_id.apply(lambda x: int(x))
    ret_df = meeting_df.merge(participant_df, on = 'meeting_id')
    
    #print(count)
    #print(count / len(m_ids))
    return ret_df

def convert_json_particpants_to_csv(data, base_df):
    '''
    Nothing special, simply makes the above json into a df by doing the same thing as the bottom. Will edit the above function
    to simply flow into this later on
    '''
    ret_df = pd.DataFrame(data)
    participant_df.meeting_id = participant_df.meeting_id.apply(lambda x: int(x))
    return_df = base_df.merge(participant_df, on = 'meeting_id')
    return return_df

def get_qos_vals(m_id, is_test = is_test):#, df): 
    client = authenticate(test = is_test)
    #This will store each list of metrics, to be paired to users, and finally paired to meetings later
    all_participants = []
    
    #This may be a wrapped function - in which case, we can pass q_qos in later from an "outer" function
    
    qos_metrics = json.loads(client.metric.list_participants_qos(
            meeting_id = str(m_id), type = 'past').content)#['participants'][0]['user_qos']
    
    #p_qos = qos_metrics['participants'][0]['user_qos']
    
    all_participants.append(qos_metrics['participants'][0]['user_qos'])
    try:
        metrics = [i for i in all_participants[0][0].keys()]# if 'cpu_usage' not in i]
        metrics.remove('date_time')
    except:
        print(f'{m_id} throws an error')
        return
    
    
    try:
        next_one = json.loads(
                client.metric.list_participants_qos(meeting_id = str(m_id), 
                type = 'past', next_page_token = qos_metrics['next_page_token']).content)
        
        all_participants.append(next_one['participants'][0]['user_qos'])
        while True:
            if 'next_page_token' not in next_one.keys():
                break
            if next_one['next_page_token'] == '':
                break
            next_one = json.loads(
                        client.metric.list_participants_qos(meeting_id = str(m_id),
                        type = 'past', next_page_token = next_one['next_page_token']).content)
            all_participants.append(next_one['participants'][0]['user_qos'])
            
        
    except KeyError:
        #Zoom's api rarely seems to not have stored data for a meeting. This results in a
        #keyerror even if the meeting_id happens to be valid. Rare but script breaking!
        pass
    
    users = {}
    for user in range(len(all_participants)):
        users[f'user_{user}'] = all_participants[user]
        
    
    #To ask Chris: I toyed with making this a new function, but it seems this is still
    #objectively better for readability regardless in the format this transforms to
    for p_qos in users.keys():
        #This will assemble all minutes, then replace the current users data
        qos_vals = {}
        for ind in range(len(users[p_qos])):
            for i in metrics:
            #    qos_vals.append(p_qos[0][i].keys())
                if ind == 0:
                    for q in users[p_qos][ind][i]:

                        if users[p_qos][ind][i][q] == '':
                            qos_vals[f'{i}_{q}'] = []

                        else:
                            qos_vals[f'{i}_{q}'] = [users[p_qos][ind][i][q]]
                else:
                    for q in users[p_qos][ind][i]:

                        if users[p_qos][ind][i][q] == '':
                            continue
                            #qos_vals[f'{i}_{q}'].append(np.NaN)

                        else:
                            try:
                                qos_vals[f'{i}_{q}'].append(users[p_qos][ind][i][q])
                            except KeyError:
                                #print(f'error in {m_id}')
                                continue
        users[p_qos] = qos_vals

    return users

def generate_qos_from_m_id(m_id, durations_dict = None, is_test = is_test, users = None): 
    client = authenticate(test = is_test)
    '''
    note: this should run only when making dataframe object, otherwise return qos_vals
    '''
    #Make sure to include the dictionary of meeting durations
    q_stats = {'meeting_id':m_id}
    if durations_dict != None:
        q_stats['duration'] = durations_dict[m_id]
    #This is now a dictionary of users, each pairing to a dictionary of metrics, each paired to a list of metrics
    if not users:
        users = get_qos_vals(m_id)
    pass_cols = ['cpu_usage_zoom_min_cpu_usage', 'cpu_usage_zoom_avg_cpu_usage', 
             'cpu_usage_zoom_max_cpu_usage', 'cpu_usage_system_max_cpu_usage']
    
    #nums = [ind.split('_')[1] for ind in users.keys()]
    #This needs to now generate {x} in addition, for numbers of users 0 - x.
    for qos_vals in users.keys():
        num = qos_vals.split('_')[1]
        for key in users[qos_vals].keys():
            count = 0
            if key in pass_cols:
                 continue

            if users[qos_vals][key] == []:
                if 'resolution' in key:
                    q_stats[f'{key}_{num}'] = np.NaN
                else:
                    q_stats[f'{key}_bad_mins_{num}'] = np.NaN
                    q_stats[f'{key}_bad_mins_{num}'] = np.NaN
                    q_stats[f'{key}_bad_mins_{num}'] = np.NaN  

                continue

            if 'resolution' in key:
                q_stats[f'{key}_{num}'] = users[qos_vals][key][0]
                continue

            if '%' in users[qos_vals][key][0]:
                #create a separate for comp and avg/max losses
                #loss should be <= 2%, so unnaceptable may be over 4%
                v_list = [float(q[:-1]) for q in users[qos_vals][key]]
                tag = '%'

                if '_loss' in key:
                    for val in v_list:
                        if val > 4.0:
                            count += 1
                    q_stats[f'{key}_min_{num}'] = np.min(v_list)
                    q_stats[f'{key}_avg_performance_{num}'] = np.round(np.mean(v_list), 2)
                    q_stats[f'{key}_max_{num}'] = np.max(v_list)
                    q_stats[f'{key}_bad_mins_{num}'] = int(count)
                    q_stats[f'{key}_bad_ratio_{num}'] = np.round((count / len(v_list)), 2)

                    if q_stats[f'{key}_bad_ratio_{num}'] > 0.4:
                        q_stats[f'{key}_poor_meeting_{num}'] = 1

                    if q_stats[f'{key}_bad_ratio_{num}'] <= 0.4:
                        q_stats[f'{key}_poor_meeting_{num}'] = 0

                if 'cpu' in key and 'max' in key:
                    if val > 70:
                        count +=1
                    q_stats[f'{key}_min_{num}'] = np.min(v_list)
                    q_stats[f'{key}_avg_performance_{num}'] = np.round(np.mean(v_list), 2)
                    q_stats[f'{key}_max_{num}'] = np.max(v_list)
                    q_stats[f'{key}_bad_mins_{num}'] = int(count)
                    q_stats[f'{key}_bad_ratio_{num}'] = np.round((count / len(v_list)), 2)

                    if q_stats[f'{key}_bad_ratio_{num}'] > 0.4:
                        q_stats[f'{key}_poor_meeting_{num}'] = 1

                    if q_stats[f'{key}_bad_ratio_{num}'] <= 0.4:
                        q_stats[f'{key}_poor_meeting_{num}'] = 0

                if 'cpu' in key and 'max' not in key:
                    for val in v_list:
                        if val > 25:
                            count +=1
                    q_stats[f'{key}_min_{num}'] = np.min(v_list)
                    q_stats[f'{key}_avg_performance_{num}'] = np.round(np.mean(v_list), 2)
                    q_stats[f'{key}_max_{num}'] = np.max(v_list)
                    q_stats[f'{key}_bad_mins_{num}'] = int(count)
                    q_stats[f'{key}_bad_ratio_{num}'] = np.round((count / len(v_list)), 2)

                    if q_stats[f'{key}_bad_ratio_{num}'] > 0.4:
                        q_stats[f'{key}_poor_meeting_{num}'] = 1

                    if q_stats[f'{key}_bad_ratio_{num}'] <= 0.4:
                        q_stats[f'{key}_poor_meeting_{num}'] = 0

                continue



            else:
                v_list = [int(q.split()[0]) for q in users[qos_vals][key]]
                tag = users[qos_vals][key][0].split()[1]
    #             print(f'{key}:{v_list}')

                if 'audio' in key and 'bitrate' in key:
                    #60-100 kbps is optimal, so under 40 could be "unacceptable"
                    for val in v_list:
                        if val < 40:
                            count += 1
                    q_stats[f'{key}_min_{num}'] = np.min(v_list)
                    q_stats[f'{key}_avg_performance_{num}'] = np.round(np.mean(v_list), 2)
                    q_stats[f'{key}_max_{num}'] = np.max(v_list)
                    q_stats[f'{key}_bad_mins_{num}'] = int(count)
                    q_stats[f'{key}_bad_ratio_{num}'] = np.round((count / len(v_list)), 2)

                    if q_stats[f'{key}_bad_ratio_{num}'] > 0.4:
                        q_stats[f'{key}_poor_meeting_{num}'] = 1

                    if q_stats[f'{key}_bad_ratio_{num}'] <= 0.4:
                        q_stats[f'{key}_poor_meeting_{num}'] = 0                


                if 'video' in key and 'bitrate' in key:
                    #600 is recommended, so under 400 is unacceptable
                    for val in v_list:
                        if val < 400:
                            count += 1
                    q_stats[f'{key}_min_{num}'] = np.min(v_list)
                    q_stats[f'{key}_avg_performance_{num}'] = np.round(np.mean(v_list), 2)
                    q_stats[f'{key}_max_{num}'] = np.max(v_list)
                    q_stats[f'{key}_bad_mins_{num}'] = int(count)
                    q_stats[f'{key}_bad_ratio_{num}'] = np.round((count / len(v_list)), 2)

                    if q_stats[f'{key}_bad_ratio_{num}'] > 0.4:
                        q_stats[f'{key}_poor_meeting_{num}'] = 1

                    if q_stats[f'{key}_bad_ratio_{num}'] <= 0.4:
                        q_stats[f'{key}_poor_meeting_{num}'] = 0                

                if 'latency' in key:
                    #latency should be under 150 ms for both audio and video
                    for val in v_list:
                        if val > 100:
                            count += 1
                    q_stats[f'{key}_min_{num}'] = np.min(v_list)
                    q_stats[f'{key}_avg_performance_{num}'] = np.round(np.mean(v_list), 2)
                    q_stats[f'{key}_max_{num}'] = np.max(v_list)
                    q_stats[f'{key}_bad_mins_{num}'] = int(count)
                    q_stats[f'{key}_bad_ratio_{num}'] = np.round((count / len(v_list)), 2)

                    if q_stats[f'{key}_bad_ratio_{num}'] > 0.4:
                        q_stats[f'{key}_poor_meeting_{num}'] = 1

                    if q_stats[f'{key}_bad_ratio_{num}'] <= 0.4:
                        q_stats[f'{key}_poor_meeting_{num}'] = 0                

                if 'jitter' in key:
                    #jitter should be under 40 ms for both audio and video
                    for val in v_list:
                        if val > 60:
                            count += 1
                    q_stats[f'{key}_min_{num}'] = np.min(v_list)
                    q_stats[f'{key}_avg_performance_{num}'] = np.round(np.mean(v_list), 2)
                    q_stats[f'{key}_max_{num}'] = np.max(v_list)
                    q_stats[f'{key}_bad_mins_{num}'] = int(count)
                    q_stats[f'{key}_bad_ratio_{num}'] = np.round((count / len(v_list)), 2)

                    if q_stats[f'{key}_bad_ratio_{num}'] >= 0.4:
                        q_stats[f'{key}_poor_meeting_{num}'] = 1

                    if q_stats[f'{key}_bad_ratio_{num}'] < 0.4:
                        q_stats[f'{key}_poor_meeting_{num}'] = 0                

                if 'framerate' in key:
                    #30 is max, less than 15 is not acceptable
                    for val in v_list:
                        if val < 18:
                            count += 1
                    q_stats[f'{key}_min_{num}'] = np.min(v_list)
                    q_stats[f'{key}_avg_performance_{num}'] = np.round(np.mean(v_list), 2)
                    q_stats[f'{key}_max_{num}'] = np.max(v_list)
                    q_stats[f'{key}_bad_mins_{num}'] = int(count)
                    q_stats[f'{key}_bad_ratio_{num}'] = np.round((count / len(v_list)), 2)

                    if q_stats[f'{key}_bad_ratio_{num}'] > 0.4:
                        q_stats[f'{key}_poor_meeting_{num}'] = 1

                    if q_stats[f'{key}_bad_ratio_{num}'] <= 0.4:
                        q_stats[f'{key}_poor_meeting_{num}'] = 0                
                

    return q_stats

def qos_data(meeting_df, return_json = is_json, is_test = is_test):
    m_ids = meeting_df.meeting_id.unique().tolist()
    m_ids = [str(i) for i in m_ids]
    
    ret_vals = []

    if return_json:
        #Generate the json data and return a list of all meetings qos vals
        for meeting in m_ids:
            try:
                qos_json = get_qos_vals(meeting)
                if 'meeting_id' not in qos_json.keys():
                    #I can't remember and will need to test if it already exists
                    qos_json['meeting_id'] = meeting
            except:
                continue

            ret_vals.append(qos_json)
   

        return ret_vals  
    #If return_json = False, make the datafram object for microstrategy storage
    for meeting in m_ids:

        #To bring up to Chris - inexplicably, this call sometimes (rarely) fails.
        #Re running the same m_id later often works at another time. No idea why. 
        try:
            q_stats = generate_qos_from_m_id(meeting)
            ret_vals.append(q_stats)
        except:
            #When the unknown error occurs, I am skipping for now. This needs to be cleaned
            continue



    m_df = pd.DataFrame(ret_vals)
    m_df.meeting_id = m_df.meeting_id.apply(lambda x: int(x))
    
    ret_df = meeting_df.merge(m_df, how = 'left', on = 'meeting_id')
    
    return pd.DataFrame(ret_df)

def convert_json_qos_to_df(json_users, meeting_df):
    '''
    Takes json data and preps for merging to primary df object, so as not to duplicate api calls. Used only when json data
    has already been created and stored in raw format. Should move to the other function collection for transforming.
    Steps: - query AWS for qos data that matches the user_ids in meeting df
           - query the csv in untransformed data directory
           - use this function to convert raw qos data and merge into untransformed df on meeting_id

    '''
    m_ids = meeting_df.meeting_id.unique().tolist()
    m_ids = [str(i) for i in m_ids]
    
    ret_vals = []
    for meeting in m_ids:
        try:
            #This SHOULD prevent the need to call the api again
            q_stats = generate_qos_from_m_id(users = json_users['meeting_id'] == meeting)
            ret_vals.append(q_stats)
        except:
            continue
    m_df = pd.DataFrame(ret_vals)
    m_df.meeting_id = m_df.meeting_id.apply(lambda x: int(x))
    
    ret_df = meeting_df.merge(m_df, how = 'left', on = 'meeting_id')
    
    return pd.DataFrame(ret_df)
        
#Make two last functions. One calls these functions to create multiple json objects to save 
# in an S3 bucket. The other creates the dataframe object to also be saved in S3 but to 
#later incorporate into microstrategy