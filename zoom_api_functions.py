import json
import datetime
import numpy as np
import pandas as pd
pd.set_option('display.max_columns', 500)
import time
from espressomaker import Espresso
from zoomus import ZoomClient

#Key and secret necessary for API access - omitted from git push
key = ''
secret = ''

#Zoomus creates a client for API requests, and can call requests as python methods on the client object
client = ZoomClient(key, secret)

def generate_user_list(make_df = True):
    user_data = json.loads(client.user.list().content)
    users = user_data['users']
    pages = user_data['page_count']
    
    
    while True:
        if 'next_page_token' not in user_data.keys():
            break
        if user_data['next_page_token'] == '':
            break
        next_page = user_data['next_page_token']
        user_data = json.loads(client.user.list(next_page_token = next_page).content)
        
        users.extend(user_data['users'])
        time.sleep(0.3)
        
#     for page in range(2, pages + 1):
#         next_page = json.loads(client.user.list(page_number = page).content)
#         users.extend(next_page['users'])

    
    if make_df == False:
        return users
    else:
        return pd.DataFrame(users)
    
def user_list():
    user_df = generate_user_list()
    user_list = user_df.id.unique().tolist()
    
    return user_list

def get_meetings(gen_list = True, user_df = None, end_date = datetime.date.today(), start_date = None,
                key = key, secret = secret):
    #Note: cannot search more than 30 days at a time - choose num_months and days = 30 instead. Also, cannot search
    #more than 6 months into the past
    
    #Build out the zoomclient object 
    client = ZoomClient(key, secret)
    count = 0
    fails = []
    
    #Generates the df of all users if not done already (expensive API use! or just generates unique ids if you
    #pass in an existing df)
    if gen_list == True:
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

                    client = ZoomClient(key, secret)

            meeting_list.append(temp)
            time.sleep(0.3)
        except KeyError:
            #This most likely means that the client access expired and needs to be refreshed

            client = ZoomClient(key, secret)

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
                #A keyerror means that the given user has no meetings available in their data for some reason
                count += 1
                fails.append(user)
                #I want to know how many fails occurred 
                continue

    
    ret_list = []
    for i in meeting_list:
        if len(i) > 0:
            for meeting in i:
                ret_list.append(meeting)
                
    ret_df = pd.DataFrame(ret_list)
    ret_df.rename(columns = {'id':'meeting_id'}, inplace = True)
    ret_df['epic_csn'] = ret_df.custom_keys.apply(lambda x: int(x[0]['value']) if not pd.isnull(x) else x)
    ret_df.drop(columns = ['uuid','topic', 
                          'type','source', 'custom_keys'], inplace = True)
            
    print(count)
    print(fails)
    return ret_df

def get_participants(meeting_df, return_json = False, key = key, secret = secret):
    #Next page values don't matter here - if they go beyond 4 or 5, they have to do with disconnects which
    #are already captured
    m_ids = meeting_df.meeting_id.unique().tolist()
    meet_data = []
    count = 0
    client = ZoomClient(key, secret)
    #meet_dict = {}
    
    for m_id in m_ids:
        
        #insert exception in case of expiration to reset client if necessary
        
        meet_dict = {'meeting_id':m_id}
        try:
            p_list = json.loads(client.metric.list_participants(meeting_id = str(m_id), 
                                                type = 'past').content)['participants']
        except KeyError:
            time.sleep(0.3)
            #Refresh authorization
            client = ZoomClient(key, secret)
            
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

        #This is done outside of this "inner" loop of participants, but would be done for each m_id in all m_ids
        meet_data.append(meet_dict)

        time.sleep(0.3)
    if return_json == True:
        ret_dict = {}
        for record in range(len(meet_data)):
            ret_dict[record] = meet_data[record]
        return ret_dict
        
    participant_df = pd.DataFrame(meet_data)
    participant_df.meeting_id = participant_df.meeting_id.apply(lambda x: int(x))
    ret_df = meeting_df.merge(participant_df, on = 'meeting_id')
    
    print(count)
    print(count / len(m_ids))
    return ret_df

def generate_disconnect_cols(df):
    leave_cols = [i for i in df.columns if 'leave_reason_' in i]
    
    #This has to be generalizable as I don't now in advance how many columns will exist
    for ind in range(len(leave_cols)):
        df[f'connection_failure_{ind}'] = df[leave_cols[ind]].apply(is_disconnected)
    
    df = df.drop(columns = leave_cols)
    
    return df

def is_disconnected(val):
    if pd.isnull(val):
        return np.NaN
    
    if 'disconnected' in val:
        return 1
    
    else:
        return 0
    
def test_simplifier(df, minim = 3, maxim = 45):
    drop_nums = []
    for num in range(minim, maxim):
        drop_nums.append(f'_{num}')
    
    
    ret_df = df.copy()
    
    drop_cols = []
    for col in ret_df.columns:
        for num in drop_nums:
            if num in col:
                drop_cols.append(col)
    
    ret_df.drop(columns = drop_cols, inplace = True)
    return ret_df

def get_qos_vals(m_id, key = key, secret = secret):#, df): 
    client = ZoomClient(key, secret)
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
        next_one = json.loads(client.metric.list_participants_qos(meeting_id = str(m_id), 
                                        type = 'past', next_page_token = qos_metrics['next_page_token']).content)
        
        all_participants.append(next_one['participants'][0]['user_qos'])
        while True:
            if 'next_page_token' not in next_one.keys():
                break
            if next_one['next_page_token'] == '':
                break
            next_one = json.loads(client.metric.list_participants_qos(meeting_id = str(m_id),
                                        type = 'past', next_page_token = next_one['next_page_token']).content)
            all_participants.append(next_one['participants'][0]['user_qos'])
            
        
    except KeyError:
        pass
    
    users = {}
    for user in range(len(all_participants)):
        users[f'user_{user}'] = all_participants[user]
        
    
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

def generate_qos_from_m_id(m_id, durations_dict = None, secret = secret, key = key): 
    client = ZoomClient(key, secret)
    #Make sure to include the dictionary of meeting durations
    q_stats = {'meeting_id':m_id}
    if durations_dict != None:
        q_stats['duration'] = durations_dict[m_id]
    #This is now a dictionary of users, each pairing to a dictionary of metrics, each paired to a list of metrics
    users = get_qos_vals(m_id)
#     print(qos_vals.keys())
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

def qos_df(meeting_df, secret = secret, key = key):
    m_ids = meeting_df.meeting_id.unique().tolist()
    m_ids = [str(i) for i in m_ids]
    
    ret_vals = []
    #This should be fine, as the token is called in internal functions
    #client = ZoomClient(key, secret)
    
    for meeting in m_ids:
        #qos_vals = get_qos_vals(meeting)
        try:
            q_stats = generate_qos_from_m_id(meeting)
            ret_vals.append(q_stats)
        except:
            continue
        
    m_df = pd.DataFrame(ret_vals)
    m_df.meeting_id = m_df.meeting_id.apply(lambda x: int(x))
    
    ret_df = meeting_df.merge(m_df, how = 'left', on = 'meeting_id')
    
    return pd.DataFrame(ret_df)

#Generates lists of columns to sort in host/attendee locator functions below
def get_cols(df):
    num_cols = []
    other_cols = []
    ints = set()
    for col in df.columns:
        try:
            int(col.split('_')[-1])
            if '_'.join(col.split('_')[:-1]) not in num_cols:
                num_cols.append('_'.join(col.split('_')[:-1]))
            ints.add(int(col.split('_')[-1]))
        except ValueError:
            other_cols.append(col)
    
    return num_cols, other_cols, ints

def host_locator(df):
    num_cols, other_cols, ints = get_cols(df)
    
    ret_df = df.copy()
    #Generates a host identifier feature for each unique user
    for num in ints:
        ret_df[f'id_{num}_is_host'] = ret_df.apply(lambda row: 'Yes' if row[f'id_{num}'] == row.host_id\
                                               else 'No', axis =1)
    
    #Reforming df.columns as a list, because "series" objects do not rearrange as easily as list objects do
    #This reorganizes the featuers so that n_is_host is next to the id_n feature
    all_cols = list(ret_df.columns)
    for num in range(len(ints)):
        if num != max(ints):
            for ind in range(len(all_cols)):
                if all_cols[ind] == f'id_{num}':
                    ind1 = ind
                if all_cols[ind] == f'id_{num}_is_host':
                    ind2 = ind
            test = all_cols[:ind1 + 1] + [all_cols[ind2]] + all_cols[ind1 +1:ind2] + all_cols[ind2 + 1:]
            all_cols = test
        if num == max(ints):
            for ind in range(len(all_cols)):
                if all_cols[ind] == f'id_{num}':
                    ind1 = ind
                if all_cols[ind] == f'id_{num}_is_host':
                    ind2 = ind
            test = all_cols[:ind1 + 1] + [all_cols[ind2]] + all_cols[ind1 +1:ind2]
            all_cols = test
    
    ret_df = ret_df[all_cols]
    
    return ret_df

def host_audio_finder(row):
    row_hosts = []
    row_atts = []
    cols = list(row.index)
    proportions = [i for i in cols if 'poor_meeting' in i]
    for ids in host_checks:
        if row[ids] == 'Yes':
            row_hosts.append(ids.split('_')[1])
        if row[ids] == 'No':
            row_atts.append(ids.split('_')[1])
    audio = [i for i in proportions if 'audio' in i and i.split('_')[-1] in row_hosts and 'input' in i]
    to_sum = [row[i] for i in audio if np.isnan(row[i]) == False]
    
    if sum(to_sum) > 0:
        ret_val = 'Yes'
    else:
        ret_val = 'No'
    return ret_val

def host_video_finder(row):
    row_hosts = []
    row_atts = []
    cols = list(row.index)
    proportions = [i for i in cols if 'poor_meeting' in i]
    for ids in host_checks:
        if row[ids] == 'Yes':
            row_hosts.append(ids.split('_')[1])
    video = [i for i in proportions if 'video' in i and i.split('_')[-1] in row_hosts and 'input' in i]
    to_sum = [row[i] for i in video if np.isnan(row[i]) == False]
    
    if sum(to_sum) > 0:
        ret_val = 'Yes'
    else:
        ret_val = 'No'
    return ret_val

def attendee_audio_finder(row):
    row_atts = []
    cols = list(row.index)
    proportions = [i for i in cols if 'poor_meeting' in i]
    for ids in host_checks:
        if row[ids] == 'No':
            row_atts.append(ids.split('_')[1])
    audio = [i for i in proportions if 'audio' in i and i.split('_')[-1] in row_atts and 'input' in i]
    to_sum = [row[i] for i in audio if np.isnan(row[i]) == False]
    
    if sum(to_sum) > 0:
        ret_val = 'Yes'
    else:
        ret_val = 'No'
    return ret_val

def attendee_video_finder(row):
    row_atts = []
    cols = list(row.index)
    proportions = [i for i in cols if 'poor_meeting' in i]
    for ids in host_checks:
        if row[ids] == 'No':
            row_atts.append(ids.split('_')[1])
    video = [i for i in proportions if 'video' in i and i.split('_')[-1]  in row_atts and 'input' in i]
    to_sum = [row[i] for i in video if np.isnan(row[i]) == False]
    
    if sum(to_sum) > 0:
        ret_val = 'Yes'
    else:
        ret_val = 'No'
    return ret_val

#This function assembles above functions into the dataset
def generate_metric_issue_features(df):
    ret_df = df.copy()
    
    ret_df['host_audio_issues'] = df.apply(host_audio_finder, axis = 1)
    ret_df['host_video_issues'] = df.apply(host_video_finder, axis = 1)
    ret_df['attendee_audio_issues'] = df.apply(attendee_audio_finder, axis = 1)
    ret_df['attendee_video_issues'] = df.apply(attendee_video_finder, axis = 1)
    
    ret_df = analysis(ret_df)
    
    return ret_df

def generate_df_features(in_df):
    df = in_df.copy()
    cols = df.columns
    proportions = [i for i in cols if 'poor_meeting' in i]
    disconnects = [i for i in cols if 'connection_failure' in i]
    video = [i for i in proportions if 'video' in i]
    audio = [i for i in proportions if 'audio' in i]
    
    df = generate_metric_issue_features(df)

    disc = df[disconnects]
    
    disc['disconnections'] = disc.apply(lambda row: row.sum(), axis = 1)
    
    df['disconnections'] = disc['disconnections']
    
    vids = df[video]
    vids['video_issues'] = vids.apply(lambda row: 'Yes' if row.sum() > 1 else 'No', axis = 1)
    df['video_issues'] = vids['video_issues']
    
    vids.drop(columns = 'video_issues', inplace = True)
    vids['severe_video_issues'] = vids.apply(lambda row: 'Yes' if row.sum() > 4.0 else 'No', axis = 1)
    df['severe_video_issues'] = vids['severe_video_issues']
    
    auds = df[audio]
    auds['audio_issues'] = auds.apply(lambda row:'Yes' if row.sum() > 1 else 'No', axis = 1)
    df['audio_issues'] = auds['audio_issues']
    auds.drop(columns = 'audio_issues', inplace = True)
    auds['severe_audio_issues'] = auds.apply(lambda row:'Yes' if row.sum() > 4.0 else 'No', axis = 1)
    df['severe_audio_issues'] = auds['severe_audio_issues']
    
    #First 10 cols are constant cols
    display = [i for i in df.columns[:10]]
    display.extend(['disconnections', 'audio_issues', 'severe_audio_issues','host_audio_issues',
                    'attendee_audio_issues','video_issues', 'severe_video_issues', 'host_video_issues',
                   'attendee_video_issues'])
    #versions = [i for i in df.columns if 'version' in i]

    the_rest = [i for i in df.columns if i not in display]# and i not in versions]
    
    #df = df[display + versions + the_rest]
    df = df[display + the_rest]
    
    return df