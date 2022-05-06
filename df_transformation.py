import datetime
import numpy as np
import pandas as pd
pd.set_option('display.max_columns', 500)

#Note: This script is ONLY necessary when preparing df for upload to microstrategy. There are no api calls or json files 
# created in this script. 

def is_disconnected(val):
    '''
    This is designed to be a functioned in df.apply() in function below. The "connection_failure" features return a string
    from zoom api response. This parses the string to search for the phrase "disconnected," and returns a 1 if present or 
    0 if not.
    '''
    if pd.isnull(val):
        return np.NaN
    
    if 'disconnected' in val:
        return 1
    
    else:
        return 0

def generate_disconnect_cols(df):
    '''
    Zoom api data creates a simple leave reason for each user. These include "ended by host," "user left," and "disconnected."
    This function simply creates a new feature for each leave reason called "is_disconnected" that gives a 1 or 0 value.
    As number of users is flexible, , feature 'leave_reason_0' to 'leave_reason_n' may be created, so this function is
    adaptable to however many users the api returns data for in a given meeting. 
    '''
    leave_cols = [i for i in df.columns if 'leave_reason_' in i]
    
    for ind in range(len(leave_cols)):
        df[f'connection_failure_{ind}'] = df[leave_cols[ind]].apply(is_disconnected)
    
    df = df.drop(columns = leave_cols)
    
    return df
    
def test_simplifier(df, minim = 3, maxim = 6):
    '''
    Function that manually shortens the width of df when created a demo, for ease of readability in demo showing.
    Each signed in participant responses range from 0 to n, depending on number of users in a meeting. This artificially
    constricts the output df to [maxim] users.
    '''
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


#Generates lists of columns to sort in host/attendee locator functions below
def get_cols(df):
    '''
    Some features from zoom api data requests are static, giving same number of features each time (here called "other cols")
    while others are flexible in how many features the api returns (called num cols, depends on number of participants/minutes
    in a zoom meeting). These range from 0 to n, where n is the max number of participants that were created in a particular
    zoom api request. ints is a quick count of how many sets of num_cols are in this dataset. This data allows other functions 
    to adapt their behavior to access the correct number of features if an api request returns more or fewer features 
    than a previous call did.
    '''
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
    '''
    Virtual visits include the host (physician) and other user attendees (generally patient/advocates/interpreters). 
    The first to sign in is designated a _0, then a 1, and so on. 0 is generally the host user, but not always. If a user
    signs out and then back in, they are designated a new feature _number. This function simply matches each user sign in's
    unique id and compares it to the virtual visit host id, to identify whether each user is the host or not.
    '''
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

def host_checks(df):
    '''
    This simply creates a list of all features in the df in which "is_host" is included in the feature name. This is 
    necessary to be called globally for the host/attendee audio/video finder functions to operate properly.
    This is because those functions take only a row as an argument rather than the entire df, to be used as .apply() 
    functions to the df for transformation.
    '''
    return [i for i in df.columns if 'is_host' in i]

def host_audio_finder(row):
    '''
    This feature is designed for ease of data analysis and aggregation. Identifies user numbers that are host, and creates 
    a 'Yes' if that host user has audio problems in input metrics (what zoom received from them), or a 'no' otherwise. 
    '''
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
    '''
    See host_audio_finder() above
    '''
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
    '''
    Same as functions above, except they identify when specifically non-host users have audio input issues for ease of 
    aggregating. This helps to find if only host or attendee had issues, if none had issues, or if both had issues. 
    '''
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
    '''
    Same as above, involving video issues.
    '''
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

def generate_metric_issue_features(df):
    '''
    This function takes the above 4 functions and alters a df taken as an argument by the actions of those functions.
    '''
    ret_df = df.copy()
    
    ret_df['host_audio_issues'] = df.apply(host_audio_finder, axis = 1)
    ret_df['host_video_issues'] = df.apply(host_video_finder, axis = 1)
    ret_df['attendee_audio_issues'] = df.apply(attendee_audio_finder, axis = 1)
    ret_df['attendee_video_issues'] = df.apply(attendee_video_finder, axis = 1)
    
    return ret_df

def generate_df_features(in_df):
    '''
    These final engineered features are designed based on requests for a method to quickly identify issues in virtual visits.
    For example, if ANY audio features display issues, then "audio_issues" triggers a "yes." If 4 or more audio features
    display issues, then 'severe_audio_issues' likewise triggers a "yes." These can work in conjunction with the host and
    attendee finders to quickly identify various groups of meetings based on their metric qualities. Finally, this organizes
    the df columns in a readable way when the intention is to read the dataframe directly rather than creating a dossier in 
    Microstrategy.
    '''
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