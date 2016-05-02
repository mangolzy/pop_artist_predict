import pandas as pd
import time
import numpy as np
from datetime import datetime

# input data as dataframe with no header

songs = pd.read_csv("../raw_data/mars_tianchi_songs.csv",header=None)
users = pd.read_csv("../raw_data/mars_tianchi_user_actions.csv",header=None)

# changing the timestamp form
def timeStampToDate(timeStamp):
    timeString = time.localtime(int(timeStamp))
    dateTime = time.strftime("%Y-%m-%d %H:%M:%S", timeString)
    return dateTime

# change TimestampToDate for all the rows
def allTimeStampToDate():
    users['action_date'] = users['action_timestamp'].map(lambda x : timeStampToDate(x))

# changing the timestamp form
def recordTimeToDate(recordtime):
    timeString = str(recordtime)
    dateTime = datetime.strptime(timeString,'%Y%m%d')
    return dateTime
# changing the recordtime string to date form
def allRecordtimeToDate():
    users['record_date'] = users['record_time'].map(lambda x : recordTimeToDate(x))

# renaming the header
def renameHeader():
    songs.columns = ['song_id','artist_id','publish_time','init_playtime','lang','gender']
    users.columns = ['user_id','song_id','action_timestamp','action_type','record_time']



# merge two tables 
#def mergeFile():
    
# temp [song_id, artist_id, publish_time, init_playtime, user_id, action_type, record_date]

# accumulate the statistics by artist and output to 50 files 
def seperateByArtistSave():
    artist_heats = pd.value_counts(mergeOnSong['artist_id'].values)
    count = 1
    for artist in artist_heats.index:
        temp = mergeOnSong[mergeOnSong['artist_id']==artist]
        path = '../dealed_data/artist_'+str(count)+'_records.csv'
        #path = '../dealed_data/'+artist+'_records.csv'
        temp.to_csv(path,na_rep='0.0')
        count = count + 1

# retain timeline for all artist
def accumulateActionByArtistSave():
    artist_heats = pd.value_counts(mergeOnSong['artist_id'].values)
    for artist in artist_heats.index:
        temp = mergeOnSong[mergeOnSong['artist_id']==artist]
        gbydate = temp.groupby('record_date', as_index=False)['action_type'].sum()
        path = '../output/'+artist+'_timeline.csv'
        gbydate.to_csv(path, na_rep = '0.0')


if __name__ == '__main__':
    renameHeader()
    allTimeStampToDate()
    allRecordtimeToDate()
    # select columns that we concern
    songs_df = pd.concat([songs['song_id'], songs['artist_id'], songs['publish_time'], songs['init_playtime']],axis = 1, keys=['song_id','artist_id','publish_time','init_playtime'])
    users_df = pd.concat([users['user_id'], users['song_id'], users['action_type'],users['record_date']], axis=1, keys = ['user_id','song_id','action_type','record_date'])
    mergeOnSong = pd.merge(songs_df,users_df, how='left', on='song_id')
    # accumulate by artist
    # seperateByArtistSave()
    accumulateActionByArtistSave()

    


