import pandas as pd
import time
import numpy as np

# input data as dataframe with no header

songs = pd.read_csv("../raw_data/mars_tianchi_songs.csv",header=None)
users = pd.read_csv("../raw_data/mars_tianchi_user_actions.csv",header=None)

# changing the timestamp form
def timeStampToDate(timeStamp):
    timeString = time.localtime(int(timeStamp))
    dateTime = time.strftime("%Y-%m-%d %H:%M:%S", timeString)
    return dateTime

# renaming the header
def renameHeader():
    songs.columns = ['song_id','artist_id','publish_time','init_playtime','lang','gender']
    users.columns = ['user_id','song_id','action_timestamp','action_type','record_time']

# change TimestampToDate for all the rows
def allTimeStampToDate():
    users['action_date'] = users['action_timestamp'].map(lambda x : timeStampToDate(x))

# saving intermediate files
def saveFile():
    songs.to_csv('../dealed_data/songs.csv',index=False)
    users.to_csv('../dealed_data/users.csv',index=False)


if __name__ == '__main__':
    renameHeader()
    allTimeStampToDate()
    saveFile()
    


