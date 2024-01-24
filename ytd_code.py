#***** YOUTUBE DATA HARVESTING AND WAREHOUSING USING MYSQL, MONGODB AND STREAMLIT *****#
#_______________________________________________________________________________________

# Import Necessary Packages
import pandas as pd
import pymongo
import streamlit as st
from streamlit_option_menu import option_menu
import time as sleep_time
import mysql
import mysql.connector
import pymysql
import re
import string
import emoji
import certifi
from googleapiclient.discovery import build
import json
from datetime import time
from datetime import timedelta
from dateutil.parser import parse
from datetime import datetime

#Fetching Channel Details
channel_id = ["UCDliHgjWiNDyVElTgqMJcKA", "UCJRQjSJA5zs3_H4Vki9zLVg", "UCfMe-ygpz4wzTD1lPo_6pYg"]
def get_channel_details(youtube, channel_id):

    request = youtube.channels().list(part="snippet,contentDetails,statistics,status", id=channel_id)
    response = request.execute()

    c_data = {
        "Channel_ID": response['items'][0]['id'],
        "Channel_Name": response['items'][0]['snippet']['title'],
        "Channel_Type": response['items'][0]['kind'],
        "Channel_Views": response['items'][0]['statistics']['viewCount'],
        "Channel_Description": response['items'][0]['snippet']['description'],
        "Channel_Subscription": response['items'][0]['statistics']['subscriberCount'],
        "Channel_video": response['items'][0]['statistics']['videoCount'],
        "playList_id": response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    }
    return c_data

#Defining YouTube API Key
def youtube_data_api():
    api_key = "AIzaSyAAhUtol-HtecKortNeOcg97zCOuYTGsT8"
    return build('youtube', 'v3', developerKey=api_key)

youtube = youtube_data_api()
channel_data = get_channel_details(youtube, 'UCDliHgjWiNDyVElTgqMJcKA')

#Fetching Video Ids
def videoId(youtube,playList_id):
    videoIds = []

    request = youtube.playlistItems().list(
        part="snippet,contentDetails",
        maxResults=25,
        playlistId=playList_id
    )
    response = request.execute()
    for items in response['items']:
        videoIds.append(items['contentDetails']['videoId'])
    return videoIds

#Collecting video Details
def video_details(youtube, videoIds):
    video_data = []
    for i in videoIds:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=i)
        response = request.execute()

        video_id = response['items'][0]['id']
        Channel_Name = response['items'][0]['snippet']['channelTitle']
        videoTitle = response['items'][0]['snippet']['title']
        videoDesc = response['items'][0]['snippet']['description']
        tags = ",".join(response['items'][0]['snippet'].get('tags',['NA']))
        published_At = response['items'][0]['snippet']['publishedAt']
        duration = response['items'][0]['contentDetails']['duration']
        viewCount = response['items'][0]['statistics']['viewCount']
        likeCount = response['items'][0]['statistics'].get('likeCount',0)
        favoriteCount = response['items'][0]['statistics']['favoriteCount']
        commentCount = response['items'][0]['statistics'].get('commentCount',0)
        caption = response['items'][0]['contentDetails']['caption']

        V_data = {"video_id": video_id,
                  "Channel_Name": Channel_Name,
                  "videoTitle": videoTitle,
                  "videoDesc": videoDesc,
                  "tags": tags,
                  "published_At": published_At,
                  "duration": duration,
                  "viewCount": viewCount,
                  "likeCount": likeCount,
                  "favoriteCount": favoriteCount,
                  "commentCount": commentCount,
                  "caption": caption}
        video_data.append(V_data)
    return video_data

#Collecting Comment details
def comment_details(youtube, video_ids):
    comments = []
    for k in video_ids:
        try:
            request = youtube.commentThreads().list(
                part="snippet,replies",
                videoId=k)
            response = request.execute()

            commentid = response['items'][0]['id']
            comment_author = response['items'][0]['snippet']['topLevelComment']['snippet']['authorDisplayName']
            publishedAt = response['items'][0]['snippet']['topLevelComment']['snippet']['publishedAt']
            comment_text = response['items'][0]['snippet']['topLevelComment']['snippet']['textDisplay']

            comment_data = {"commentid": commentid,
                            "comment_author": comment_author,
                            "publishedAt": publishedAt,
                            "comment_text": comment_text}
            comments.append(comment_data)
        except:
            comments.append({'commentid': k, 'comment_author': None, 'comment_text': None, 'publishedAt': None})
    return comments

# Defining MongoDB Connection
myclient = pymongo.MongoClient("mongodb+srv://BK_Indrani:indrani@cluster0.j7mih4h.mongodb.net/?retryWrites=true&w=majority",tlsCAFile=certifi.where())
my_db = myclient["Youtube"]
mycol = my_db["Youtube_test"]

def main(channel_id):
    channelData = get_channel_details(youtube, channel_id)
    playList_id = channelData['playList_id']
    videoID = videoId(youtube,playList_id)
    videoData = video_details(youtube, videoID)
    commentData = comment_details(youtube, videoID)
    data = {"channelData": channelData,
            "videoData": videoData,
            "commentsData": commentData
            }
    mycol.insert_one(data)
    return data


def is_date_or_time(value):
    if isinstance(value, (int, float)):
        return False

    try:
        parse(str(value))
        return True
    except (ValueError, OverflowError):
        return False

#Remove special Characters and emojis from data
def remove_special_characters(data):
    cleaned_data = {}
    for key, value in data.items():
        if isinstance(value, dict):
            cleaned_data[key] = remove_special_characters(value)
        elif isinstance(value, list):
            cleaned_data[key] = [remove_special_characters(item) for item in value]
        else:
            if not is_date_or_time(value):
                cleaned_value = re.sub(r'[^\w\s]', '', str(value))
                cleaned_value = emoji.demojize(cleaned_value)
                cleaned_data[key] = cleaned_value
            else:
                cleaned_data[key] = value
    return cleaned_data

#Defining MYSQL
mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Indranibk95@",
        database="youtubeDatabase")
cursor = mydb.cursor()
mydb.commit()

#Inserting Channel Data in from MongoDB to Mysql
def channel_insertion():
    drop_cquery = '''DROP TABLE IF EXISTS channeltable'''
    cursor.execute(drop_cquery)
    mydb.commit()
    query = '''CREATE TABLE IF NOT EXISTS channeltable(Channel_ID VARCHAR(225) PRIMARY KEY,
                            Channel_Name VARCHAR(225),
                            Channel_Type VARCHAR(255),
                            Channel_Views BIGINT,
                            Channel_Desc VARCHAR(3000),
                            Channel_Subscription BIGINT,
                            Channel_video BIGINT,
                            PlayListId VARCHAR(100));'''
    cursor.execute(query)
    mongo_data = mycol.find()
    for record in mongo_data:
        text_data = record.get('channelData', '')
        text_new = remove_special_characters(text_data)
        columns = ', '.join("" + str(x).replace('/', '_') + "" for x in text_new.keys())  # ch_list.keys())
        values = ', '.join("'" + str(x).replace('/', '_') + "'" for x in text_new.values())  # ch_list.values())
        insert_query = "INSERT INTO channeltable VALUES (%s);" % (values)
        cursor.execute(insert_query)
        mydb.commit()
    return "Inserted Channel Data to MySql"
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)
def iso8601_duration_to_mysql_time(duration_str):
    match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration_str)
    hours, minutes, seconds = map(int, match.groups(default='0'))

    seconds_match = re.match(r'^(\d+)', str(seconds))
    seconds = int(seconds_match.group(1)) if seconds_match else 0

    total_seconds = hours * 3600 + minutes * 60 + seconds
    return str(timedelta(seconds=total_seconds))

#Inserting Video Data in from MongoDB to Mysql
def videoInsertion():
    drop_vquery = '''DROP TABLE IF EXISTS videostable'''
    cursor.execute(drop_vquery)
    mydb.commit()
    query = ''' CREATE TABLE IF NOT EXISTS videostable(video_id VARCHAR(255) PRIMARY KEY,
              Channel_Name VARCHAR(255),  
              videoTitle VARCHAR(255),
              videoDesc VARCHAR(3000),
              tags VARCHAR(3000),
              published_At DATETIME,
              duration TIME,
              viewCount INT,
              likeCount INT,
              favoriteCount INT,
              commentCount INT,
              caption VARCHAR(255));'''
    cursor.execute(query)

    mongo_data = mycol.find()
    for record in mongo_data:
        for i in range(len(record["videoData"])):
            video_data = record['videoData'][i]
            video_new = remove_special_characters(video_data)

            published_at_str = video_new.get('published_At', '')
            published_at_datetime = datetime.strptime(published_at_str, '%Y-%m-%dT%H:%M:%SZ')
            video_new['published_At'] = published_at_datetime

            duration_str = video_new.get('duration', '')
            video_new['duration'] = iso8601_duration_to_mysql_time(duration_str)

            json_data = json.dumps(video_new, cls=DateTimeEncoder)
            video_new = json.loads(json_data)

            insert_query = '''INSERT INTO videostable(video_id,
                                        Channel_Name,
                                        videoTitle,
                                        videoDesc,
                                        tags,
                                        published_At,
                                        duration,
                                        viewCount,
                                        likeCount,
                                        favoriteCount,
                                        commentCount,
                                        caption)VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'''
            values = (
                video_new.get('video_id', ''),
                video_new.get('Channel_Name', ''),
                video_new.get('videoTitle', ''),
                video_new.get('videoDesc', ''),
                video_new.get('tags', ''),
                video_new.get('published_At', ''),
                video_new.get('duration', ''),
                video_new.get('viewCount', ''),
                video_new.get('likeCount', ''),
                video_new.get('favoriteCount', ''),
                video_new.get('commentCount', ''),
                video_new.get('caption', ''))
            cursor.execute(insert_query,values)
    mydb.commit()
    return "Inserted Video Data to Mysql"

#Inserting Comments Data in from MongoDB to Mysql
def commentInsert():

    drop_cmquery = '''DROP TABLE IF EXISTS commentstable'''
    cursor.execute(drop_cmquery)
    mydb.commit()
    query = '''CREATE TABLE IF NOT EXISTS commentstable(commentid VARCHAR(255) PRIMARY KEY,
          comment_author VARCHAR(255),
          publishesAt VARCHAR(100),
          comment_text VARCHAR(600));'''
    cursor.execute(query)
    for ch_data in mycol.find():
        for i in range(len(ch_data["commentsData"])):
            cm_list = ch_data["commentsData"][i]
            cleaned_data = remove_special_characters(cm_list)
            columns = ', '.join("" + str(x).replace('/', '_') + "" for x in cleaned_data.keys())
            values = ', '.join("'" + str(x).replace('/', '_') + "'" for x in cleaned_data.values())
            query = "INSERT INTO commentstable VALUES (%s);" % (values)
            cursor.execute(query)
            mydb.commit()
    return "Inserted Comment Data to MySql"

def channel_table():
   channel_insertion()
   videoInsertion()
   commentInsert()
   return "Tables created successfully"

# Streamlit
st.header(':red[YOUTUBE DATA HARVESTING AND WAREHOUSING]', divider='rainbow')

c_id = st.text_input("Enter Youtube Channel Id")
if st.button("Collect and Store Data"):
    ch_ids = []
    mycol = my_db["Youtube_test"]
    for ch_data in mycol.find():
        ch_ids.append(ch_data["channelData"]["Channel_ID"])
    if c_id in ch_ids:
        st.success("Channel ID already Exists")
    else:
        insert = main(c_id)
        st.success(insert)
if st.button("Data Migration to MYSQL"):
    Table = channel_table()
    st.success(Table)

questions = st.selectbox("Select your Question", ("1. What are the names of all the videos and their corresponding channels?",
             "2. which channels have the most number of videos, and how many videos do they have?",
             "3. what are the top 10 most viewed videos, and their respective channels?",
             "4. How many comments were made on each video, and what are their corresponding video names?",
             "5. which videos have the highest number of likes and what are their corresponding channel names?",
             "6. what are total number likes and dislikes for each video what are their corresponding video names?",
             "7. what is total number of views for each channel and what are their corresponding channel names?",
             "8. what are the names of all the channels that have published videos in the year 2022?",
             "9. what is average duration of all videos in each channel what's their corresponding channel names?",
             "10. which videos have highest number of comments, and what are their corresponding channel names?"))

if questions == "1. What are the names of all the videos and their corresponding channels?":
    sql1 = '''select videoTitle as videoTitle, Channel_Name as Channel_Name from videostable order by Channel_Name;'''
    cursor.execute(sql1)
    data1 = cursor.fetchall()
    q1 = pd.DataFrame(data1, columns=["videoTitle", "Channel_Name"])
    st.write(q1)

elif questions == "2. which channels have the most number of videos, and how many videos do they have?":
    sql2 = '''select Channel_Name as Channel_Name, Channel_video as no_videos from channeltable
              order by Channel_video desc;'''
    cursor.execute(sql2)
    data2 = cursor.fetchall()
    q2 = pd.DataFrame(data2, columns=["ChannelName", "NO_OF_videos"])
    st.write(q2)

elif questions == "3. what are the top 10 most viewed videos, and their respective channels?":
    sql3 = '''select viewCount as viewCount, Channel_Name as Channel_Name, videoTitle as videoTitle from videostable
              where viewCount is not null order by viewCount desc limit 10;'''
    cursor.execute(sql3)
    data3 = cursor.fetchall()
    q3 = pd.DataFrame(data3, columns=["view_count", "channelName", "video_title"])
    st.write(q3)

elif questions == "4. How many comments were made on each video, and what are their corresponding video names?":
    sql4 = '''select commentCount as commentCount, videoTitle as videoTitle from videostable where commentCount is not null;'''
    cursor.execute(sql4)
    data4 = cursor.fetchall()
    q4 = pd.DataFrame(data4, columns=["comment_counts", "video_title"])
    st.write(q4)

elif questions == "5. which videos have the highest number of likes and what are their corresponding channel names?":
    sql5 = '''select videoTitle as videoTitle, Channel_Name as Channel_Name, likeCount as likeCount from videostable
              where likeCount is not null order by likeCount desc;'''
    cursor.execute(sql5)
    data5 = cursor.fetchall()
    q5 = pd.DataFrame(data5, columns=["video_title","channel-name","like_count"])
    st.write(q5)

elif questions == "6. what are total number likes and dislikes for each video what are their corresponding video names?":
    sql6 = '''SELECT videoTitle, SUM(likeCount) AS total_likes FROM videostable GROUP BY videoTitle;'''
    cursor.execute(sql6)
    data6 = cursor.fetchall()
    q6 = pd.DataFrame(data6, columns=["videoTitle", "likeCount"])
    st.write(q6)

elif questions == "7. what is total number of views for each channel and what are their corresponding channel names?":
    sql7 = '''select Channel_Name as Channel_Name, Channel_Views as Channel_Views from channeltable;'''
    cursor.execute(sql7)
    data7 = cursor.fetchall()
    q7 = pd.DataFrame(data7, columns=["channel_name", "total_views"])
    st.write(q7)

elif questions == "8. what are the names of all the channels that have published videos in the year 2022?":
    sql8 = '''select videoTitle, published_At as published_At, 
              Channel_Name from videostable
              where year(published_At) = 2022;'''
    cursor.execute(sql8)
    data8 = cursor.fetchall()
    q8 = pd.DataFrame(data8, columns=["videoTitle", "published_At", "Channel_Name"])
    st.write(q8)

elif questions == "9. what is average duration of all videos in each channel what's their corresponding channel names?":
    sql9 = '''select Channel_Name as Channel_Name, AVG(duration) as duration from videostable group by Channel_Name;'''
    cursor.execute(sql9)
    data9 = cursor.fetchall()
    q9 = pd.DataFrame(data9, columns=["Channel_Name", "duration"])
    T9 = []
    for index,row in q9.iterrows():
        channelTitle = row["Channel_Name"]
        avg_dur = row["duration"]
        avg_dur_str = str(avg_dur)
        T9.append(dict(channel_title=channelTitle, avgDuration=avg_dur_str))
    df9 = pd.DataFrame(T9)
    st.write(q9)

elif questions == "10. which videos have highest number of comments, and what are their corresponding channel names?":
    sql10 = '''select videoTitle as videoTitle, Channel_Name as Channel_Name, commentCount as commentCount 
               from videostable where commentCount is not null order by commentCount desc;'''
    cursor.execute(sql10)
    data10 = cursor.fetchall()
    q10 = pd.DataFrame(data10, columns=["video_title","channelName_", "comment_Count"])
    st.write(q10)
mydb.commit()

#*****************************************************************************************************************************************
