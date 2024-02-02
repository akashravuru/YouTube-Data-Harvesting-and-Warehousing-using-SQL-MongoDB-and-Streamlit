from googleapiclient.discovery import build
import pymongo
import pandas as pd
import mysql.connector
import streamlit as st

#Connecting to API key 
def Api_connect():
    Api_Id="AIzaSyBEqnGgFZvFds8boo9idgS3YmppzszkXl4"
    api_service_name = "youtube"
    api_version ="v3"

    youtube=build(api_service_name,api_version,developerKey=Api_Id)

    return youtube
youtube = Api_connect()


#Function to get Channel Details 
def get_channel_info(channel_id):
    request = youtube.channels().list(
            part = "snippet,contentDetails,statistics",
            id = channel_id
        )
    response = request.execute()

    for i in response ["items"]:
        data= dict(Channel_Name=i["snippet"]["title"],
                Channel_Id=i["id"],
                Subscribers=i["statistics"]["subscriberCount"],
                Views=i["statistics"]["viewCount"],
                Total_Uploads= i["statistics"]["videoCount"],
                Channel_Description=i["snippet"]["description"],
                Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
        return data

# Function to get Video IDs
def get_video_Ids(channel_id):
    video_ids=[]
    response = youtube.channels().list(id =channel_id,
                part = "contentDetails").execute()
    Playlist_Id=response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    next_page_token= None

    while True:

        response1=youtube.playlistItems().list(
                                        part='snippet',
                                        playlistId=Playlist_Id,
                                        maxResults=50,pageToken=next_page_token).execute()
        for i in range(len(response1["items"])):
            video_ids.append(response1["items"][i]["snippet"]["resourceId"]["videoId"])
        
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids


# Function to get Video information 
video_data=[]
def get_video_info(video_ids):
    
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()

        for item in response["items"]:
            data=dict(Channel_name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Title= item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description = item['snippet'].get('description'),
                    Publish_date=item['snippet']['publishedAt'],
                    Duration = item['contentDetails']['duration'],
                    Views = item['statistics'].get('viewCount'),
                    Likes =item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favorite_Count =item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_Status=item['contentDetails']['caption'])
            video_data.append(data)
    return video_data    

#Getting Comments information
def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids: 
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50,
            )

            response=request.execute()
            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                Comment_data.append(data)
    except:
        pass
    return Comment_data

#Getting playlist details 

def get_playlist_info(channel_id):
    next_page_token = None
    All_data=[]

    while True:
        request=youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response=request.execute()

        for item in response['items']:
            data=dict(Playlist_Id=item['id'],
                    Title= item['snippet']['title'],
                    Channel_Id=item['snippet']['channelId'],
                    Channel_Name=item['snippet']['channelTitle'],
                    Published_At = item['snippet']['publishedAt'],
                    Video_count= item['contentDetails']['itemCount']
                    
                    )
            All_data.append(data)
        next_page_token= response.get('nextPageToken')
        if next_page_token is None:
            break
    return All_data


#Connecting to mongodb using Pymongo

from pymongo import MongoClient
client=MongoClient("mongodb+srv://ravuruakash:12345@cluster0.8xh637e.mongodb.net/?retryWrites=true&w=majority")
db = client["Youtube_data"]

#Getting Channel Details 
def channel_details(channel_id):
   ch_details = get_channel_info(channel_id)
   vi_ids = get_video_Ids(channel_id)
   pl_details = get_playlist_info(channel_id)
   vi_info = get_video_info(vi_ids)
   com_info = get_comment_info(vi_ids)

   col1 = db["channel_details"]

   # Check for existing channel_id using a unique index
   existing_document = col1.find_one({"channel_information.Channel_Id": channel_id})
   if existing_document:
       return "Channel already exists in database."

   # Create a unique index on channel_id if it doesn't exist
   col1.create_index("channel_information.Channel_Id", unique=True)

   try:
       col1.insert_one({"channel_information": ch_details, "playlist_details": pl_details,
                        "video_information": vi_info, "comment_information": com_info})
       return "Uploaded Successfully"
   except pymongo.errors.DuplicateKeyError:
       return "Channel already exists in database."
   

# Creating tables for details
def channel_table():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="Scarlett@2i2",
        database="youtubedb"
    )

    cur = mydb.cursor()

    drop_query = 'drop table if exists Details'
    cur.execute(drop_query)
    mydb.commit()

    create_query = "CREATE TABLE IF NOT EXISTS Details(Channel_Name VARCHAR(100), Channel_Id VARCHAR(100) PRIMARY KEY, Subscribers BIGINT, Views BIGINT, Total_Uploads BIGINT, Channel_Description TEXT, Playlist_Id VARCHAR(100))"
    cur.execute(create_query)
    mydb.commit()

    ch_list = []
    db = client["Youtube_data"]
    col1 = db["channel_details"]
    for ch_data in col1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data["channel_information"])
    
    df = pd.DataFrame(ch_list)

    for index, row in df.iterrows():
        insert_query = '''insert into Details(Channel_Name,
                                        Channel_Id,
                                        Subscribers,
                                        Views,
                                        Total_Uploads,
                                        Channel_Description,
                                        Playlist_Id)
                                        
                                        values(%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['Channel_Name'],
                  row['Channel_Id'],
                  row['Subscribers'],
                  row['Views'],
                  row['Total_Uploads'],
                  row['Channel_Description'],
                  row['Playlist_Id'])

        try:
            cur.execute(insert_query, values)
            mydb.commit()

        except:
            print('Channel details already inserted')



#Getting Playlist Details and creating table
def playlist_table():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="Scarlett@2i2",
        database="youtubedb")

    cur = mydb.cursor()

    drop_query= 'drop table if exists playlists'
    cur.execute(drop_query)
    mydb.commit()

    create_query = "CREATE TABLE IF NOT EXISTS playlists(Playlist_Id VARCHAR(100) PRIMARY KEY, Title VARCHAR(100), Channel_Id VARCHAR(100), Channel_Name VARCHAR(100), Published_At VARCHAR(50), Video_count INT)"
    cur.execute(create_query)
    mydb.commit()

    pl_list=[]
    db = client["Youtube_data"]
    col1 = db["channel_details"]
    for pl_data in col1.find({}, {"_id": 0, "playlist_details": 1}):
        for i in range(len(pl_data["playlist_details"])):
            pl_list.append(pl_data["playlist_details"][i])

    df1=pd.DataFrame(pl_list)

    for index,row in df1.iterrows():
        insert_query= '''insert into playlists(Playlist_Id,
                                        Title,
                                        Channel_Id,
                                        Channel_Name,
                                        Published_At,
                                        Video_count)
                                        
                                        values(%s,%s,%s,%s,%s,%s)'''
        values=(row['Playlist_Id'],
            row['Title'],
            row['Channel_Id'],
            row['Channel_Name'],
            row['Published_At'],
            row['Video_count'])


        cur.execute(insert_query,values)
        mydb.commit()

def videos_table():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="Scarlett@2i2",
        database="youtubedb")

    cur = mydb.cursor()

    drop_query = "DROP TABLE IF EXISTS videos"  # Ensure proper capitalization
    cur.execute(drop_query)
    mydb.commit()

    create_query = "CREATE TABLE IF NOT EXISTS videos(Channel_name VARCHAR(100),Channel_Id VARCHAR(100),Video_Id VARCHAR(100) PRIMARY KEY,Title VARCHAR(100),Thumbnail VARCHAR(200),Description TEXT,Publish_date TEXT,Duration TEXT,Views BIGINT,Likes INT,Comments INT,Favorite_Count INT,Definition VARCHAR(10),Caption_Status VARCHAR(50))"
    cur.execute(create_query)
    mydb.commit()

    vi_list=[]
    db = client["Youtube_data"]
    col1 = db["channel_details"]
    for vi_data in col1.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])

    df2=pd.DataFrame(vi_list)



    from mysql.connector import IntegrityError  # Import IntegrityE
    for index, row in df2.iterrows():
        try:
            insert_query = '''insert into videos(Channel_name,
                                                Channel_Id,
                                                Video_Id,
                                                Title,
                                                Thumbnail,
                                                Description,
                                                Publish_date,
                                                Duration,
                                                Views,
                                                Likes,
                                                Comments,
                                                Favorite_Count,
                                                Definition,
                                                Caption_Status)
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            values = (row['Channel_name'],
                    row['Channel_Id'],
                    row['Video_Id'],
                    row['Title'],
                    row['Thumbnail'],
                    row['Description'],
                    row['Publish_date'],
                    row['Duration'],
                    row['Views'],
                    row['Likes'],
                    row['Comments'],
                    row['Favorite_Count'],
                    row['Definition'],
                    row['Caption_Status'])

            cur.execute(insert_query, values)
            mydb.commit()

        except IntegrityError:
            pass  # Do nothing when a duplicate entry is encountered


def comments_table():

    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="Scarlett@2i2",
        database="youtubedb")

    cur = mydb.cursor()

    drop_query= 'drop table if exists comments'
    cur.execute(drop_query)
    mydb.commit()

    create_query = '''CREATE TABLE IF NOT EXISTS comments(Comment_Id VARCHAR (100) PRIMARY KEY,
                                                        Video_Id VARCHAR(100),
                                                        Comment_text TEXT,
                                                        Comment_Author VARCHAR(150),
                                                        Comment_Published TEXT)'''
    cur.execute(create_query)
    mydb.commit()

    com_list=[]
    db = client["Youtube_data"]
    col1 = db["channel_details"]
    for com_data in col1.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])

    df3=pd.DataFrame(com_list)

    for index,row in df3.iterrows():
        insert_query= '''insert into comments(Comment_Id,
                                        Video_Id,
                                        Comment_text,
                                        Comment_Author,
                                        Comment_Published
                                        )
                                        
                                        values(%s,%s,%s,%s,%s)'''
        values=(row['Comment_Id'],
            row['Video_Id'],
            row['Comment_text'],
            row['Comment_Author'],
            row['Comment_Published'])


        cur.execute(insert_query,values)
        mydb.commit()

def tables():
    channel_table()
    playlist_table()
    videos_table()
    comments_table()

    return"Tables Created Succesfully"
 
def show_channels_table():
    ch_list=[]
    db = client["Youtube_data"]
    col1 = db["channel_details"]
    for ch_data in col1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)

    return df

def show_playlist_table():
    pl_list=[]
    db = client["Youtube_data"]
    col1 = db["channel_details"]
    for pl_data in col1.find({}, {"_id": 0, "playlist_details": 1}):
        for i in range(len(pl_data["playlist_details"])):
            pl_list.append(pl_data["playlist_details"][i])

    df1=st.dataframe(pl_list)

    return df1

def show_videos_table():
    vi_list=[]
    db = client["Youtube_data"]
    col1 = db["channel_details"]
    for vi_data in col1.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])

    df2=st.dataframe(vi_list)
    return df2

def show_comments_table():
    com_list=[]
    db = client["Youtube_data"]
    col1 = db["channel_details"]
    for com_data in col1.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])

    df3=st.dataframe(com_list)
    
    return df3


#Streamlit

with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skill Acquisition")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("Mongo Db")
    st.caption("API Integration")
    st.caption("Data Management Using MongoDB and SQL")

channel_id = st.text_input("Input Channel ID")
if st.button("Compile and Store Data"):
    ch_ids=[]
    db=client["Youtube_data"]
    col1=db["channel_details"]
    for ch_data in col1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]['Channel_Id'])
    
    if channel_id in ch_ids:
        st.success("Channel information for the specified channel ID already exists")
    else:
       insert=channel_details(channel_id)
       st.success(insert)

if st.button("Switch to SQL"):
    Table = tables()
    st.success(Table)
show_table=st.radio("Choose table for display",("Channels","Playlists","Videos","Comments"))

if show_table=="Channels":
    show_channels_table()

elif show_table=="Playlists":
    show_playlist_table()

elif show_table=="Videos":
    show_videos_table()

elif show_table=="Comments":
    show_channels_table()


#SQL Connection
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="Scarlett@2i2",
    database="youtubedb")

cur = mydb.cursor()

question = st.selectbox("Pick your Question",("1. Video content and Channel title",
                                              "2. Channels with the highest video count",
                                              "3. Top 10 videos by views",
                                              "4. Comments received in each video",
                                              "5. Top liked videos",
                                              "6. Total likes across all videos",
                                              "7. Total views per channel",
                                              "8. Videos released in 2022",
                                              "9. Average video duration by channel",
                                              "10. Videos with the most comments"
                                              ))


if question=="1. Video content and Channel title":

    query1= ("SELECT Title AS videos, Channel_Name AS channelname from videos")
    cur.execute(query1)


    t1=cur.fetchall()

    df=pd.DataFrame(t1,columns=["video title","channel name"])

    st.write(df)

elif question=="2. Channels with the highest video count":

    query2= ('''SELECT Channel_Name AS channelname,Total_Uploads as no_videos from Details
                ORDER BY Total_Uploads DESC ''')
    cur.execute(query2)


    t2=cur.fetchall()

    df2=pd.DataFrame(t2,columns=["channel name","num of videos"])

    st.write(df2)


elif question== "3. Top 10 videos by views":

    query3 = ('''SELECT Views as views, Channel_Name AS channelname, Title as video_title FROM videos
            WHERE VIEWS IS NOT NULL ORDER BY VIEWS DESC LIMIT 10 ''')
    cur.execute(query3)

    t3 = cur.fetchall()

    df3 = pd.DataFrame(t3, columns=["views", "channel name", "video title"])
    st.write(df3)

elif question== "4. Comments received in each video":

    query4 = ('''SELECT Comments as num_comments, Title as video_title FROM videos
            WHERE COMMENTS IS NOT NULL''')
    cur.execute(query4)

    t4 = cur.fetchall()

    df4 = pd.DataFrame(t4, columns=["comments", "video title"])
    st.write(df4)

elif question== "5. Top liked videos":

    query5 = ('''SELECT Title as video_title,Channel_Name AS channel_name,Likes as likecount FROM videos''')
    cur.execute(query5)

    t5 = cur.fetchall()

    df5 = pd.DataFrame(t5, columns=["video title", "channel name", "like count"])
    st.write(df5)

elif question== "7. Total views per channel":

    query7 = ('''SELECT Channel_Name as channel_name, Views AS totalviews FROM Details''')
    cur.execute(query7)

    t7 = cur.fetchall()

    df7 = pd.DataFrame(t7, columns=["channel name ","total views"])
    st.write(df7)
    
elif question== "8. Videos released in 2022":

    query8 = ('''SELECT Title as video_title, Publish_Date AS publishyear,Channel_Name as channelname FROM videos
            where extract(year from Publish_date)=2022''')
    cur.execute(query8)

    t8 = cur.fetchall()

    df8 = pd.DataFrame(t8, columns=["video title","published date","channel name"])
    st.write(df8)
elif question== "9. Average video duration by channel":

    query9 = ('''SELECT Channel_Name as channel_name,AVG(Duration) FROM videos group by channel_name''')
    cur.execute(query9)

    t9 = cur.fetchall()

    df9 = pd.DataFrame(t9, columns=["channel_name","average_duration"])
    df9

elif question== "10. Videos with the most comments":

    query10 = ('''SELECT Title as video_title,Channel_Name AS channel_name,Comments AS comments FROM videos WHERE comments 
            is not NULL ORDER BY COMMENTS DESC''')
    cur.execute(query10)

    t10 = cur.fetchall()

    df10 = pd.DataFrame(t10, columns=["video_title","channel_name","comments"])
    st.write(df10)
    
