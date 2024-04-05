import pymongo
import psycopg2
import pandas as pd
import streamlit as st
from googleapiclient.discovery import build
from PIL import Image

api_key = "AIzaSyD6vLsmEDpghkhWu8hytSi1szPUe_dj7g0"
youtube = build("youtube", "v3", developerKey=api_key)

#get_channel_details

def get_channel_details(channel_id):
    request=youtube.channels().list(part="snippet,statistics,contentDetails",id=channel_id)
    response=request.execute()
    
    for i in response["items"]:
       data=dict(channel_name=i["snippet"]["title"],
                 channel_description=i["snippet"]["description"],
                 Channel_Id=i["id"],
                 publishedTime=i["snippet"]["publishedAt"],
                 subscriberCount=i["statistics"]["subscriberCount"],
                 videoCount=i["statistics"]["videoCount"] )
       return data
    
#get_playlist_details

def get_playlist_details(channel_id):

    playlist_details=[]
    next_page_token=None

    while True:
    

        request=youtube.playlists().list(part="snippet,contentDetails",
                                            channelId=channel_id,
                                            maxResults=50,pageToken=next_page_token
                                            
                                            )
        playlist_details=[]
        response=request.execute()
        for i in  response["items"]:
            data=dict(playlist_id=i["id"],
                        title=i["snippet"]["title"],
                        channel_id=i["snippet"]["channelId"],
                        channel_name=i["snippet"]["channelTitle"],
                        publishedAt=i["snippet"]["publishedAt"],
                        video_count=i["contentDetails"]["itemCount"]
                        
                        )
            playlist_details.append(data)
        next_page_token = response.get('nextPageToken') 
        if next_page_token is None:
            break

    return   playlist_details
        

# get video details

def get_video_details(video_id):

    video_details=[]
    for i in video_id:
        request=youtube.videos().list(part="snippet,ContentDetails,statistics",id=i)
        response=request.execute()

        for i in response["items"]:
            data=dict(Channel_Name = i['snippet']['channelTitle'],
                        Channel_Id = i['snippet']['channelId'],
                        Video_Id = i['id'],
                        Title = i['snippet']['title'],
                        Tags = i['snippet'].get('tags'),
                        Description = i['snippet']['description'],
                        Published_Date = i['snippet']['publishedAt'],
                        Duration = i['contentDetails']['duration'],
                        Views = i['statistics']['viewCount'],
                        Likes = i['statistics'].get('likeCount'),
                        Comments = i['statistics'].get('commentCount'),                       
                        )
            video_details.append(data)    
    return  video_details   

#get channel_videos

def get_channel_videos(channel_id):
        video_ids = []
        request=youtube.channels().list(part="contentDetails",id=channel_id)
        response1=request.execute()

        for i in  response1["items"]:
                playlist_id=i["contentDetails"]["relatedPlaylists"]["uploads"]
                next_page_token = None
                while True:
                        request=youtube.playlistItems().list(part="contentDetails,snippet",
                                           playlistId = playlist_id, 
                                           maxResults = 50,
                                           pageToken = next_page_token)
                        
                        
                        response=request.execute()
                        for i in response['items']:
                         video_ids.append(i['snippet']['resourceId']['videoId'])
                         next_page_token = response.get('nextPageToken')  


                        if next_page_token is None:
                         break
                        
                return video_ids
        
#get_comment_details        

def get_comment_details(video_ides):
        comment_details=[]

        try:
            for i in video_ides:
                request=youtube.commentThreads().list(part="snippet",
                                                    maxResults=50,
                                                    videoId=i )
                response=request.execute()

                for i in response["items"]:
                    data=dict(channel_id=i["snippet"]["channelId"],
                            video_id=i["snippet"]["videoId"],
                            comment_id=i["id"],
                            author=i["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                            comment_text=i["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                            comment_published=i["snippet"]["topLevelComment"]["snippet"]["publishedAt"],

                            )
                    comment_details.append(data)
        except :
            pass

        return comment_details      

#connection to mongodb

client=pymongo.MongoClient("mongodb+srv://ajaikumar:ajai1994@cluster0.yehtbce.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db=client["youtube_data_test"]


#insert into mongodb

def insert_into_mongodb(channel_id):
    CHANNEL_DETAILS=   get_channel_details(channel_id)
    VIDEO_ID= get_channel_videos(channel_id) 
    PLAYLIST_DETAILS=get_playlist_details(channel_id)   
    VIDEO_DETAILS=get_video_details(VIDEO_ID)
    COMMENT_DETAILS= get_comment_details(VIDEO_ID)

    coll1=db["channel_information"]
    coll1.insert_one({"channel_info":CHANNEL_DETAILS,"video_info":VIDEO_DETAILS,"playlist_info":PLAYLIST_DETAILS,"comment_info":COMMENT_DETAILS})
   
    return "completed_successful"
    

#insert into sql
#channels_table

def channel_tabel(channel_name_s):


    mydb=psycopg2.connect(user="postgres",
                        database="youtube_test",
                        host="localhost",
                        port=5432,
                        password="ajai"
                        
                        )
    cursor=mydb.cursor()


    try:

        create_querry='''CREATE TABLE IF NOT EXISTS channel_details(channel_name varchar(255),
                                                                    channel_description text  ,
                                                                    Channel_Id varchar(255)primary key,
                                                                    publishedTime timestamp,
                                                                    subscriberCount bigint,
                                                                    videoCount bigint)
                                                                    '''
        cursor.execute(create_querry)
        mydb.commit()

    except:
     print("channels table alrady created")

    ch_data=[]
    db=client["youtube_data_test"]
    coll1=db["channel_information"]

    for i in coll1.find({"channel_info.channel_name":channel_name_s},{"_id":0}):
        ch_data.append(i["channel_info"])
        df=pd.DataFrame(ch_data)
        
   
                                                                
    insert_querry='''INSERT INTO channel_details(channel_name,
                                                    channel_description,
                                                    Channel_Id,
                                                    publishedTime,
                                                    subscriberCount,
                                                    videoCount)
                                                    values(%s,%s,%s,%s,%s,%s)'''
        
    try:    

        data=df.values.tolist()
        cursor.executemany(insert_querry,data)
        mydb.commit()

    except:
        print("channel_values alredy inserted")


#insert into sql
#playlist_table


def playlist_table(channel_name_s):
    mydb=psycopg2.connect(user="postgres",
                        database="youtube_test",
                        host="localhost",
                        port=5432,
                        password="ajai"
                        
                        )
    cursor=mydb.cursor()

   
    try:
        create_querry1='''CREATE TABLE IF NOT EXISTS playlist_details(playlist_id varchar(255) primary key,
                                                                    playlist_title varchar(255)  ,
                                                                    Channel_Id varchar(255),
                                                                    channel_name varchar(255),
                                                                    publishedAt timestamp,
                                                                    video_count int)
                                                                    '''
        cursor.execute(create_querry1)
        mydb.commit()
    except:
       print("playlist table alrady created")

    pl_data=[]
    db=client["youtube_data_test"]
    coll1=db["channel_information"]

    for i in coll1.find({"channel_info.channel_name":channel_name_s},{"_id":0}):
     for j in range(len(i["playlist_info"])):
      pl_data.append(i["playlist_info"][j])
    df1=pd.DataFrame(pl_data)
        
                                                           
    insert_querry1='''INSERT INTO playlist_details(playlist_id,
                                                    playlist_title,
                                                    Channel_Id,
                                                    channel_name,
                                                    publishedAt,
                                                    video_count)
                                                    values(%s,%s,%s,%s,%s,%s)'''
    
    try:

        data1=df1.values.tolist()
        cursor.executemany(insert_querry1,data1)
        mydb.commit()
    except:
       print("channel_values alredy inserted")


# video_details_table
#videos_table


def videos_table(channel_name_s):

    mydb=psycopg2.connect(user="postgres",
                        database="youtube_test",
                        password="ajai",
                        host="localhost",
                        port=5432)
    cursor=mydb.cursor()


    try:

        create_querry2='''CREATE TABLE IF NOT EXISTS video_details(channel_name varchar(255),
                                                                    channel_id varchar(255),
                                                                    video_id varchar(255) primary key,
                                                                    title varchar(255),
                                                                    tags text,
                                                                    description text,
                                                                    publishedAt timestamp,
                                                                    duration interval,
                                                                    viewCount bigint,
                                                                    likeCount bigint,
                                                                    commentCount bigint)'''
        cursor.execute(create_querry2)
        mydb.commit()

    except:
     print("playlist table alrady created")
        
  

    vd_data=[]
    db=client["youtube_data_test"]
    coll1=db["channel_information"]
    for i in coll1.find({"channel_info.channel_name":channel_name_s},{"_id":0}):
        for j in range(len(i["video_info"])):
            vd_data.append(i["video_info"][j])
        df2=pd.DataFrame(vd_data)



    insert_querry2='''INSERT INTO video_details(channel_name,
                                                channel_id,
                                                video_id,
                                                title,
                                                tags,
                                                description,
                                                publishedAt,
                                                duration,
                                                viewCount,
                                                likeCount,
                                                commentCount)
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
    
    
    try:
        
        data2=df2.values.tolist()
        cursor.executemany(insert_querry2,data2)
        mydb.commit()

    except:
       print("videos_values alredy inserted")    



#insert into comments table


def comments_table(channel_name_s):

    mydb=psycopg2.connect(user="postgres",
                        database="youtube_test",
                        password="ajai",
                        port=5432,
                        host="localhost")
    cursor=mydb.cursor()


    try:


        create_querry3='''CREATE TABLE IF NOT EXISTS comments_details(channel_id varchar(255),
                                                                    video_id varchar(255),
                                                                    comment_id varchar(255),
                                                                    author varchar(255),
                                                                    comment_text text,
                                                                    comment_published timestamp)'''
        cursor.execute(create_querry3)
        mydb.commit()
    except:
           print("comments table alrady created")

    comment_details=[]
    db=client["youtube_data_test"]
    coll1=db["channel_information"]

    for i in coll1.find({"channel_info.channel_name":channel_name_s},{"_id":0}):
        for j in range(len(i["comment_info"])):
            comment_details.append(i["comment_info"][j])
        df3=pd.DataFrame(comment_details)

    insert_querry3='''INSERT INTO comments_details(channel_id,
                                                    video_id,
                                                    comment_id,
                                                    author,
                                                    comment_text,
                                                    comment_published)
                                                    values(%s,%s,%s,%s,%s,%s)'''   
    try:
        data3=df3.values.tolist() 
        cursor.executemany(insert_querry3,data3)
        mydb.commit()

        
    except:
       print("comments_values alredy inserted") 


#execution og table

def create_table(single_channel):

    channel_tabel(single_channel)
    playlist_table(single_channel)
    videos_table(single_channel)
    comments_table(single_channel)
    return st.success("TABLES CREATED SUCCESSFULLY")

#tabels to view

def ch_table(channel_name_s):
         
        ch_data=[]
        db=client["youtube_data_test"]
        coll1=db["channel_information"]

        for i in coll1.find({"channel_info.channel_name":channel_name_s},{"_id":0}):
         ch_data.append(i["channel_info"])
        channels_table=st.dataframe(ch_data)
        return channels_table

#PLAYLIST INFO

def pl_table(channel_name_s):
    pl_data=[]
    db=client["youtube_data_test"]
    coll1=db["channel_information"]

    for i in coll1.find({"channel_info.channel_name":channel_name_s},{"_id":0}):
     for j in range(len(i["playlist_info"])):
         pl_data.append(i["playlist_info"][j])
    playlist_table=st.dataframe(pl_data)
    return playlist_table

#VIDEO_DETAILS

def vid_table(channel_name_s):
    vd_data=[]
    db=client["youtube_data_test"]
    coll1=db["channel_information"]

    for i in coll1.find({"channel_info.channel_name":channel_name_s},{"_id":0}):
     for j in range(len(i["video_info"])):
            vd_data.append(i["video_info"][j])
    video_table=st.dataframe(vd_data)        
    return video_table 

#COMMENTS_TABLE

def comm_table(channel_name_s):
    comm_data=[]
    db=client["youtube_data_test"]
    coll1=db["channel_information"]

    for i in coll1.find({"channel_info.channel_name":channel_name_s},{"_id":0}):
        for j in range(len(i["comment_info"])):
            comm_data.append(i["comment_info"][j])
    comments_table=st.dataframe(comm_data)       
    return  comments_table     

#STREAMLIT PART

st.title(":WHITE[YOUTUBE DATA HARVESTING AND WAREHOUSING]")

with st.sidebar:
    image = Image.open(r"C:\Users\USER\Downloads\pngwing.com.png")
    st.image(image, caption="YOUTUBE", use_column_width=True)
    st.header("SKILL TAKE AWAY")
    st.caption('Python scripting') 
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption(" Data Managment using MongoDB and SQL")
    image = Image.open(r"C:\Users\USER\Desktop\python\final\mongodb.png")
    st.image(image, use_column_width=True)
    image1 = Image.open(r"C:\Users\USER\Desktop\python\final\python_original_logo_icon_146381.png")
    st.image(image1, use_column_width=True)
    image2 = Image.open(r"C:\Users\USER\Desktop\python\final\folder_postgres_icon_161286.png")
    st.image(image2, use_column_width=True)
    image3 = Image.open(r"C:\Users\USER\Desktop\python\final\streamlit_logo_icon_249495.png")
    st.image(image3, use_column_width=True)
    image4 = Image.open(r"C:\Users\USER\Desktop\python\final\microsoft_visual_studio_code_macos_bigsur_icon_189957.png")
    st.image(image4, use_column_width=True)


channel_id = st.text_input("ENTER THE CHANNEL ID")
channels = channel_id.split(',')
channels = [ch.strip() for ch in channels if ch]

if st.button("Collect and Store data 🤑"):
    for channel in channels:
        ch_ids = []
        db = client["youtube_data_test"]
        coll1 = db["channel_information"]
        for ch_data in coll1.find({},{"_id":0,"channel_info":1}):
            ch_ids.append(ch_data["channel_info"]["Channel_Id"])
        if channel in ch_ids:
            st.success("Channel details of the given channel id: " + channel + " already exists")
        else:
            output =insert_into_mongodb(channel_id)
            st.success(output)

# get uniqe channel names and store in sql

alldata=[]
db=client["youtube_data_test"]
coll1=db["channel_information"]

for i in coll1.find({},{"_id":0,"channel_info":1}):
    alldata.append(i["channel_info"]["channel_name"])            


unique_channel=st.selectbox("SELECT THE CHANNEL NAME TO VIEW DETAILS",alldata)            
            
if st.button("Migrate to SQL"):
    display = create_table(unique_channel)
    st.success(display)

show_table = st.radio("SELECT THE TABLE FOR VIEW",(":WHITE[CHANNELS]",":WHITE[PLAYLISTS]",":WHITE[VIDEOS]",":WHITE[COMMENTS]"))    

if show_table == ":WHITE[CHANNELS]":
    ch_table(unique_channel)
elif show_table == ":WHITE[PLAYLISTS]":
    pl_table(unique_channel)
elif show_table ==":WHITE[VIDEOS]":
    vid_table(unique_channel)
elif show_table == ":WHITE[COMMENTS]":
    comm_table(unique_channel)

#sql connection

mydb=psycopg2.connect(user="postgres",
                    database="youtube_test",
                    host="localhost",
                    port=5432,
                    password="ajai"
                    
                    )
cursor=mydb.cursor() 


question= st.selectbox('Please Select Your Question',
    ('1. All the videos and the Channel Name',
     '2. Channels with most number of videos',
     '3. 10 most viewed videos',
     '4. Comments and author',
     '5. Videos with highest likes',
     '6. likes of all videos',
     '7. channels and subscribers',
     '8. videos published in the year 2022',
     '9. average duration of all videos in each channel',
     '10. videos with highest number of comments'))   

if question == '1. All the videos and the Channel Name':
    query1 = "select Title as videos, Channel_Name as ChannelName from video_details;"
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    st.write(pd.DataFrame(t1, columns=["Video Title","Channel Name"]))   

elif question == '2. Channels with most number of videos':
    query2 = "select Channel_Name as ChannelName,videoCount as NO_Videos from channel_details order by videoCount desc;"
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    st.write(pd.DataFrame(t2, columns=["Channel Name","No Of Videos"]))   

elif question == '3. 10 most viewed videos':
    query3 = '''select viewCount as views , Channel_Name as ChannelName,Title as VideoTitle from video_details 
                        where viewCount is not null order by Views desc limit 10;'''
    cursor.execute(query3)
    mydb.commit()
    t3 = cursor.fetchall()
    st.write(pd.DataFrame(t3, columns = ["views","channel Name","video title"]))     


elif question == '4. Comments and author':
    query4 = "select comment_text as comments ,author as Author from comments_details where comment_text is not null;"
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    st.write(pd.DataFrame(t4, columns=[" Comments", "Author"]))

elif question == '5. Videos with highest likes':
    query5 = '''select Title as VideoTitle, Channel_Name as ChannelName, LikeCount as Likes from video_details
                       where LikeCount is not null order by LikeCount desc;'''
    cursor.execute(query5)
    mydb.commit()
    t5 = cursor.fetchall()
    st.write(pd.DataFrame(t5, columns=["video Title","channel Name","like count"]))


elif question == '6. likes of all videos':
    query6 = '''select likeCount as likes,Title as VideoTitle from video_details;'''
    cursor.execute(query6)
    mydb.commit()
    t6 = cursor.fetchall()
    st.write(pd.DataFrame(t6, columns=["like count","video title"])) 

elif question == '7. channels and subscribers':
    query7 = "select Channel_Name as ChannelName, subscriberCount as subscribersCount from channel_details;"
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    st.write(pd.DataFrame(t7, columns=["channel name","subscriber_Count"]))      




elif question == '8. videos published in the year 2022':
    query8 = '''select Title as Video_Title, Publishedat as VideoRelease, Channel_Name as ChannelName from video_details 
                where extract(year from Publishedat) = 2022;'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    st.write(pd.DataFrame(t8,columns=["Name", "Video Publised On", "ChannelName"]))     

elif question == '9. average duration of all videos in each channel':
    query9 =  "SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM video_details GROUP BY Channel_Name;"
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    t9 = pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])
    T9=[]
    for index, row in t9.iterrows():
        channel_title = row['ChannelTitle']
        average_duration = row['Average Duration']
        average_duration_str = str(average_duration)
        T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
    st.write(pd.DataFrame(T9))   

elif question == '10. videos with highest number of comments':
    query10 = '''select Title as VideoTitle, Channel_Name as ChannelName, Commentcount as CommentCount from video_details 
                       where Commentcount is not null order by Commentcount desc;'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    st.write(pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'NO Of Comments']))     










                                                   
                   

                    

     

        
    
    
