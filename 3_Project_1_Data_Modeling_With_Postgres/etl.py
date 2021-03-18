import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
    
    
def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)
    
    # insert song record
    song_data = (df[["song_id", "title", "artist_id", "year", "duration"]].values).tolist()[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = (df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values).tolist()[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file 
    df = pd.read_json(filepath, lines=True)
    #df = df[df["userId"] != '']

    # filter by NextSong action
    df = df[df["page"] == "NextSong"] 
    
    # convert timestamp column to datetime
    df['ts'] = df['ts'].astype('datetime64[ms]')
    
    # insert time data records
    time_data = {'start_time': df.ts.values, "hour" : df.ts.dt.hour.values, "day" : df.ts.dt.day.values, "week" : df.ts.dt.week.values, \
                 "month" : df.ts.dt.month.values, "year" : df.ts.dt.year.values, "weekday" : df.ts.dt.weekday_name.values}
    #column_labels = 
    time_df = pd.DataFrame(data=time_data) 

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))

        
def postgresql_to_dataframe(conn, select_query, column_names):
    """
    Tranform a SELECT query into a pandas dataframe
    """
    cursor = conn.cursor()
    try:
        cursor.execute(select_query)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cursor.close()
        return 1
    
    # Naturally we get a list of tupples
    tupples = cursor.fetchall()
    cursor.close()
    
    # We just need to turn it into a pandas dataframe
    df = pd.DataFrame(tupples, columns=column_names)
    return df


def df_source_info(df, name):
    # This function is to watch general information about the data that is taking to build tables.
    
    print("\n\n\t\t\t\t DF first 3 rows overview from {} table".format(name))
    print(df.head(3))
    
    print("\n\n\t\t\t\t Data Frame general information \n", df.info())
    
    print("\n\t\t\t\t Duplicate Rows: ",len(df[df.duplicated()]))

    
def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    # Looking tables and data
    # I know is not a good practice to pull a entire DB table to a DF but we don't have massive amounts data
    columns1 = ["songplay_id","start_time", "user_id", "level", "song_id", "artist_id", "session_id", "location", "user_agent"]
    df_songplays = postgresql_to_dataframe(conn, "select * from songplays", columns1)
    df_source_info(df_songplays, "songplays")
    
    columns2 = ["user_id", "first_name", "last_name", "gender", "level"]
    df_songplays = postgresql_to_dataframe(conn, "select * from users", columns2)
    df_source_info(df_songplays, "users")
    
    columns3 = ["song_id", "title", "artist_id", "year", "duration"]
    df_songplays = postgresql_to_dataframe(conn, "select * from songs", columns3)
    df_source_info(df_songplays, "songs")
    
    columns4 = ["artist_id", "name", "location", "latitude", "longitude"]
    df_songplays = postgresql_to_dataframe(conn, "select * from artists", columns4)
    df_source_info(df_songplays, "artists")
    
    columns5 = ["start_time", "hour", "day", "week", "month", "year", "weekday"]
    df_songplays = postgresql_to_dataframe(conn, "select * from time", columns5)
    df_source_info(df_songplays, "time")
    
    
    conn.close()


if __name__ == "__main__":
    main()