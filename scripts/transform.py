import pandas as pd
import glob
import os

from scripts.utils.aux_funcs import *


def dataprep_viz(input='../spotify-listening-data/data/raw/Spotify',
                 output='../spotify-listening-data/data/clean/Spotify/streaming_data.parquet',
                 output_auxtracksdata='../spotify-listening-data/data/clean/Spotify/aux_tracksdata.parquet'):
    
    '''
    Function for the first step of the data pipeline.
    It reads all JSON files from the input directory, processes the data,
    and saves the cleaned data to Parquet files.
    '''
    
    # creating a list of all JSON files in the input directory
    json_files = glob.glob(os.path.join(input, '*.json'))

    if not json_files:
        print(f"Nothing found in '{input}'")
        return

    # reading and puting all JSON files into a single DataFrame
    dfs = []
    for file in json_files:
        df = pd.read_json(file)
        dfs.append(df)
    
    df = pd.concat(dfs, ignore_index=True)
    print("\nAll combined successfully.")
    print(f"Records: {len(df):,}")

    # renaming columns
    columns_rename = {'master_metadata_track_name': 'track_name',
                      'master_metadata_album_artist_name': 'album_artist_name',
                      'master_metadata_album_album_name': 'album_name'}
    df.rename(columns=columns_rename, inplace=True)
    
    df = delete_columns(df, ['spotify_episode_uri', 'ip_addr', 'reason_start'])
    
    # column converting to categorical type
    df = dtype_trt(df, 
                   ['ts', 'platform', 'ms_played', 'offline',
                    'offline_timestamp', 'shuffle', 'skipped',
                    'incognito_mode'],

                   {'ts': 'datetime',
                    'platform': 'str',
                    'ms_played': 'int',
                    'offline': 'float',
                    'offline_timestamp': 'float',
                    'shuffle': 'boolean',
                    'skipped': 'boolean',
                    'incognito_mode': 'boolean'})

    ###########
    
    # creating a new df with spotify_track_uri in order to get main_genre by artists and song cover later
    df2 = (
        df[['track_name', 'album_name', 'album_artist_name', 'spotify_track_uri']]
        .dropna()
        .drop_duplicates()
        .reset_index(drop=True)
    )

    ###########
    
    # mapping 'platform' values to more readable names
    df['platform'] = df['platform'].astype(str).str.strip().str.title()
    
    # separating 'ts' into 'date' and 'time'
    df['date'] = df['ts'].dt.date
    df['time'] = df['ts'].dt.time

    # remove rows with empty values
    df = dropna_rows(df, ['track_name', 'album_name', 'album_artist_name'])
    
    # remove rows with empty 'album_name'
    # df = df[df['album_name'].notna() & (df['album_name'].str.strip() != '')]
    
    # filling NaN values that came from the JSON files
    df.fillna({'offline':0}, inplace=True)
    df.fillna({'offline_timestamp':0}, inplace=True)
    
    merged_streaming_history = df.copy()
    aux_enrichment = df2.copy()
    
    # Saving the DataFrame to Parquet files
    save_as_parquet(merged_streaming_history, output)
    save_as_parquet(aux_enrichment, output_auxtracksdata)
    print(f"\nDone!  Streaming data saved in '{output}' and Tracks data in '{output_auxtracksdata}'")
    print(f"Streaming data records (Only tracks): {len(df):,}")
    print(f'NaN values: {df.isna().sum().sum()}')
    
    return merged_streaming_history, aux_enrichment