import pandas as pd
from tqdm import tqdm
from scripts.constants.spotipy_config import get_spotify_client


def chunkify(lst, size):
    """Yield successive n-sized chunks from a list."""
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def fetch_spotify_items(client, ids, fetch_func, batch_size, key):
    """Generic batch fetcher for Spotify items (tracks, artists, albums)."""
    all_items = []
    for chunk in tqdm(chunkify(ids, batch_size), total=-(-len(ids) // batch_size), desc=f"Fetching {key.capitalize()}"):
        results = fetch_func(chunk)
        all_items.extend(results[key])
    return all_items


def extract_track_info(track):
    """Extract relevant info from a Spotify track object."""
    if not track:
        return None
    artist_id = track['artists'][0]['id'] if track['artists'] else None
    album_id = track['album']['id'] if track['album'] else None
    album_image = track['album']['images'][0]['url'] if track['album']['images'] else None
    return {
        'spotify_track_id': track['id'],
        'spotify_artist_id': artist_id,
        'spotify_album_id': album_id,
        'album_cover_image': album_image
    }


def extract_artist_info(artist):
    """Extract relevant info from a Spotify artist object."""
    if not artist:
        return None
    return {
        'spotify_artist_id': artist['id'],
        'main_genre': artist['genres'][0].title() if artist['genres'] else None,
        'artist_image': artist['images'][0]['url'] if artist['images'] else None
    }


def extract_album_info(album):
    """Extract relevant info from a Spotify album object."""
    if not album:
        return None
    return {
        'spotify_album_id': album['id'],
        'album_genre': album['genres'][0].title() if album['genres'] else None
    }


def data_enrichment():
    """Enrich track data using the Spotify API and export to a parquet file."""
    print("🚀 Starting Spotify data enrichment...")

    sp = get_spotify_client()

    # Load input data
    try:
        df = pd.read_parquet("data/clean/Spotify/aux_tracksdata.parquet")
    except FileNotFoundError:
        raise SystemExit("❌ File 'aux_tracksdata.parquet' not found.")

    # Extract Spotify track IDs
    df['spotify_track_id'] = df['spotify_track_uri'].str.split(':').str[-1]
    track_ids = df['spotify_track_id'].dropna().unique().tolist()

    print(f"🔍 Fetching {len(track_ids)} tracks...")
    tracks_data = fetch_spotify_items(sp, track_ids, sp.tracks, 50, key='tracks')

    # Process track info
    enriched_tracks = [extract_track_info(t) for t in tracks_data if t]
    df_tracks = pd.DataFrame(enriched_tracks)

    # Collect unique artist and album IDs
    artist_ids = df_tracks['spotify_artist_id'].dropna().unique().tolist()
    album_ids = df_tracks['spotify_album_id'].dropna().unique().tolist()

    print(f"\n🎤 Fetching {len(artist_ids)} artists and 💿 {len(album_ids)} albums...")

    artists_data = fetch_spotify_items(sp, artist_ids, sp.artists, 50, key='artists')
    albums_data = fetch_spotify_items(sp, album_ids, sp.albums, 20, key='albums')

    df_artists = pd.DataFrame([extract_artist_info(a) for a in artists_data if a])
    df_albums = pd.DataFrame([extract_album_info(a) for a in albums_data if a])

    # Merge track, artist and album data
    print("\n🔗 Merging data and applying fallbacks...")
    df_enriched = (
        df_tracks
        .merge(df_artists, on='spotify_artist_id', how='left')
        .merge(df_albums, on='spotify_album_id', how='left')
    )

    # Fallback: use album genre if main genre is missing
    df_enriched['main_genre'] = df_enriched['main_genre'].fillna(df_enriched['album_genre'])

    # Final merge with original
    df_final = df.merge(df_enriched, on='spotify_track_id', how='left')

    # Handle missing genres
    missing_genres = df_final['main_genre'].isna().sum()
    total = len(df_final)
    df_final['main_genre'] = df_final['main_genre'].fillna("Not found")

    # Rename columns for dashboard consistency
    df_final.rename(columns={
        'track_name': 'song',
        'album_name': 'album',
        'album_artist_name': 'artist',
        'main_genre': 'genre'
    }, inplace=True)

    # Ensure string types for Power BI compatibility
    df_final = df_final.astype(str)

    # Export enriched dataset
    output_path = "data/clean/tracksdata.parquet"
    df_final.to_parquet(output_path, index=False)

    print(f"\n✅ Exported enriched data to {output_path}")
    print(f"ℹ️ Missing genres: {missing_genres} / {total} ({missing_genres/total:.2%})")

    return df_final
