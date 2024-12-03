SELECT * FROM SpotifyDB WHERE artist_name='Taylor Swift';

UPDATE SpotifyDB SET 
        track_name='Blinding Lights', 
        artist_name='The Weeknd', 
        artist_count=1,
        released_year=2019, 
        released_month=11, 
        released_day=29, 
        in_spotify_playlists=500, 
        in_spotify_charts=150, 
        streams=3200000000, 
        in_apple_playlists=200, 
        key='G#', 
        mode='Major', 
        danceability_percent=65, 
        valence_percent=75, 
        energy_percent=85, 
        acousticness_percent=5, 
        instrumentalness_percent=0, 
        liveness_percent=10, 
        speechiness_percent=4, 
        cover_url='https://link_to_cover_image.com'
        WHERE id=1;

DELETE FROM SpotifyDB WHERE id=2;

INSERT INTO SpotifyDB (
    track_name, artist_name, artist_count, released_year, released_month, released_day, 
    in_spotify_playlists, in_spotify_charts, streams, in_apple_playlists, key, mode, 
    danceability_percent, valence_percent, energy_percent, acousticness_percent, 
    instrumentalness_percent, liveness_percent, speechiness_percent, cover_url) 
    VALUES (
    'Levitating', 'Dua Lipa', 1, 2020, 3, 27, 700, 220, 1200000000, 180, 'C', 'Minor', 
    70, 80, 90, 10, 0, 5, 3, 'https://link_to_cover_image.com'
);

SELECT * FROM SpotifyDB WHERE artist_name='Dua Lipa';

SELECT * FROM SpotifyDB;

INSERT INTO SpotifyDB (
    track_name, artist_name, artist_count, released_year, released_month, released_day, 
    in_spotify_playlists, in_spotify_charts, streams, in_apple_playlists, key, mode, 
    danceability_percent, valence_percent, energy_percent, acousticness_percent, 
    instrumentalness_percent, liveness_percent, speechiness_percent, cover_url) 
    VALUES (
    'Good 4 U', 'Olivia Rodrigo', 1, 2021, 5, 14, 900, 300, 2000000000, 250, 'A', 'Major', 
    60, 50, 70, 20, 0, 15, 5, 'https://link_to_cover_image.com'
);

SELECT * FROM SpotifyDB;

UPDATE SpotifyDB SET 
        track_name='Montero (Call Me By Your Name)', 
        artist_name='Lil Nas X', 
        artist_count=1,
        released_year=2021, 
        released_month=3, 
        released_day=26, 
        in_spotify_playlists=800, 
        in_spotify_charts=200, 
        streams=1700000000, 
        in_apple_playlists=220, 
        key='C#', 
        mode='Minor', 
        danceability_percent=75, 
        valence_percent=60, 
        energy_percent=80, 
        acousticness_percent=15, 
        instrumentalness_percent=0, 
        liveness_percent=8, 
        speechiness_percent=6, 
        cover_url='https://link_to_cover_image.com'
        WHERE id=3;

DELETE FROM SpotifyDB WHERE id=3;

SELECT * FROM SpotifyDB WHERE artist_name='Lil Nas X';

SELECT * FROM SpotifyDB WHERE artist_name='Taylor Swift';

UPDATE SpotifyDB SET 
        track_name='As It Was', 
        artist_name='Harry Styles', 
        artist_count=1,
        released_year=2022, 
        released_month=4, 
        released_day=1, 
        in_spotify_playlists=600, 
        in_spotify_charts=180, 
        streams=2300000000, 
        in_apple_playlists=200, 
        key='D', 
        mode='Major', 
        danceability_percent=62, 
        valence_percent=58, 
        energy_percent=76, 
        acousticness_percent=12, 
        instrumentalness_percent=0, 
        liveness_percent=10, 
        speechiness_percent=4, 
        cover_url='https://link_to_cover_image.com'
        WHERE id=4;

DELETE FROM SpotifyDB WHERE id=4;

INSERT INTO SpotifyDB (
    track_name, artist_name, artist_count, released_year, released_month, released_day, 
    in_spotify_playlists, in_spotify_charts, streams, in_apple_playlists, key, mode, 
    danceability_percent, valence_percent, energy_percent, acousticness_percent, 
    instrumentalness_percent, liveness_percent, speechiness_percent, cover_url) 
    VALUES (
    'Heat Waves', 'Glass Animals', 1, 2020, 6, 29, 1000, 350, 2600000000, 300, 'E', 'Major', 
    72, 64, 78, 7, 0, 6, 4, 'https://link_to_cover_image.com'
);

SELECT * FROM SpotifyDB WHERE artist_name='Glass Animals';

SELECT * FROM SpotifyDB;```sql
INSERT INTO SpotifyDB VALUES (
            Blinding Lights, The Weeknd, 1, 2019, 11, 
            29, 500, 150, 3200000000, 
            200, G#, Major, 65, 75, 
            85, 5, 0, 10, 
            4, https://cover.url);
```

```sql
UPDATE SpotifyDB SET 
        track_name=Good 4 U, artist_name=Olivia Rodrigo, artist_count=1, 
        released_year=2021, released_month=5, released_day=14, 
        in_spotify_playlists=900, in_spotify_charts=300, 
        streams=2000000000, in_apple_playlists=250, key=A, mode=Major, 
        danceability_percent=60, valence_percent=50, 
        energy_percent=70, acousticness_percent=20, 
        instrumentalness_percent=0, liveness_percent=15, 
        speechiness_percent=5, cover_url=https://cover.url 
        WHERE id=1;
```

```sql
UPDATE SpotifyDB SET artist_name = 'Chris' WHERE id = 3;
```

```sql
UPDATE SpotifyDB SET artist_name = 'Chris' WHERE id = 3;
```

```sql
UPDATE SpotifyDB SET artist_name = 'Chris' WHERE id = 3;
```

