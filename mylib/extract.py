import os
import requests


def extract(
    url="https://raw.githubusercontent.com/"
    "nogibjj/chris_moreira_week5_python_sql_db_project/main/"
    "data/Spotify_Most_Streamed_Songs.csv",
    file_path="data/Spotify_Most_Streamed_Songs.csv",
    timeout=10,
):
    """Extract a URL to a file path."""

    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with requests.get(url, timeout=timeout) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)

    return "success"
