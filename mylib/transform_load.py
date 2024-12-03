import os
import pandas as pd
from databricks import sql
from dotenv import load_dotenv
import logging
import urllib3

# Disable SSL warnings and enable verbose logging
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(level=logging.DEBUG)

# Load environment variables from .env file
load_dotenv(override=True)


# Load the CSV file and insert it into Databricks
def load(dataset="data/Spotify_Most_Streamed_Songs.csv"):
    """Transforms and Loads data into the Databricks database"""
    # Check if the dataset exists before proceeding
    if not os.path.exists(dataset):
        raise FileNotFoundError(f"Dataset file {dataset} not found.")

    # Load the CSV file
    df = pd.read_csv(dataset, delimiter=",", skiprows=1)
    df.columns = df.columns.str.strip()

    logging.debug(f"Columns in the dataset: {df.columns}")

    # Retrieve environment variables
    server_h = os.getenv("server_host")
    access_token = os.getenv("databricks_api_key")
    http_path = os.getenv("sql_http")

    # Error handling for missing environment variables
    if not server_h or not access_token or not http_path:
        raise ValueError("Environment variables not set correctly.")

    # Ensure the http_path is stripped properly
    http_path = http_path.strip()

    full_url = (
        f"https://{server_h}"
        f"{http_path if http_path.startswith('/') else '/' + http_path}"
    )
    logging.debug(f"Connecting to: {full_url}")

    # Databricks connection logic
    try:
        with sql.connect(
            server_hostname=server_h,
            http_path=http_path,
            access_token=access_token,
            timeout=30,
        ) as connection:
            c = connection.cursor()

            # Check if the table exists
            c.execute("SHOW TABLES FROM default LIKE 'csm_87_Spotify*'")
            result = c.fetchall()

            # Create table if it doesn't exist
            if not result:
                logging.debug(
                    "Table does not exist. Creating csm_87_SpotifyDB table..."
                )
                c.execute(
                    """
                    CREATE TABLE IF NOT EXISTS csm_87_SpotifyDB (
                        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,  
                        track_name STRING,
                        artist_name STRING,
                        artist_count INT,
                        released_year INT,
                        released_month INT,
                        released_day INT,
                        in_spotify_playlists INT,
                        in_spotify_charts INT,
                        streams BIGINT,
                        in_apple_playlists INT,
                        key STRING,
                        mode STRING,
                        danceability_percent INT,
                        valence_percent INT,
                        energy_percent INT,
                        acousticness_percent INT,
                        instrumentalness_percent INT,
                        liveness_percent INT,
                        speechiness_percent INT,
                        cover_url STRING
                    )
                    """
                )
                logging.debug("Table created successfully.")

            logging.debug(f"Data from CSV: \n{df.head()}")

            c.close()

        return "success"

    except Exception as e:
        logging.error(f"Error while connecting to Databricks: {e}")
        raise


if __name__ == "__main__":
    load()
