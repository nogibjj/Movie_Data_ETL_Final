import os
from databricks import sql
from dotenv import load_dotenv
import logging
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(level=logging.DEBUG)

logging.debug(f"Before loading .env - server_host: " f"{os.getenv('server_host')}")
logging.debug(f"Before loading .env - sql_http: " f"{os.getenv('sql_http')}")
logging.debug(
    f"Before loading .env - databricks_api_key: " f"{os.getenv('databricks_api_key')}"
)

load_dotenv(override=True)

logging.debug(f"After loading .env - server_host: " f"{os.getenv('server_host')}")
logging.debug(f"After loading .env - sql_http: " f"{os.getenv('sql_http')}")
logging.debug(
    f"After loading .env - databricks_api_key: " f"{os.getenv('databricks_api_key')}"
)


def get_connection():
    server_h = os.getenv("server_host")
    access_token = os.getenv("databricks_api_key")
    http_path = os.getenv("sql_http")

    logging.debug(f"Connecting to Databricks at: {server_h}{http_path}")

    try:
        connection = sql.connect(
            server_hostname=server_h, http_path=http_path, access_token=access_token
        )
        return connection
    except Exception as e:
        logging.error(f"Failed to connect to Databricks: {e}")
        raise


# Function for joining tables
def query_join():
    connection = get_connection()
    cursor = connection.cursor()

    # SQL query for joining the table with a version of itself
    query = """
        WITH artist_version AS (
            SELECT 
                DISTINCT artist_name,
                CASE 
                    WHEN artist_name LIKE '%,%' THEN 'Multiple Artists'
                    ELSE 'Single Artist'
                END AS Single_Double
            FROM csm_87_SpotifyDB
        )
        SELECT 
            s.*,
            a.Single_Double
        FROM csm_87_SpotifyDB s
        LEFT JOIN artist_version a
        ON s.artist_name = a.artist_name
    """

    # Execute the query
    cursor.execute(query)
    cursor.fetchall()  # Fetch records

    connection.close()
    return "Join Success"  # Return success message


# Function for aggregating the data
def query_aggregate():
    connection = get_connection()
    cursor = connection.cursor()

    # SQL query for aggregating data by year
    query = """
        SELECT 
            s.released_year,
            COUNT(s.track_name) AS track_count,
            SUM(s.in_spotify_playlists) AS total_in_spotify_playlists,
            COUNT(
                CASE WHEN a.Single_Double = 'Single Artist' THEN 1 END
            ) AS single_artist_count,
            COUNT(
                CASE WHEN a.Single_Double = 'Multiple Artists' THEN 1 END
            ) AS multiple_artist_count
        FROM csm_87_SpotifyDB s
        LEFT JOIN (
            SELECT 
                DISTINCT artist_name,
                CASE 
                    WHEN artist_name LIKE '%,%' THEN 'Multiple Artists'
                    ELSE 'Single Artist'
                END AS Single_Double
            FROM csm_87_SpotifyDB
        ) a
        ON s.artist_name = a.artist_name
        GROUP BY s.released_year
    """

    # Execute the query
    cursor.execute(query)
    cursor.fetchall()

    connection.close()
    return "Aggregate Success"  # Return success message


# Function for sorting the results
def query_sort():
    connection = get_connection()
    cursor = connection.cursor()

    # SQL query for sorting by year
    query = """
        SELECT 
            s.released_year,
            COUNT(s.track_name) AS track_count,
            SUM(s.in_spotify_playlists) AS total_in_spotify_playlists,
            COUNT(
                CASE WHEN a.Single_Double = 'Single Artist' THEN 1 END
            ) AS single_artist_count,
            COUNT(
                CASE WHEN a.Single_Double = 'Multiple Artists' THEN 1 END
            ) AS multiple_artist_count
        FROM csm_87_SpotifyDB s
        LEFT JOIN (
            SELECT 
                DISTINCT artist_name,
                CASE 
                    WHEN artist_name LIKE '%,%' THEN 'Multiple Artists'
                    ELSE 'Single Artist'
                END AS Single_Double
            FROM csm_87_SpotifyDB
        ) a
        ON s.artist_name = a.artist_name
        GROUP BY s.released_year
        ORDER BY s.released_year
    """

    # Execute the query
    cursor.execute(query)
    records = cursor.fetchall()

    # Debugging the fetched records
    logging.debug(f"Sort Query Results: {records}")

    if records:
        connection.close()
        return "Sort Success"
    else:
        connection.close()
        return "Sort Failed"
