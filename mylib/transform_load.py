import os
import pandas as pd
from databricks import sql
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG)

# Load environment variables
load_dotenv(override=True)


def load(dataset="data/movies.csv"):
    """Transforms and Loads data into the Databricks database."""
    # Check if the dataset exists before proceeding
    if not os.path.exists(dataset):
        raise FileNotFoundError(f"Dataset file {dataset} not found.")

    # Load and transform the dataset
    df = pd.read_csv(dataset, delimiter=",")
    df.columns = df.columns.str.strip()

    # Unpivot the genre column
    df = (
        df.assign(genre=df["genre"].str.split("|"))
        .explode("genre")
        .reset_index(drop=True)
    )
    logging.debug(f"Transformed dataset:\n{df.head()}")

    # Retrieve environment variables
    server_h = os.getenv("SERVER_HOST")
    access_token = os.getenv("DATABRICKS_API_KEY")
    http_path = os.getenv("SQL_HTTP")

    if not server_h or not access_token or not http_path:
        raise ValueError("Environment variables not set correctly.")

    # Connect to Databricks and load the data
    try:
        with sql.connect(
            server_hostname=server_h,
            http_path=http_path,
            access_token=access_token,
            timeout=30,
        ) as connection:
            cursor = connection.cursor()

            # Create the table if it doesn't exist
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS csm_87_movies (
                    id BIGINT,
                    title STRING,
                    genre STRING
                )
                USING DELTA;
                """
            )
            logging.info("Persistent table created or exists already.")

            # Insert data row-by-row with proper escaping
            logging.info("Inserting data into the table row-by-row.")
            for _, row in df.iterrows():
                id_val = int(row["id"])
                title_val = row["title"].replace("'", "''")  # Escape single quotes
                genre_val = row["genre"].replace("'", "''")  # Escape single quotes

                cursor.execute(
                    f"""
                    INSERT INTO csm_87_movies (id, title, genre)
                    VALUES ({id_val}, '{title_val}', '{genre_val}');
                    """
                )
            logging.info("Data successfully inserted into the csm_87_movies table.")

    except Exception as e:
        logging.error(f"Error while connecting to Databricks: {e}")
        raise


if __name__ == "__main__":
    load()
