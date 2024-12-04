import os
import requests
import pandas as pd


def extract(
    url="https://github.com/mohammedalawami/Movielens-Dataset/raw/master/datasets/movies.dat",
    file_path="data/movies.csv",  # Save the data as a CSV file directly
    timeout=10,
    encoding="latin1",  # Use an encoding that can handle non-UTF-8 characters
):
    """
    Extracts a dataset from a URL, processes it, and saves it as a CSV file.
    """

    # Create the directory if it doesn't exist
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    # Download the file from the URL
    temp_dat_path = "data/temp_movies.dat"  # Temporary file for the .dat file
    with requests.get(url, timeout=timeout) as r:
        r.raise_for_status()  # Raise an error for bad HTTP responses
        with open(temp_dat_path, "wb") as f:
            f.write(r.content)

    # Load the data into a pandas DataFrame
    # The original file uses "::" as a separator
    try:
        df = pd.read_csv(
            temp_dat_path, sep="::", header=None, engine="python", encoding=encoding
        )
        df.columns = ["id", "title", "genre"]  # Adjust column names

        # Save the DataFrame as a CSV file
        df.to_csv(file_path, index=False)
        print(f"Data saved as CSV at {file_path}")
        print(df.head())  # Display the first 5 rows

        # Remove the temporary .dat file
        os.remove(temp_dat_path)

    except Exception as e:
        print(f"Error reading or processing the file: {e}")

    return "success"


# Run the extract function
if __name__ == "__main__":
    extract()
