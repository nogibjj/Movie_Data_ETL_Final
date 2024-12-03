from mylib.extract import extract
from mylib.transform_load import load
from mylib.query import query_join, query_aggregate, query_sort

# Extract
extract()

# Transform & Load
load()


# Query operations
def main_results():
    return {
        "extract_to": extract(),  # Assuming this extracts to a CSV or destination
        "transform_db": load(),  # Loading transformed data into Databricks
        "join": query_join(),  # Perform the join query
        "aggregate": query_aggregate(),  # Perform the aggregation query
        "sort": query_sort(),  # Perform the sorting query
    }


if __name__ == "__main__":
    results = main_results()
    print("Main Results:", results)
