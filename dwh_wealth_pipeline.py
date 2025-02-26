import dlt
import requests

from src.ingestion_gorila import get_data

def pipeline_test():
    # Specify the URL of the API endpoint
    url = "https://api.github.com/repos/dlt-hub/dlt/issues"
    # Make a request and check if it was successful
    response = requests.get(url)
    response.raise_for_status()

    pipeline = dlt.pipeline(
        pipeline_name='test',
        destination='motherduck',
        dataset_name='test'
    )
    load_info = pipeline.run(
        response.json(),
        table_name="issues",
        write_disposition="replace"
    )
    #print(response.json())
    print(load_info)

def pipeline_gorila():
    data = get_data()
    pipeline = dlt.pipeline(
        pipeline_name='gorila',
        destination='motherduck',
        dataset_name='bronze'
    )
    load_info = pipeline.run(
        data,
        table_name="gorila",
        write_disposition="replace"
    )
    print(load_info)

if __name__ == "__main__":
    #pipeline_test()
    pipeline_gorila()