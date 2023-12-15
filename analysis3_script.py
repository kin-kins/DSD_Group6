import pandas as pd
import json
import sys
import os
from opensearchpy import OpenSearch

pd.options.mode.chained_assignment = None

# Specify your Elasticsearch cluster credentials
username = 'admin'
password = 'Elasticsearch123!#'
index_name = 'analysis3'
http_auth = (username, password)
hosts = 'https://search-dsdgroup6-pt4vzj4nnlkksrgfvv6rmiuxgu.us-east-1.es.amazonaws.com:443'

# Create an Elasticsearch client with authentication
es = OpenSearch(hosts=hosts, http_auth=http_auth)


def get_common_trending_videos_for_year(df, input_year):
    # Convert 'trending_date' column to datetime
    df['trending_date'] = pd.to_datetime(df['trending_date'], format='%Y-%m-%dT%H:%M:%SZ')

    # Create 'year' and 'month' columns
    df['year'] = df['trending_date'].dt.year
    df['month'] = df['trending_date'].dt.month

    # Filter data for the specified year
    year_data = df[df['year'] == input_year]

    # Initialize an empty DataFrame to store results
    result_df = pd.DataFrame()

    # Iterate through each month
    for month in year_data['month'].unique():
        # Filter data for the specific month
        monthly_data = year_data[year_data['month'] == month]

        # Group by video_id and aggregate relevant information
        grouped_common_trending_videos = monthly_data.groupby('video_id').agg({
            'title': 'first',
            'trending_date': 'first',
            'channelId': 'first',
            'channelTitle': 'first',
            'categoryId': 'first',
            'view_count': 'sum',
            'likes': 'sum',
            'dislikes': 'sum',
            'year': 'first',
            'month': 'first',
            'country_name': lambda x: ', '.join(x.unique())
        }).reset_index()

        # Filter for videos that are common in all countries
        common_trending_videos = grouped_common_trending_videos[
            grouped_common_trending_videos['country_name'].apply(lambda x: len(x.split(', '))) == len(
                monthly_data['country_name'].unique())
            ]

        # Add a new column with concatenated values of 'year', 'month', and 'video_id'
        common_trending_videos['id'] = common_trending_videos['year'].astype(str) + '_' + \
                                                        common_trending_videos['month'].astype(str)

        # Select relevant columns
        selected_columns = ['video_id', 'trending_date', 'title', 'channelTitle', 'view_count', 'country_name', 'year',
                            'month', 'id']

        # Display the common trending videos with selected columns
        common_trending_videos_selected = common_trending_videos[selected_columns]

        # Append results to the overall result DataFrame
        result_df = pd.concat([result_df, common_trending_videos_selected])

    # Sort the result by 'trending_date'
    result_df = result_df.sort_values(by='trending_date')

    return result_df


def main():
    try:
        dir = os.path.dirname(__file__)
        df = pd.read_csv('preprocessed_single_file_dataset.csv')

        # Take the input year from the command line argument
        input_year = int(sys.argv[1])
        analysis_id = sys.argv[2]
        id_status = es.get(index="analysis_record", id=analysis_id)["_source"]

        # Get the common trending videos for the specified year
        result_for_year = get_common_trending_videos_for_year(df, input_year)

        # Convert the result to JSON format
        result_json = result_for_year.to_json(orient='records', date_format='iso', default_handler=str)

        # Convert the JSON string to a list
        result_list = json.loads(result_json)

        # Print the list
        print(f"\nCommon Trending Videos for {input_year} (Sorted by Trending Date) in List format:")
        print(result_list)
        for entry in result_list:
            entry["year"] = input_year  # Add the input year to each entry
            try:
                response = es.index(index=index_name, id=entry["id"], body=entry)
                print(f"Document inserted successfully. Document ID: {response['_id']}")
                id_status["status"] = "success"
                es.index(index="analysis_record", id=analysis_id, body=id_status)

            except Exception as e:
                id_status["status"] = "error"
                es.index(index="analysis_record", id=analysis_id, body=id_status)
                print(f"Error inserting document: {e}")


    except IndexError:
        print("Please provide the year as a command line argument.")


if __name__ == "__main__":
    main()