import pandas as pd

# Read the CSV file containing all the data
file_path = 'C:\\Users\\jbdou\\Downloads\\preprocessed_single_file_dataset.csv'
df = pd.read_csv(file_path)

# Convert 'trending_date' column to datetime
df['trending_date'] = pd.to_datetime(df['trending_date'], format='%Y-%m-%dT%H:%M:%SZ')

# Create a new column for the month and year
df['month'] = df['trending_date'].dt.month
df['year'] = df['trending_date'].dt.year

# Initialize an empty DataFrame to store results
result_df = pd.DataFrame()

# Iterate through each year and month
for year in df['year'].unique():
    for month in df['month'].unique():
        # Filter data for the specific year and month
        monthly_data = df[(df['year'] == year) & (df['month'] == month)]

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
            'country_name': lambda x: ', '.join(x.unique())
        }).reset_index()

        # Filter for videos that are common in all countries
        common_trending_videos = grouped_common_trending_videos[
            grouped_common_trending_videos['country_name'].apply(lambda x: len(x.split(', '))) == len(
                monthly_data['country_name'].unique())
            ]

        # Select relevant columns
        selected_columns = ['video_id', 'trending_date', 'title', 'channelTitle', 'view_count', 'country_name']

        # Display the common trending videos with selected columns
        common_trending_videos_selected = common_trending_videos[selected_columns]

        # Append results to the overall result DataFrame
        result_df = pd.concat([result_df, common_trending_videos_selected])

# Sort the result by 'trending_date'
result_df = result_df.sort_values(by='trending_date')

# Display the final sorted result
print("\nCommon Trending Videos for Each Whole Month Among All Countries (Sorted by Trending Date):")
print(result_df)

