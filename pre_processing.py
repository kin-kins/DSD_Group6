import os
import pandas as pd

def preprocess_csv(input_file, output_file, columns_to_drop,filename):
    data = pd.read_csv(input_file)
    data.drop(columns=columns_to_drop, inplace=True)
    data['country_name'] = filename[:2]
    data.to_csv(output_file, index=False)

directory = 'C:\\Users\\vishy\\OneDrive\\Desktop\\CONCORDIA\\COMP6231\\PROJECT\\'
all_data = []
for filename in os.listdir(directory):
    if filename.endswith('.csv'):
        input_csv = os.path.join(directory, filename)
        output_csv = os.path.join(directory, f"processed_{filename}")
        print(filename[:2])
        columns_to_drop = ['tags', 'comment_count', 'thumbnail_link', 'comments_disabled','ratings_disabled', 'description']
        preprocess_csv(input_csv, output_csv, columns_to_drop,filename)
        df = pd.read_csv(output_csv)
        all_data.append(df)

merged_data = pd.concat(all_data, ignore_index=True)

merged_csv_path = os.path.join(directory, 'preprocessed_single_file_dataset.csv')
merged_data.to_csv(merged_csv_path, index=False)