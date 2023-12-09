import pandas as pd
import json

class Category_Analysis:
    def extract_min_and_max_category_count(self,csv_file_path):
        data = pd.read_csv(csv_file_path)
        category_counts = data.groupby('categoryId').size().reset_index(name='counts')
        # print(category_counts)
        max_count = category_counts['counts'].max()
        category_max = category_counts.loc[category_counts['counts'] == max_count, 'categoryId'].values
        # Find the minimum count
        min_count = category_counts['counts'].min()
        category_min = category_counts.loc[category_counts['counts'] == min_count, 'categoryId'].values
        # print(f"Category ID(s) with maximum count ({max_count}): {category_max}")
        # print(f"Category ID(s) with minimum count ({min_count}): {category_min}")
        return category_min,category_max
    def fetch_category_name_from_json(self,json_file_path):

        # Read the JSON file
        with open(json_file_path,'r') as json_file:
            data = json.load(json_file)

        category_min_id = category_min[0]  # Replace with your category min ID
        category_max_id = category_max[0]  # Replace with your category max ID

        titles = {}
        for item in data['items']:
            if item['id'] == str(category_min_id):
                titles['min'] = item['snippet']['title']
            elif item['id'] == str(category_max_id):
                titles['max'] = item['snippet']['title']
        return titles


if __name__ == '__main__':
    csv_path = 'C:/Users/aayan/Desktop/Fall 2023/DSD/Project/DSD Youtube Dataset/preprocessed_single_file_dataset.csv'
    json_path='C:/Users/aayan/Desktop/Fall 2023/DSD/Project/DSD Youtube Dataset/BR_category_id.json'
    category_obj=Category_Analysis()
    category_min,category_max=category_obj.extract_min_and_max_category_count(csv_path)
    category_name=category_obj.fetch_category_name_from_json(json_path)
    print(f"Category name of least preferred video: {category_name.get('min')}")
    print(f"Category name of most preferred video: {category_name.get('max')}")