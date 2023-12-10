import pandas as pd
import json
import sys
import os
from elasticsearch import Elasticsearch
from opensearchpy import OpenSearch

username = 'admin'
password = 'Elasticsearch123!#'
index_name = 'analysis2'
http_auth = (username, password)
hosts = 'https://search-dsdgroup6-pt4vzj4nnlkksrgfvv6rmiuxgu.us-east-1.es.amazonaws.com:443'
es = OpenSearch(hosts=hosts, http_auth=http_auth)
class Category_Analysis:
    def extract_category_count(self,csv_file_path,year):
        data = pd.read_csv(csv_file_path)
        data['trending_date'] = pd.to_datetime(data['trending_date'], format='%Y-%m-%dT%H:%M:%SZ').dt.tz_localize(None)
        final_data=data[data['trending_date'].dt.year==year]
        category_counts = final_data.groupby('categoryId').size().reset_index(name='counts')
        return category_counts
    def fetch_category_name_from_json(self,json_file_path):

        # Read the JSON file
        with open(json_file_path,'r') as json_file:
            data = json.load(json_file)
        sorted_counts = category_counts.sort_values(by='counts', ascending=False)
        json_dict = {}
        for item in data['items']:
            json_dict[item['id']] = item['snippet']['title']

        list_of_dicts = sorted_counts.to_dict(orient='records')
        for dictionary in list_of_dicts:
            dictionary['year'] = 2022
        for item in list_of_dicts:
            category_id = str(item['categoryId'])  # Convert to string to match keys in json_dict
            if category_id in json_dict:
                item['category_name'] = json_dict[category_id]
            else:
                item['category_name'] = 'Unknown'

        for item in list_of_dicts:
            item['ID'] = f"{item['categoryId']}_{item['year']}"

        print(list_of_dicts)
        for single_dict in list_of_dicts:
            try:
                response = es.index(index=index_name, id=single_dict["ID"], body=single_dict)
                print(f"Document inserted successfully. Document ID: {response['_id']}")
            except Exception as e:
                print(f"Error inserting document: {e}")

if __name__ == '__main__':
    #csv_path = 'C:/Users/aayan/Desktop/Fall 2023/DSD/Project/DSD Youtube Dataset/preprocessed_single_file_dataset.csv'
    #json_path='C:/Users/aayan/Desktop/Fall 2023/DSD/Project/DSD Youtube Dataset/BR_category_id.json'
    dir = os.path.dirname(__file__)
    csv_path = os.path.join(dir, 'preprocessed_single_file_dataset.csv')
    json_path=os.path.join(dir, 'BR_category_id.json')
    year=int(sys.argv[1])
    #year=2023
    category_obj=Category_Analysis()
    category_counts=category_obj.extract_category_count(csv_path,year)
    category_name=category_obj.fetch_category_name_from_json(json_path)
