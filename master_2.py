import time

from opensearchpy import OpenSearch
import os
import datetime
username = 'admin'
password = 'Elasticsearch123!#'
index_name = 'analysis_record'
http_auth = (username, password)
hosts = 'https://search-dsdgroup6-pt4vzj4nnlkksrgfvv6rmiuxgu.us-east-1.es.amazonaws.com:443'
es = OpenSearch(hosts=hosts, http_auth=http_auth)


def get_documents_by_status():
    try:
        result = es.search(index=index_name, body={
            'query': {
                'terms': {
                    'status': ['new', 'processing']
                }
            } , "size": 20
        })

        # Extract the documents from the response
        documents = [hit for hit in result['hits']['hits']]

        return documents
    except Exception as e:
        print(f"Error: {e}")


def analysis_iterator(year,version,id):
    if  version=="1":
        os.system(f"python3 analysis1_script.py {year} {id}")
    if version == "2":
        os.system(f"python3 analysis2_script.py {year} {id}")
    if version == "3":
        os.system(f"python3 Analysis_1/Analysis_1_script.py {year} {id}")


def main():
    while True:
        all_documents = get_documents_by_status()
        for docs in all_documents:
            version = docs["_source"]["analysisVersion"]
            year = docs["_source"]["analysisYear"]
            if docs["_source"]["status"] == "new":
                print(f"Processing analysis {version} for the year {year}")
                docs["_source"]["status"] = "processing"
                docs["_source"]["processing_timestamp"] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                response = es.index(index=index_name, id=docs["_id"], body=docs["_source"])
                analysis_iterator(year,version, docs["_id"])
        print("Sleeping 10 Seconds")
        time.sleep(10)




if __name__ == '__main__':
    main()