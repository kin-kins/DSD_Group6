from elasticsearch import Elasticsearch
from opensearchpy import OpenSearch

# Specify your Elasticsearch cluster credentials
username = 'admin'
password = 'Elasticsearch123!#'
index_name = 'analysis_record'
http_auth = (username, password)
hosts = 'https://search-dsdgroup6-pt4vzj4nnlkksrgfvv6rmiuxgu.us-east-1.es.amazonaws.com:443'
# Create an Elasticsearch client with authentication
es = OpenSearch(hosts=hosts, http_auth=http_auth)


def get_documents_by_status():
    try:
        # Using the search API with a query to filter documents
        result = es.search(index=index_name, body={
            'query': {
                'terms': {
                    'status': ['new', 'processing']
                }
            }
        })

        # Extract the documents from the response
        documents = [hit['_source'] for hit in result['hits']['hits']]

        return documents
    except Exception as e:
        print(f"Error: {e}")

# Call the function and print the result
all_documents = get_documents_by_status()
for docs in all_documents:
    print(docs)
# print("All documents:", all_documents)
