# -*- coding: utf-8 -*-
"""
Created on Fri Dec 15 13:37:57 2023

@author: vishy
"""

import pandas as pd
import operator
from enum import Enum
import sys
import json
import os
from elasticsearch import Elasticsearch
from opensearchpy import OpenSearch
from datetime import datetime

username = 'admin'
password = 'Elasticsearch123!#'
index_name = 'analysis_record'
http_auth = (username, password)
hosts = 'https://search-dsdgroup6-pt4vzj4nnlkksrgfvv6rmiuxgu.us-east-1.es.amazonaws.com:443'

es = OpenSearch(hosts=hosts, http_auth=http_auth)


def requirementsDocument(year,Version):
    reqDocument = {
    "processid": datetime.now(),
    "status": "new",
    "timestamp": datetime.now(),
    "analysisVersion": Version,
    "analysisYear": year
    }
    try:
      response = es.index(index=index_name,id=reqDocument["processid"], body=reqDocument)
      print(f"Document inserted successfully. Document ID: {response['_id']}")
    except Exception as e:
       print(f"Error inserting document: {e}")

if __name__ == '__main__':
    version = int(input("Please enter the Analysis Version Number"))
    while(version < 1 or version > 3):
        print("Please enter the Analysis Version Number between 1 and 3")
        version = int(input())
    year = int(input("Please enter year between 2020 and 2023 "))
    while(year < 2020 or year > 2023):
        year = int(input("Please enter year between 2020 and 2023"))
    requirementsDocument(year,version)