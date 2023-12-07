# -*- coding: utf-8 -*-
"""
Created on Tue Nov 28 20:44:53 2023

@author: vishy
"""

import pandas as pd
import operator
from enum import Enum

pd.options.mode.chained_assignment = None

class Country(Enum):        #Country
    INDIA = "IN"
    KOREA = "KR"
    GERMANY = "DE"
    FRANCE = "FR"
    BRAZIL = "BR"
    CANADA = "CA"
    BRITAIN = "GB"
    JAPAN = "JP"
    MEXICO = "MX"
    
class Months(Enum):     # Months
    JANUARY = 1
    FEBRUARY = 2
    MARCH = 3
    APRIL = 4
    MAY = 5
    JUNE = 6
    JULY = 7
    AUGUST = 8
    SEPTEMBER = 9
    OCTOBER = 10
    NOVEMBER = 11
    DECEMBER = 12
    
yearToCalculate = 2021   # Year to calculate
class Analysis1:
    def __init__(self,dataset_path=None):
        self.dataset_path = dataset_path
    def processingMostViews(self,df):
        dictionary_viewCount = {'video_id':-1}          # videoId view count
        dictionary_channelId={'channel_id':-1}  # channelId view count
        dictionary_videoId={'video_id':'channel_id'}     # videoId channel id
        dictionary_channelName={'channel_id':'title'}
        for ind in df.index:
            if(df['view_count'][ind] == 'view_count'):
                continue
            dictionary_viewCount.update({df['video_id'][ind]:df['view_count'][ind]})
            dictionary_videoId.update({df['video_id'][ind]:df['channelId'][ind]})
            dictionary_channelName.update({df['channelId'][ind]:df['channelTitle'][ind]})
        for key,value in dictionary_videoId.items():
            if value not in dictionary_channelId.keys():
               dictionary_channelId.update({value:dictionary_viewCount[key]})    
            else:
                viewCount = (int)(dictionary_channelId[value]) + (int)(dictionary_viewCount[key])
                dictionary_channelId.update({value:viewCount})  
        dictionary_channelId.pop('channel_id',None)
        dictionary_channelId.pop('view_count',None)
        dictionary_viewCount.pop('video_id',None)
        dictionary_videoId.pop('video_id',None)
        dictionary_channelName.pop('channel_id',None)
        max_channel_id, views = max(dictionary_channelId.items(), key=lambda x: int(x[1]))
        channelName=dictionary_channelName[max_channel_id]
        return channelName,views
    def processingMonths(self,df,selectedYear):
        df['trending_date'] = pd.to_datetime(df['trending_date'],format='%Y-%m-%dT%H:%M:%SZ').dt.tz_localize(None)
        monthSelected = 1;
        monthDict = {}
        for month in Months:
            monthSelected = month.value;
            selectedRange = df[((df['trending_date'].dt.year >= selectedYear) & (df['trending_date'].dt.month >= monthSelected)) & ((df['trending_date'].dt.year <= selectedYear) & (df['trending_date'].dt.month <= monthSelected))]
            channelName,views = self.processingMostViews(selectedRange)
            channelDict = {channelName:views}
            monthDict[month.name] = channelDict
            
        return monthDict
    def processingCountry(self):
        columnList = list(pd.read_csv(self.dataset_path,skiprows=0,nrows=0).columns)
        df = pd.read_csv(self.dataset_path, names=columnList,skiprows=1)
        countryDict = {}
        Year = yearToCalculate   # YEAR TO BE PROCESSED
        for country in Country:

            countryDf = df[df['country_name']==country.value]
            monthDict = self.processingMonths(countryDf,Year)
            countryDict[country.name]=monthDict
        return countryDict
if __name__ == '__main__':
    solution = Analysis1(dataset_path="C:\\Users\\vishy\\OneDrive\\Desktop\\CONCORDIA\\COMP6231\\PROJECT\\archive(3)\\preprocessed_single_file_dataset.csv")
    resultDict = solution.processingCountry()
    print("\n")
    print("==========================" + str(yearToCalculate) + "==========================")
    print("\n")
    for country in Country:
        print("\n")
        print(country.name)
        print("===========")
        monthDicts = resultDict[country.name]
        for key,value in monthDicts.items():
            print("\n")
            print("MONTH :- " + key)
            channelDict = value
            for channelName,viewCount in channelDict.items():
                print("CHANNEL NAME : " + channelName + " TOTAL VIEWS : " + str(viewCount))




