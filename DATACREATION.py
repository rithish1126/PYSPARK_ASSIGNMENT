import requests 
import pandas as pd
import json
import csv
from pyspark.sql import SparkSession


spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate() #SPARK SESSION CREATED TO BE USED IN THE API FILE
def gettingdatafromapi(): #FUNCTION RESPONSIBLE TO GET THE INFORMATION FROM THE API(RAPID API)
    url = "https://covid-19-india2.p.rapidapi.com/details.php" 
    headers = {
        "X-RapidAPI-Key": "ADD-KEY",
        "X-RapidAPI-Host": "covid-19-india2.p.rapidapi.com"
    }
    response = requests.request("GET", url, headers=headers) #RESPONSE OBJECT RECIEVED FROM API
    x =response.json() #CONVERT THE RESPONSE OBJECT TO A DICTIONARY
    x.popitem() #REMOVING THE LAST TWO ENTRIES AS THEY ARE NOT REQUIRED
    x.popitem()
    return x #RETURNING THE DICTIONARY
def creatingcsvfromjson(x): #FUNCTION RESPONSIBLE FOR CREATING A CSV FROM THE DICTIONARY
    with open('output.csv', mode='w', newline='') as file: #OPEN OR MAKE AN OUTPUT.CSV FILE TO STORE THE API DATA
        writer = csv.writer(file) #POINTER TO WRITE DATA IN CSV
        writer.writerow(['slno', 'state', 'confirm','cured','death','total']) #WRITING THE FIRST ROW AS HEADERS
        for person in x.values(): #ITERATING THROUGH THE DICTIONARY AND WRITING THEM TO THE CSV
            writer.writerow([person['slno'], person['state'], person['confirm'],person['cured'],person['death'],person['total']])


