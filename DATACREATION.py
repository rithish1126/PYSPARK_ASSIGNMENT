import requests 
import csv
from pyspark.sql import SparkSession

# SPARK SESSION CREATED TO BE USED IN THE API FILE
spark = SparkSession.builder.master("local[1]").appName("SparkBy\
Examples.com").getOrCreate() 

# FUNCTION RESPONSIBLE TO GET THE INFORMATION FROM THE API(RAPID API)


def gettingdatafromapi(): 
    url = "https://covid-19-india2.p.rapidapi.com/details.php" 
    headers = {
        "X-RapidAPI-Key": "43ebc47e8bmsh166b24698835233p14c901jsn0e9e1cccde20",
        "X-RapidAPI-Host": "covid-19-india2.p.rapidapi.com"
    }
    # RESPONSE OBJECT RECIEVED FROM API
    response = requests.request("GET", url, headers=headers) 
    # CONVERT THE RESPONSE OBJECT TO A DICTIONARY
    x = response.json() 
    # REMOVING THE LAST TWO ENTRIES AS THEY ARE NOT REQUIRED
    x.popitem() 
    x.popitem()
    # RETURNING THE DICTIONARY
    return x 

# FUNCTION RESPONSIBLE FOR CREATING A CSV FROM THE DICTIONARY


def creatingcsvfromjson(x):
    # OPEN OR MAKE AN OUTPUT.CSV FILE TO STORE THE API DATA
    with open('output.csv', mode='w', newline='') as file:
        # POINTER TO WRITE DATA IN CSV
        writer = csv.writer(file) 
        # WRITING THE FIRST ROW AS HEADERS
        writer.writerow(['slno', 'state', 'confirm', 'cured', 'dea\
th', 'total'])
        # ITERATING THROUGH THE DICTIONARY AND WRITING THEM TO THE CSV
        for person in x.values(): 
            writer.writerow([person['slno'], person['state'], person['conf\
irm'], person['cured'], person['death'], person['total']])


