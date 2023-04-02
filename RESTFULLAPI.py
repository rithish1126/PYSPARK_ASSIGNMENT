from flask import Flask,jsonify, render_template
from DATACREATION import gettingdatafromapi,creatingcsvfromjson,spark
import pandas as pd
import json
import csv
import os
import requests
app = Flask(__name__)
def createdataframefromcsv():
    df = spark.read.format("csv").option("header","true").load("output.csv")#convert the csv to a data frame to run queryies on
    df.printSchema()
    df.show()
    return df
@app.route('/')#the home route displays a menu of all the information to gain
def home():
     return jsonify({'/show_csv_data' : " SHOW CSV FILE DATA",
                     '/show_api_data' : " SHOW API RETURNED DATA",
                     '/mk_csv_df':" MAKE A CSV FILE FROM THE DATA RETURNED FROM API",
                     '/get_most_affected':" MOST AFFECTED STATE( total death/total covid cases)",
                     '/get_least_affected':" LEAST AFFECTED STATE ( total death/total covid cases)",
                     '/highest_covid_cases':" STATE WITH THE HIGHEST COVID CASES",
                     '/least_covid_cases':" STATE WITH THE LEAST COVID CASES",
                     '/total_cases':" TOTAL CASES",
                     '/handle_well':" STATE THAT HANDLED THE MOST COVID CASES EFFICIENTLY( total recovery/ total covid cases)",
                     '/least_well':" STATE THAT HANDLED THE MOST COVID CASES LEAST EFFICIENTLY( total recovery/ total covid cases)"
                     })
@app.route('/show_csv_data')
def show_all(): #Show all the data from the csv file
    try: #tries to see if the file is available if not returns a "File missing message"
        with open("/Users/devarithish/Desktop/PYSPARK ASSIGNMENT/output.csv") as file:
            reader = csv.reader(file)
            header = next(reader)
            return render_template("home.html", header=header, rows=reader)
    except:
        return  "<h1>:(ERROR)FILE MISSING</h1>"
@app.route('/show_api_data')    
def show_api_data(): #Function to directly fetch data from the api to display to user
    url = "https://covid-19-india2.p.rapidapi.com/details.php"

    headers = {
        "X-RapidAPI-Key": "ADD-KEY",
        "X-RapidAPI-Host": "covid-19-india2.p.rapidapi.com"
    }

    response = requests.request("GET", url, headers=headers)
    csv_columns = ['slno','state','confirm','cured','death','total']
    x =response.json()
    return x
@app.route('/mk_csv_df')#Function to get data from the api and create a csv file with data if the file doesn't exist
def make_csv():
    json_object=gettingdatafromapi()#api data with last two records gone
    creatingcsvfromjson(json_object)#creating the csv file from the dictionary returned
    return  "<h1>:FILE CREATED</h1>"#return text once file is created
@app.route('/get_most_affected')
def get_most_affected_state():#function to get the most affected state from the data frame
    if not os.path.exists("/Users/devarithish/Desktop/PYSPARK ASSIGNMENT/output.csv") :
        return  "<h1>:(ERROR)FILE MISSING</h1>"
    else:
        df=createdataframefromcsv()#create the data fram of the csv file to work on
        df.createOrReplaceTempView("TABLE")#get a temporary view of the dataframe to write sql queries on
        ans=spark.sql("SELECT state, death/confirm AS affected FROM table").orderBy("affected",ascending=False).select("state").limit(1).collect()
        state=ans[0][0]#select the state and use the formula in the question and order in descending and only return the top value to the variable
        return jsonify({'Most affected state ': state}) #print the answer
@app.route('/get_least_affected')
def get_least_affected_state():#function to get least affected state from the data frame
    
     if not os.path.exists("/Users/devarithish/Desktop/PYSPARK ASSIGNMENT/output.csv") :
        return  "<h1>:(ERROR)FILE MISSING</h1>"
     else:
        df=createdataframefromcsv()
        df.createOrReplaceTempView("TABLE")
        sub=spark.sql("SELECT state, death/confirm AS affected FROM TABLE").orderBy("affected",ascending=True).select("state").limit(1).collect()
        state=sub[0][0]#the same thing as above but ordered in ascending order
        return jsonify({'Least affected state ': state})
@app.route('/highest_covid_cases')
def highest_covid_cases():
    if not os.path.exists("/Users/devarithish/Desktop/PYSPARK ASSIGNMENT/output.csv") :
        return  "<h1>:(ERROR)FILE MISSING</h1>"
    else:
        df=createdataframefromcsv()
        df.createOrReplaceTempView("TABLE")
        sub=df.orderBy("confirm",ascending=False).select("state","confirm").limit(1).collect()
        state=sub[0][0] #order in descending by the confirmed covid cases and then return the top record to the variable
        confirmed=sub[0][1]
        return jsonify({'State with highest cases ': state,'Confirmed Covid cases':confirmed})
@app.route('/least_covid_cases')
def least_covid_cases():
    if not os.path.exists("/Users/devarithish/Desktop/PYSPARK ASSIGNMENT/output.csv") :
        return  "<h1>:(ERROR)FILE MISSING</h1>"
    else:
        df=createdataframefromcsv()
        df.createOrReplaceTempView("TABLE")
        sub=df.orderBy("confirm",ascending=True).select("state","confirm").limit(1).collect()
        state=sub[0][0]#order in ascending by the confirmed covid cases and then return the top record to the variable
        confirmed=sub[0][1]
        return jsonify({'State with the least cases':state,'Confirmed Covid cases':confirmed})
@app.route('/total_cases')
def total_cases():
    if not os.path.exists("/Users/devarithish/Desktop/PYSPARK ASSIGNMENT/output.csv") :
        return  "<h1>:(ERROR)FILE MISSING</h1>"
    else:
        df=createdataframefromcsv()
        df.createOrReplaceTempView("TABLE")
        sub=spark.sql("SELECT SUM(total) AS TOTALCASES FROM TABLE").collect()#SUM function to sum all the numbers under the totalcases field and return that
        return jsonify({'Total cases':sub[0][0]})
@app.route('/handle_well')
def state_handle_well():
    if not os.path.exists("/Users/devarithish/Desktop/PYSPARK ASSIGNMENT/output.csv") :
        return  "<h1>:(ERROR)FILE MISSING</h1>"
    else:
        df=createdataframefromcsv()
        df.createOrReplaceTempView("TABLE")#USE THE FORMULA GIVEN IN QUESTION AND USE EFFICIANCY TO ORDER BY DESCENDING ORDER AND RETURN THE TOP MOST RECORD
        sub=spark.sql("SELECT STATE , CURED/CONFIRM AS efficiancy from table").orderBy("efficiancy",ascending=False).limit(1).collect()
        return jsonify({'WELL HANDLED STATE':sub[0][0],'Efficiancy':sub[0][1]})
@app.route('/least_well')
def state_least_well():
    if not os.path.exists("/Users/devarithish/Desktop/PYSPARK ASSIGNMENT/output.csv") :
        return  "<h1>:(ERROR)FILE MISSING</h1>"
    else:
        df=createdataframefromcsv()
        df.createOrReplaceTempView("TABLE")#USE THE FORMULA GIVEN IN QUESTION AND USE EFFICIANCY TO ORDER BY ASCENDING ORDER AND RETURN THE TOP MOST RECORD
        sub=spark.sql("SELECT STATE , CURED/CONFIRM AS efficiancy from table").orderBy("efficiancy",ascending=True).select("state","efficiancy").limit(1).collect()
        return jsonify({'LEAST WELL HANDLED STATE':sub[0][0],'Efficiancy':sub[0][1]})
if __name__ == '__main__':
    app.run(port=8000,debug=True)





