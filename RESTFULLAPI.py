
from flask import Flask, jsonify, render_template
from DATACREATION import gettingdatafromapi, creatingcsvfromjson, spark
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank
import csv
import os
import requests
from pyspark.sql.functions import col
from pyspark.sql.functions import regexp_replace
app = Flask(__name__)


def csv_to_data_frame():
    # convert the csv to a data frame to run queries on
    df = spark.read.format("csv").option("header", "true").load("output.csv") 
    # Cleaning but removing "_corrupt_record Column and nUll records"
    df = df.drop('_corrupt_record')
    df = df.where((df.state.isNotNull()) & (df.state != ''))
    df = df.withColumn("confirm", col("confirm").cast("Long")).withColumn("cu\
red", col("cured").cast("Long")).withColumn("death", col("death").cast("Long"))
    # Remove stars that are present in the data
    df = df.select("slno", "state", "confirm", "cured", "death", "total")
    df = df.withColumn('state', regexp_replace('state', '\*', ""))
    df.show()
    return df
# the home route displays a menu of all the information to gain
# App Route


@app.route('/')  
def home():
    return jsonify({'/show_csv_data': " SHOW CSV FILE DATA",
                    '/show_api_data': " SHOW API RETURNED DATA",
                    '/mk_csv_df': " MAKE A CSV FILE FROM THE DATA RETURNED FR\
OM API",
                    '/get_most_affected': " MOST AFFECTED STATE( total death/\
total covid cases)",
                    '/get_least_affected': " LEAST AFFECTED STATE ( total \
death/total covid cases)",
                    '/highest_covid_cases': " STATE WITH THE HIGHEST COVID \
CASES",
                    '/least_covid_cases': "STATE WITH THE LEAST COVID CASES",
                    '/total_cases': "TOTAL CASES",
                    '/handle_well': "STATE THAT HANDLED THE MOST COVID CASES \
EFFICIENTLY( total recovery/ total covid cases)",
                    '/least_well': "STATE THAT HANDLED THE MOST COVID CASES \
LEAST EFFICIENTLY( total recovery/ total covid cases)"
                    })


@app.route('/show_csv_data')
# Show all the data from the csv file
def show_data_from_csv(): 
    # tries to see if the file is available if not returns a "File missing
    # message"
    try: 
        # Opens the csv file and returns data with the html template
        with open("output.csv") \
         as file:
            reader = csv.reader(file)
            header = next(reader)
            return render_template("home.html", header=header, rows=reader)
    except FileNotFoundError:
        return "<h1>:(ERROR)FILE MISSING</h1>"
    

@app.route('/show_api_data')    
# Function to directly fetch data from the api to display to user
def show_api_data(): 
    url = "https://covid-19-india2.p.rapidapi.com/details.php"

    headers = {
        "X-RapidAPI-Key": "43ebc47e8bmsh166b24698835233p14c901jsn0e9e1cccde20",
        "X-RapidAPI-Host": "covid-19-india2.p.rapidapi.com"
    }

    response = requests.request("GET", url, headers=headers)
    x = response.json()
    return x

# Function to get data from the api and create a csv file with data if the 
# file doesn't exist


@app.route('/mk_csv_df')
def make_csv():
    # api data with last two records gone
    json_object = gettingdatafromapi()
    # creating the csv file from the dictionary returned
    creatingcsvfromjson(json_object)
    # return text once file is created
    return "<h1>:FILE CREATED</h1>"


@app.route('/get_most_affected')
# function to get the most affected state from the data frame
def get_most_affected_state():
    if not os.path.exists("output.csv"):
        return "<h1>:(ERROR)FILE MISSING</h1>"
    else:
        # create the data frame of the csv file to work on
        df = csv_to_data_frame()
        df.createOrReplaceTempView("TABLE")
        # select columns we are interested in
        sub = spark.sql("SELECT state, death/confirm AS affected FROM TABLE")
        # Order by affected
        windowSpec = Window.orderBy(sub["affected"].desc())
        # Add ranks to the records using dense rank
        df_with_rank = sub.withColumn("dense_\
rank", dense_rank().over(windowSpec))
        # Get the records having rank 1
        top_record = df_with_rank.filter(df_with_rank["dense_\
rank"] == 1).collect()[0][0]
        print(type(df))
        return jsonify({'Most affected state ': top_record})
        

@app.route('/get_least_affected')
# Function to get least affected state from the data frame
def get_least_affected_state():
    
    if not os.path.exists("output.csv"):
        return "<h1>:(ERROR)FILE MISSING</h1>"
    else:
        # create the data fram of the csv file to work on
        df = csv_to_data_frame()
        df.createOrReplaceTempView("TABLE")
        # select columns we are interested in
        sub = spark.sql("SELECT state, death/confirm AS affected FROM TABLE")
        # Order by affected
        windowSpec = Window.orderBy(sub["affected"].asc())
        # Add ranks to the records using dense rank
        df_with_rank = sub.withColumn("dense\
_rank", dense_rank().over(windowSpec))
        # Get the records having rank 1
        top_record = df_with_rank.filter(df_with_rank["dense\
_rank"] == 1).collect()[0][0]
        print(type(df))
        return jsonify({'Least affected state ': top_record})
    

@app.route('/highest_covid_cases')
def highest_covid_cases():
    if not os.path.exists("output.csv"):
        return "<h1>:(ERROR)FILE MISSING</h1>"
    else:
        # create the data fram of the csv file to work on
        df = csv_to_data_frame()
        df.createOrReplaceTempView("TABLE")
        # select columns we are interested in
        sub = spark.sql("SELECT state, confirm FROM TABLE")
        # Order by confirmed cases
        windowSpec = Window.orderBy(sub["confirm"].desc())
        # Adding ranks to ordered records
        df_with_rank = sub.withColumn("dense\
_rank", dense_rank().over(windowSpec))
        state = df_with_rank.filter(df_with_rank["dense\
_rank"] == 1).collect()[0][0]
        confirm = df_with_rank.filter(df_with_rank["dense\
_rank"] == 1).collect()[0][1]
        return jsonify({'State with highest cases ': state, 'Confirmed Covid\
cases': confirm})


@app.route('/least_covid_cases')
def least_covid_cases():
    if not os.path.exists("output.csv"):
        return "<h1>:(ERROR)FILE MISSING</h1>"
    else:
        # create the data fram of the csv file to work on
        df = csv_to_data_frame()
        df.createOrReplaceTempView("TABLE")
        # select columns we are interested in
        sub = spark.sql("SELECT state, confirm FROM TABLE")
        # Order by confirmed cases
        windowSpec = Window.orderBy(sub["confirm"].asc())
        # Adding ranks to ordered records
        df_with_rank = sub.withColumn("dense_\
rank", dense_rank().over(windowSpec))
        state = df_with_rank.filter(df_with_rank["dense\
_rank"] == 1).collect()[0][0]
        confirm = df_with_rank.filter(df_with_rank["dense\
_rank"] == 1).collect()[0][1]
        return jsonify({'State with highest cases ': state, 'Confirmed \
Covid cases': confirm})
    

@app.route('/total_cases')
def total_cases():
    if not os.path.exists("output.csv"):
        return "<h1>:(ERROR)FILE MISSING</h1>"
    else:
        df = csv_to_data_frame()
        df.createOrReplaceTempView("TABLE")
        # SUM function to sum all the numbers under the totalcases field and 
        # return that
        sub = spark.sql("SELECT SUM(total) AS TOTALCASES FROM TABLE").collect()
        return jsonify({'Total cases': sub[0][0]})
    

@app.route('/handle_well')
def state_handle_well():
    if not os.path.exists("output.csv"):
        return "<h1>:(ERROR)FILE MISSING</h1>"
    else:
        # create the data fram of the csv file to work on
        df = csv_to_data_frame()
        df.createOrReplaceTempView("TABLE")
        # select columns we are interested in
        sub = spark.sql("SELECT STATE , CURED/CONFIRM \
AS efficiancy from TABLE")
        # Order by efficiancy
        windowSpec = Window.orderBy(sub["efficiancy"].desc())
        # Adding ranks to ordered records
        df_with_rank = sub.withColumn("dense\
_rank", dense_rank().over(windowSpec))
        # Get the records having rank == 1
        top_record = df_with_rank.filter(df_with_rank["dense\
_rank"] == 1).collect()[0][0]
        
        return jsonify({'MOST WELL HANDLED STATE': top_record})

       
@app.route('/least_well')
def state_least_well():
    if not os.path.exists("output.csv"):
        return "<h1>:(ERROR)FILE MISSING</h1>"
    else:
        # create the data fram of the csv file to work on
        df = csv_to_data_frame()
        df.createOrReplaceTempView("TABLE") 
        # select columns we are interested in
        sub = spark.sql("SELECT STATE , CURED/CONFIRM \
AS efficiancy from TABLE")
        # Order by efficiancy
        windowSpec = Window.orderBy(sub["efficiancy"].asc())
        # Adding ranks to ordered records
        df_with_rank = sub.withColumn("dense\
_rank", dense_rank().over(windowSpec))
        # Get the records having rank == 1
        top_record = df_with_rank.filter(df_with_rank["dense\
_rank"] == 1).collect()[0][0]
        return jsonify({'LEAST WELL HANDLED STATE': top_record})


if __name__ == '__main__':
    app.run(port=8000, debug=True)








