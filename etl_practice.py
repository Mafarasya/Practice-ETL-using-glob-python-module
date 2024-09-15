import glob 
import pandas as pd 
import xml.etree.ElementTree as ET 
from datetime import datetime 
import json

# Paths
log_file = "log_file.txt"
target_file = "transformed_data.csv"

# Extraction
# CSV
def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

# JSON
def extract_from_json(file_to_process):
    with open(file_to_process, 'r') as file:
        data = json.load(file)  
    dataframe = pd.DataFrame(data)
    return dataframe

# XML - Using ElementTree
# 'car_model', 'year_of_manufacture', 'price', 'fuel'
def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()

    for car in root.findall('row'):
        car_model = car.find("car_model").text
        year_of_manufacture = car.find("year_of_manufacture").text
        price = car.find("price").text
        fuel = car.find("fuel").text
        dataframe = pd.concat([dataframe, pd.DataFrame([{"car_model": car_model, "year_of_manufacture": year_of_manufacture, "price": price, "fuel": fuel}])], ignore_index=True)

    return dataframe

# Data Extraction
def extract():
    extracted_data = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])

    # # CSV
    for csvfile in glob.glob("./data_source/*.csv"):
        extracted_data = pd.concat([extracted_data, extract_from_csv(csvfile)], ignore_index=True)

    # # JSON
    for jsonfile in glob.glob("./data_source/*.json"):
        extracted_data = pd.concat([extracted_data, extract_from_json(jsonfile)], ignore_index=True)

    # XML
    for xmlfile in glob.glob("./data_source/*.xml"):
        extracted_data = pd.concat([extracted_data, extract_from_xml(xmlfile)], ignore_index=True)

    return extracted_data

# Transformation
def transform(data):
    '''Convert price  -> rounded to 2 decimal places'''

    data['price'] = round(data.price, 2)

    return data

def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file)

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ', ' + message + '\n')

# ETL Process
log_progress("ETL Job Started")

# Log the beginning of the Extraction Process
log_progress("Extract Phase Started")
extracted_data = extract()

# Log the completion of the Extraction Process
log_progress("Extract Phase Ended")

# Log the beginning of the Transformation Process
log_progress("Transformation Phase Started")
transformed_data = transform(extracted_data)
print("Transformed Data")
print(transformed_data)

# # Log the completion of the Transformation Process
log_progress("Transformation Phase Ended")

# Load Process
log_progress("Load Phase Started")
load_data(target_file, transformed_data)

# Log the completion of the Loading Process
log_progress("Load Phase Ended")

log_progress("ETL Job Ended")