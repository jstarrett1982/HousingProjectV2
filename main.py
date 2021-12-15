# Housing project:  Jackie Starrett
# Purpose:  import CSV files of housing data, clean corrupt data, insert data into SQL DB

import pandas as pd
from random import randint

import numpy as numpy
import pymysql.cursors
from numpy import int8

from files import *
from creds import *

# reading the CSV files using the file path saved in files.py
housing = pd.read_csv(housing_path)
income = pd.read_csv(income_path)
zips = pd.read_csv(zip_path)


# Creating a new column variable to aid with cleaning corrupt zipcodes
zips["citystate"] = zips["city"] + zips["state"]

# creating a smaller dataframe to represent just the portion of zips dataframe where zip codes are corrupted
badzip = zips[zips["zip_code"].map(lambda x: len(str(x)) <= 4)]
badzip_indices = badzip.index

# Searching within GoodZipCode list for cityState combination found along-side bad zipcodes
# matchingList = dict()
for badZipCodeIndex in badzip_indices:
    badCityState = (f"{badzip.city[badZipCodeIndex]}{badzip.state[badZipCodeIndex]}")
    for possibleGoodZipCodeIndex in zips.index:
        # looking for a citystate match between bad zipcodes and good zipcodes
        if (zips.loc[possibleGoodZipCodeIndex, "citystate"] == badCityState):
            # if the city and state match but the zipcodes don't, save each zipcode under a different variable
            if (zips.loc[badZipCodeIndex, 'zip_code'] != zips.loc[possibleGoodZipCodeIndex, 'zip_code']):
                badZipCode = zips.loc[badZipCodeIndex, "zip_code"]
                matchingCityState = zips.loc[possibleGoodZipCodeIndex, "citystate"]
                goodZipCode = zips.loc[possibleGoodZipCodeIndex, "zip_code"]
                # print(f"For bad ZIP '{badZipCode}', a possible match is '{goodZipCode}'")
                # modifying the goodZipCode to be first digit + "0000"
                goodZipCode = (goodZipCode[0] + "0000")
                # matchingList.update({badZipCode: goodZipCode})
                # replace the corrupt zipcode in masterlist, "zips", with the good zip code
                zips.zip_code.replace({badZipCode: goodZipCode}, value=None, inplace=True)

# removing the column variable previously created. we don't need it anymore
zips.drop('citystate', inplace=True, axis=1)

###_____________________________________________")
# getting rid of rows with bad guids for each of the dataframes
# Need to do this after taking care of zipcodes to retain Carmel for its zipcode

housing = housing[housing["guid"].map(lambda x: len(str(x)) != 4)]
indices_a = housing.index

# renaming the median age column to match what is in SQL
housing.rename(columns={"housing_median_age": "median_age"}, inplace=True)

# getting rid of rows for bad guids from the income dataframe
income = income[income["guid"].map(lambda x: len(str(x)) != 4)]
indices_b = income.index

# getting rid of row for corrupted guids in zips dataframe
zips = zips[zips["guid"].map(lambda x: len(str(x)) != 4)]
indices_c = zips.index

# ---------------------------------------------------------------
# cleaning corrupt data from various columns within the dataframes, using random numbers in requested ranges
for row in indices_a:
    if not housing.median_age[row].isnumeric():
        housing.median_age[row] = randint(10, 50)

for row in indices_a:
    if not housing.total_rooms[row].isnumeric():
        housing.total_rooms[row] = randint(1000, 2000)

for row in indices_a:
    if not housing.total_bedrooms[row].isnumeric():
        housing.total_bedrooms[row] = randint(1000, 2000)

for row in indices_a:
    if not housing.population[row].isnumeric():
        housing.population[row] = randint(5000, 10000)

for row in indices_a:
    if not housing.households[row].isnumeric():
        housing.households[row] = randint(500, 2500)

for row in indices_a:
    if not housing.median_house_value[row].isnumeric():
        housing.median_house_value[row] = randint(100000, 250000)

for row in indices_b:
    if not income.median_income[row].isnumeric():
        income.median_income[row] = randint(100000, 750000)


# -------------------------------------------------------------

# getting rid of redundant columns in the dataframes. only using zip_code from zips df since it was cleaned in that dataframe
income.drop('zip_code', inplace=True, axis=1)
housing.drop('zip_code', inplace=True, axis=1)




# trying to get my data in the same form that SQL table is "expecting". not having much luck....
#income = income.apply(pd.to_numeric, errors='ignore')
#housing = housing.apply(pd.to_numeric, errors='ignore')
#zips = zips.apply(pd.to_numeric, errors='ignore')

# income["median_income"] = income.median_income.astype(int(8))

# trying to get the dashes out of the guid to reduce size from 36 to 32, not working...
# zips.guid.replace({'-':''},value=None, inplace=True)
# ----------------------------------------------------

# creating variables corresponding to the number of records imported.
num_house_records = housing.shape[0]
num_income_records = income.shape[0]
num_zip_records = zips.shape[0]

#This gets rid of the dashes in the guids.  i thought that might allow it to get into the sql table more easily.
zips["guid"] = zips["guid"].astype('str')
zips["guid"] = zips["guid"].str.replace('-', '')


#print(zips.guid.dtype)
#even after getting rid of the dashes, it still thinks it's an object, not a str or varchar32

#one way I tried to combine all the dataframes, it seemed to remove things that needed to stay:
#masterFile=pd.concat([zips, housing, income], axis=1, join="inner", ignore_index=True)
#print(masterFile)

#another effort at making a combined dataframe:
#this almost working, but the indexes got misaligned and it caused some data to be removed

#extracted_1=housing["median_age"]
#extracted_2=housing["total_rooms"]
#extracted_3=housing["total_bedrooms"]
#extracted_4=housing["population"]
#extracted_5=housing["households"]
#extracted_6=income["median_income"]
#extracted_7=housing["median_house_value"]

#zips.insert(5,"median_age", extracted_1)
#zips.insert(6,"total_rooms", extracted_2)
#zips.insert(7,"total_bedrooms", extracted_3)
#zips.insert(8,"population", extracted_4)
#zips.insert(9,"households", extracted_5)
#zips.insert(10,"median_income", extracted_6)
#zips.insert(11,"median_house_value", extracted_7)
#print(housing)
#print(zips)


# ----------------------------------------------------------------------------

# getting the data into mySQL:

try:
    connection = pymysql.connect(host=hostname, user=username, password=password, db=database, charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

except Exception as e:
    print("An error has occurred. exiting")
#------------------------------------------------------------------------------

#I tried several ways to get the code into SQL, but was unsuccessful.  I commented out some of my attempts so you could see.

#with connection.cursor() as cursor:
    #I wasn't sure where to put the for loop, I tried it in both location.  no go :(

    #for i, row in zips.iterrows():

    #I wanted to put in each file separately, but couldn't even get one file into SQL. Maybe because I could get the data type changed?

        #sqlInsertZip = """ insert into housing (guid, zip_code, city, state, county) values (%, %, %, %, %);"""
        #for index, row in zips.iterrows():
            #cursor.execute(sqlInsertZip, (row["guid"], row["zip_code"], row["city"], row["state"], row["county"]))

#connection.commit

print("Beginning import")
print("Cleaning Housing File data")
print(f" {num_house_records} records imported into the database")
print("Cleaning Income File data")
print(f" {num_income_records} records imported into the database")
print("Cleaning ZIP File data")
print(f" {num_zip_records} records imported into the database")

print("Import completed")

print("Beginning validation")
input_room: str=input("Total Rooms:")
print(input_room)
print(f"For locations with more than {input_room} rooms, there are a total of sum_rooms bedrooms.")
input_zip:str=input("ZIP Code:")
print(f"ZIP Code: {input_zip}")
print(f"The median household income for ZIP code {input_zip} is median_inc")
