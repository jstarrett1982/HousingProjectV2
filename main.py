import pandas as pd
from random import randint
from files import *

housing = pd.read_csv(housing_path)
income = pd.read_csv(income_path)
zip = pd.read_csv(zip_path)

print("_____________________________________________")
# getting rid of rows with bad guids
housing = housing[housing["guid"].map(lambda x: len(str(x)) != 4)]
indices_a = housing.index

zip["citystate"]= zip["city"] + zip["state"]
print(zip)

income = income[income["guid"].map(lambda x: len(str(x)) != 4)]
indices_b = income.index

zip = zip[zip["guid"].map(lambda x: len(str(x)) != 4)]
indices_c = zip.index

for row in indices_a:
    if not housing.housing_median_age[row].isnumeric():
        housing.housing_median_age[row] = randint(10, 50)

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

housing.drop('zip_code', inplace=True, axis=1)

for row in indices_b:
    if not income.median_income[row].isnumeric():
        income.median_income[row] = randint(100000, 750000)

income.drop('zip_code', inplace=True, axis=1)




badzip = zip[zip["zip_code"].map(lambda x: len(str(x)) <= 4)]
print(badzip)

goodzip = zip[zip["zip_code"].map(lambda x: len(str(x)) > 4)]
print(goodzip)

badzip_indices = badzip.index
print(badzip_indices)

bad_city_state = []
for i in badzip_indices:
    bad_city_state.append(f"{badzip.city[i]}{badzip.state[i]}")
print(bad_city_state)

for i in badzip_indices:
    tempvar= (f"{badzip.city[i]}{badzip.state[i]}")
    for j in zip:
       print(zip.citystate.iloc[j])
       # if zip.citystate == tempvar:
            #print(zip.zip)


#bad_county_dict = {bad_county}
#for i in badzip_indices:
    #bad_county_dict.append(badzip.county[i]:index)
#print(bad_county)

#bad_county_df = badzip["county"]
#print(bad_county_df)

#total_county_df = zip["county"]
#print(total_county_df)
#list_indices=[]
#for item in badzip_indices:
    #badzip.county[item] == item
    #list_ind = goodzip.zip_code[goodzip.county.str.contains(item)]
    #if goodzip.county.str.contains(item):
        #badzip.replace(item, list_ind)
        #continue

#print(badzip)



#this one works, sort of
#list_indices=[]
#for item in bad_county:
    #list_ind = goodzip.zip_code[goodzip.index[goodzip.county.str.contains(item)]]
    #print(list_ind.to_string(index=True))
    #list_indices.append(list_ind.to_string(index=True))

#print(list_indices)




# for j in indices_c:
# zip.county[j].isin([bad_county])
# list_indices.append.[j]index

#print("Beginning import")
#print("Cleaning Housing File data")
#print( (num_house_records) "records imported into the database")
#print("Cleaning Income File data")
#print((num_income_records) "records imported into the database")
#print("Cleaning ZIP File data")
#print((num_zip_records) "records imported into the database")
#print("Import completed")

#print("Beginning validation")
#print("Total Rooms:" (input_room))
#print("For locations with more than" (input_room) "rooms, there are a total of" (sum_rooms) "bedrooms.")
#print("ZIP Code: "(input_zip))
#print("The median household income for ZIP code "(input_zip) "is "(median_inc))
#print("Program exiting.")
