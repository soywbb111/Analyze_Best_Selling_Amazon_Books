import pandas as pd
# Rest of the file go here...
# 52 books best sellers in 25 Sep 2025.
df = pd.read_csv("data/raw/52books_online_results.csv")
#print(df.head()) #5 rows
print(df.shape) #rows, columns
print(df.info()) #name, data type of columns
print(df.describe()) #get like max, avg, min

#clean data before running an analysis - practice

#drop duplicate
df.drop_duplicates(inplace=True)

#renaming columns 
df.rename(columns={"Name":"Title", "User Rating": "Rating", "Publisher_year":"Publication Year"}, inplace=True)

#covert the data type


