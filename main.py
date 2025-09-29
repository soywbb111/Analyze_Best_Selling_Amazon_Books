import pandas as pd
# Rest of the file go here...
# 52 books best sellers in 25 Sep 2025.
df = pd.read_csv("data/raw/52books_online_results.csv")
# print(df.head()) #5 rows
print(df.shape) #rows, columns
print(df.info()) #name, data type of columns
print(df.describe()) #get like max, avg, min

# clean data before running an analysis - practice

# drop duplicate
df.drop_duplicates(inplace=True)

# renaming columns 
df.rename(columns={"Name":"Title", "User Rating": "Rating", "Publisher_year":"Publication Year"}, inplace=True)

# covert the data type
# run an analysis
author_counts = df["Author"].value_counts()
print(author_counts)

# Q1: authors with high average user ratings
avg_rating_by_author = df.groupby("Author")["Rating"].mean().sort_values(ascending=False)
print(avg_rating_by_author)

# Q2: the genre with the highest average book price
genre_price = df.groupby("Genre")["Price"].mean()
print(genre_price)

# Q3: the genre with the highest user rating
genre_rating = df.groupby("Genre")["Rating"].mean()
print(genre_rating)

# Q4: Is there a correlation between price and user rating?
# Draw graph to see correlation
import seaborn as sns
import matplotlib.pyplot as plt
plt.figure(figsize=(10, 6))
sns.regplot(x ="Price", y = "Rating", data = df, scatter_kws={"alpha":0.5})
plt.title("Correlation Between Price and User Rating")
plt.xlabel("Price (USD)")
plt.ylabel("Rating (Star)")
plt.show()

# Q5: Are recently published books rated higher than older ones?
year_rating = df.groupby("Publication Year")["Rating"].mean()
plt.figure(figsize=(10, 6))
year_rating.plot(kind='line', marker='o')
plt.title("Average Rating by Publication Year")
plt.xlabel("Publication Year (Year)")
plt.ylabel("Rating (Star)")
plt.show()

# Q6: 
books_per_year = df['Publication Year'].value_counts().sort_index()
plt.figure(figsize=(10, 6))
books_per_year.plot(kind='bar')
plt.title("Number of Best-Selling Books by Year")
plt.xlabel("Publication Year (Year)")
plt.ylabel("Number of Books")
import matplotlib.ticker as mticker
ax = plt.gca()
ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
plt.show()



