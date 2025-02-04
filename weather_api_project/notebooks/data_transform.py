import pandas as pd
import datetime

#Read the weather_data.csv file
df = pd.read_csv("weather_api_project/data/csv/weather_data.csv", header=0)

#Remove spaces and drop null values
df.columns = df.columns.str.strip().dropna(how="any")

#Convert date information to datetime then set "time" as the index
df["time"] = pd.to_datetime(df["time"])
df.set_index("time", inplace=True)

#Calculate monthly medians on temperature, relative humidity, precipitation, and surface pressure for all available months then save as a new DataFrame
monthly_medians = df.resample('M').median()

#Convert the monthly medians DataFrame into a CSV file called "monthly_medians_mg.csv"
monthly_medians.to_csv("monthly_medians_mg.csv")

#Calculate yearly medians on temperature, relative humidity, precipitation, and surface pressure for all available years then save a new DataFrame
yearly_medians = df.resample('Y').median()
print(yearly_medians)

#Reset yearly_medians index and rename time to year
yearly_medians = yearly_medians.reset_index()
yearly_medians = yearly_medians.rename(columns={"time":"year"})
yearly_medians["year"] = yearly_medians["year"].dt.year

print(yearly_medians)

#Remove "Unnamed: 0" column that does not correspond to any desired information from the yearly_medians DataFrame
yearly_medians = yearly_medians.loc[:, ~yearly_medians.columns.str.contains("^Unnamed")]

print(yearly_medians)

#Convert the "yearly_medians" DataFrame into a CSV file called "yearly_medians_mg.csv"
yearly_medians.to_csv("yearly_medians_mg.csv", index=False)

#Convert the br_final.csv file into a DataFrame & set index to "year"
df_br1 = pd.read_csv("weather_api_project/data/csv/br_final.csv")
df_br1["year"] = pd.to_datetime(df_br1["year"], format="%Y")
df_br1["year"] = df_br1["year"].dt.year
df_br1.set_index("year", inplace=True)

#Remove row "2018" due to NaN values in 2/3 (bear and non bear mills) of the desired columns for that row and other null values
df_br1.dropna(inplace=True)

#Drop avg_unemp_perc column due to it containing majority empty values
df_br1.drop(columns="avg_unemp_perc", inplace=True)
print(df_br1)

#Drop other unneeded columns from df_br1 ahead of next calculation
df_br1.drop(columns=["type", "country", "nonbear_thous_hect", "bear_thous_hect", "trees_hect_nonbear", "trees_hect_bear"], inplace=True)
print(df_br1)

#Calculate yearly medians of millions_60kgs_bag, nonbear_mill_trees, bear_mill_trees from df_br1 for all years available for Minas Gerais
df_mg = df_br1[df_br1["subdivision"]=="Minas Gerais"]
minas_df = df_mg.groupby("year")[[
    "million_60kgs_bag",
    "nonbear_mill_trees",
    "bear_mill_trees"
]].median()
print(df_mg, minas_df)

#Convert the minas_df into a CSV file called minas_yr_medians.csv
minas_df.to_csv("minas_yr_medians.csv")

#Prepare the minas_df DataFrame for concatenation by resetting its index and storing the dataframe in a new variable to prevent an empty Dataframe during merge
minas_mer = minas_df.reset_index()
print(minas_mer)

#Merge the minas_mer and yearly_medians DataFrames, drop rows with NaN values which results in only keeping the years 2022, 2023
merged_df = yearly_medians.merge(minas_mer, on="year", how="outer")
merged_df = merged_df.set_index("year")
merged_df = merged_df.loc[[2022, 2023]]
print(merged_df)

#Convert merged_df to CSV file 
merged_df.to_csv("merged_weather_data.csv")
