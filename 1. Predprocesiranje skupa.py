import pandas as pd
import csv

CSV_FILE_PATH = "/Users/patrik/Downloads/Traffic_Violations.csv"

df = pd.read_csv(CSV_FILE_PATH, delimiter=',')
print("CSV size before: ", df.shape)


df = df.dropna() 
df.columns = df.columns.str.lower() 
df.columns = df.columns.str.replace(' ', '_') 

date_column = "date_of_stop"  
if date_column in df.columns:
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce').dt.strftime('%Y-%m-%d')

print("CSV size after: ", df.shape) 
print(df.head()) 


df20 = df.sample(frac=0.2, random_state=1)
df = df.drop(df20.index)
print("CSV size 80: ", df.shape)
print("CSV size 20: ", df20.shape)



df.to_csv(
    "Traffic_Violations_PROCESSED.csv", 
    index=False,
    quoting=csv.QUOTE_NONE,  
    escapechar='\\'  
)

df20.to_csv(
    "Traffic_Violations_PROCESSED_20.csv", 
    index=False,
    quoting=csv.QUOTE_NONE,
    escapechar='\\'
)


df.to_csv("Traffic_Violations_PROCESSED.csv", index=False)
df20.to_csv("Traffic_Violations_PROCESSED_20.csv", index=False) 


