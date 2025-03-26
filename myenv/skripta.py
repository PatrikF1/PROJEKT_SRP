import pandas as pd

PATH = "/Users/patrik/Downloads/Traffic_Violations.csv"

data = pd.read_csv(PATH, delimiter=',')

# Prvih 5 redaka
print("\nPrvih 5 redaka:\n", data.head())

# Dimenzije skupa podataka
print("\nDimenzije (broj redaka, broj stupaca):", data.shape)

# Broj nedostajućih vrijednosti po stupcu
print("\nNedostajuće vrijednosti po stupcu:\n", data.isna().sum())

# Broj jedinstvenih vrijednosti po stupcu
print("\nBroj jedinstvenih vrijednosti po stupcu:\n", data.nunique())

# Tipovi podataka
print("\nTipovi podataka po stupcu:\n", data.dtypes)

# Frekvencije vrijednosti po stupcu
print("\nFrekvencije vrijednosti po stupcu:")
for column in data:
    print(f"\nStupac: {column}")
    print(data[column].value_counts())
    print("-" * 40) 

# Nazivi stupaca
print("\nNazivi stupaca:\n", data.columns.values)

