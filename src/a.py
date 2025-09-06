import pandas as pd

df = pd.read_csv('_aviation_outputs/filtered_aviation_2010.csv')
top_15 = df['DestinationCode'].value_counts().head(15)
print("Top 15 most frequent DestinationCodes:")
for code, count in top_15.items():
    print(f"{code}: {count} times")