import pandas as pd
import matplotlib.pyplot as plt

#eth_filtered_aviation_output
class tabla():
    
    path_aviacion='eth_filtered_aviation_output.csv'
    df=pd.read_csv(path_aviacion)
    print(df.head(45))
    print(df.info())

    plt.figure(figsize=(10, 6))
    