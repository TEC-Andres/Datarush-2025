'''
#    
#       Andrés Rodríguez Cantú & Ethiel Favila Alvarado
#
#       Copyright (C) Tecnológico de Monterrey
#
#       File: src/main.py
#
#       Created:                09/05/2025
#       Last Modified:          09/05/2025
'''
from path import *
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np

class LoadData():
    def __init__(self):
        pass

    def load_data(self):
        # Load countries data
        self.countries_df = pd.read_csv(countries)
        print("Countries Data Loaded:")

        # Load global holidays data
        self.holidays_df = pd.read_csv(global_holidays)
        print("Global Holidays Data Loaded:")

        # Load aviation data for each year in parallel
        aviation_paths = {}
        for i in range(2010, 2020):
            raw_path = globals()[f'aviation_{i}']
            sanitized_path = raw_path.replace('src\\..\\', '')
            aviation_paths[i] = sanitized_path

        def load_excel(year, path):
            df = pd.read_excel(path)
            print(f"Aviation Data {year} Loaded: shape={df.shape}")
            return year, df

        self.aviation_data = {}
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(load_excel, year, path) for year, path in aviation_paths.items()]
            for future in as_completed(futures):
                year, df = future.result()
                self.aviation_data[year] = df
        

if __name__ == "__main__":
    loader = LoadData()
    loader.load_data()