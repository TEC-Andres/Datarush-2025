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
            aviation_paths[i] = os.path.normpath(raw_path)

        pilgrimage = {
            'pilgrimage1': pilgrimage_2010,
            'pilgrimage2': pilgrimage_2016,
            'pilgrimage3': pilgrimage_2018,
            'pilgrimage4': pilgrimage_2019
        }
        for key, path in pilgrimage.items():
            pilgrimage[key] = os.path.normpath(path)

        def load_excel(year, path):
            df = pd.read_excel(path)
            print(f"Data {year} Loaded: shape={df.shape}")
            return year, df

        self.aviation_data = {}
        self.pilgrimage_data = {}

        # Load pilgrimage data in parallel
        with ThreadPoolExecutor() as executor:
            pilgrimage_futures = [executor.submit(load_excel, year, path) for year, path in pilgrimage.items()]
            for future in as_completed(pilgrimage_futures):
                year, df = future.result()
                self.pilgrimage_data[year] = df
                
        # Load aviation data in parallel
        with ThreadPoolExecutor() as executor:
            aviation_futures = [executor.submit(load_excel, year, path) for year, path in aviation_paths.items()]
            for future in as_completed(aviation_futures):
                year, df = future.result()
                self.aviation_data[year] = df


if __name__ == "__main__":
    loader = LoadData()
    loader.load_data()