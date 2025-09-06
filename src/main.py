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
        self.countries_df = None
        self.holidays_df = None
        self.aviation_data = {}
        self.pilgrimage_data = {}
        self.population_census_df = None

    def _load_csv(self, path, name=None):
        df = pd.read_csv(path)
        if name:
            print(f"{name} Data Loaded: shape={df.shape}")
        return df
    
    def _load_excel(self, path, name=None):
        df = pd.read_excel(path)
        if name:
            print(f"{name} Data Loaded: shape={df.shape}")
        return df

    def _load_excels_parallel(self, data_dict, print_prefix):
        def load_excel(key, path):
            df = pd.read_excel(path)
            print(f"{print_prefix} {key} Loaded: shape={df.shape}")
            return key, df
        results = {}
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(load_excel, key, path) for key, path in data_dict.items()]
            for future in as_completed(futures):
                key, df = future.result()
                results[key] = df
        return results

        
    def load_data(self):
        self.countries_df = self._load_csv(countries, name="Countries")
        self.holidays_df = self._load_csv(global_holidays, name="Global Holidays")
        self.population_census_df = self._load_excel("db/muslims/population_census/muslim_population_by_country.xlsx", name="Population Census") # No clue how to load this path from env vars. It just loads bad for no apparent reason

        aviation_paths = {i: os.path.normpath(globals()[f'aviation_{i}']) for i in range(2010, 2020)}
        pilgrimage = {
            'pilgrimage1': os.path.normpath(pilgrimage_2010),
            'pilgrimage2': os.path.normpath(pilgrimage_2016),
            'pilgrimage3': os.path.normpath(pilgrimage_2018),
            'pilgrimage4': os.path.normpath(pilgrimage_2019)
        }

        self.aviation_data = self._load_excels_parallel(aviation_paths, print_prefix="Aviation Data")
        self.pilgrimage_data = self._load_excels_parallel(pilgrimage, print_prefix="Pilgrimage Data")


if __name__ == "__main__":
    loader = LoadData()
    loader.load_data()