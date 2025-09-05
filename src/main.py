'''
#       Sesion 4: For and while loops
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

class Main():
    def __init__(self):
        pass

    def load_data(self):
        # Load countries data
        self.countries_df = pd.read_csv(countries)
        print("Countries Data Loaded:")
        print(self.countries_df.head())

        # Load global holidays data
        self.holidays_df = pd.read_csv(global_holidays)
        print("\nGlobal Holidays Data Loaded:")
        print(self.holidays_df.head())

        # Load aviation data for each year
        for i in range(2010, 2020):
            raw_path = globals()[f'aviation_{i}']
            sanitized_path = raw_path.replace('src\\..\\', '')
            aviation_data = pd.read_excel(sanitized_path)
            print(f"\nAviation Data for {i} Loaded:")
            print(aviation_data.head())
        
if __name__ == "__main__":
    main = Main()
    main.load_data()