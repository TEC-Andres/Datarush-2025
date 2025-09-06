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
from dates import *
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import os
import json
import pickle
from pathlib import Path
import time
from hijridate import Hijri
from datetime import timedelta
from airports import airport_data
import matplotlib.pyplot as plt
import seaborn as sns

# -----------------------
# Configuration
# -----------------------
CACHE_DIR = Path("__cache__")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_FILE = CACHE_DIR / "data_cache.pkl"
CACHE_META = CACHE_DIR / "data_cache_meta.json"

# -----------------------
# CacheManager
# -----------------------
class CacheManager:
    """
    Responsible for:
      - discovering source file paths (mirrors what LoadData would read)
      - computing mtimes for cache invalidation
      - loading/saving the cached payload (pickled)
      - clearing the cache
    """
    def __init__(self, cache_file=CACHE_FILE, meta_file=CACHE_META, force_reload_env="FORCE_RELOAD"):
        self.cache_file = Path(cache_file)
        self.meta_file = Path(meta_file)
        self.force_reload_env = force_reload_env

    def gather_source_paths(self):
        """
        Mirror the logical sources the loader will read.
        Returns: dict mapping logical names -> absolute file paths (strings)
        """
        source_paths = {}
        # CSVs / single files
        if 'countries' in globals():
            source_paths['countries'] = os.path.normpath(countries)
        if 'global_holidays' in globals():
            source_paths['global_holidays'] = os.path.normpath(global_holidays)
        # population census (original code used hardcoded path)
        source_paths['population_census'] = os.path.normpath("db/muslims/population_census/muslim_population_by_country.xlsx")

        # aviation files for years 2010..2019 (if defined in globals())
        for i in range(2010, 2020):
            if f'aviation_{i}' in globals():
                source_paths[f'aviation_{i}'] = os.path.normpath(globals()[f'aviation_{i}'])
        # pilgrimage files (map to labelled keys)
        pilgrimage_vars = ('pilgrimage_2010','pilgrimage_2016','pilgrimage_2018','pilgrimage_2019')
        for idx, varname in enumerate(pilgrimage_vars, start=1):
            if varname in globals():
                source_paths[f'pilgrimage_{idx}'] = os.path.normpath(globals()[varname])

        return source_paths

    def compute_mtimes(self, source_paths):
        """
        Given a mapping of names->paths, compute modification times (or None if missing).
        """
        mtimes = {}
        for key, path in source_paths.items():
            try:
                mtimes[key] = os.path.getmtime(path)
            except Exception:
                mtimes[key] = None
        return mtimes

    def is_cache_valid(self, current_meta):
        """
        True if cache exists and saved metadata exactly matches current_meta.
        """
        if os.environ.get(self.force_reload_env, "") == "1":
            return False
        if not self.cache_file.exists() or not self.meta_file.exists():
            return False
        try:
            with open(self.meta_file, "r", encoding="utf-8") as f:
                saved_meta = json.load(f)
        except Exception:
            return False
        return saved_meta == current_meta

    def load_cache(self):
        """
        Returns the payload dict saved in the cache file (unpickled).
        """
        with open(self.cache_file, "rb") as f:
            payload = pickle.load(f)
        return payload

    def save_cache(self, payload, source_paths):
        """
        payload: dict to pickle (should contain DataFrames / dicts)
        source_paths: the mapping used to compute metadata
        """
        # Save pickled payload
        with open(self.cache_file, "wb") as f:
            pickle.dump(payload, f, protocol=pickle.HIGHEST_PROTOCOL)
        # Save meta (mtimes) for invalidation
        meta = self.compute_mtimes(source_paths)
        with open(self.meta_file, "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2)
        print(f"Cache saved to {self.cache_file}")

    def clear_cache(self):
        if self.cache_file.exists():
            self.cache_file.unlink()
        if self.meta_file.exists():
            self.meta_file.unlink()
        print("Cache cleared.")

# -----------------------
# LoadData
# -----------------------
class LoadData():
    def __init__(self, cache_manager: CacheManager = None):
        self.countries_df = None
        self.holidays_df = None
        self.aviation_data = {}
        self.pilgrimage_data = {}
        self.population_census_df = None
        self.cache_manager = cache_manager or CacheManager()

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

    def _load_from_cache_payload(self, payload):
        """
        Given the dict returned by CacheManager.load_cache(), populate members.
        """
        self.countries_df = payload.get("countries_df")
        self.holidays_df = payload.get("holidays_df")
        self.population_census_df = payload.get("population_census_df")
        self.aviation_data = payload.get("aviation_data", {})
        self.pilgrimage_data = payload.get("pilgrimage_data", {})
        saved_at = payload.get("saved_at")
        if saved_at:
            print(f"Loaded cached data (saved at {time.ctime(saved_at)})")
        else:
            print("Loaded cached data")

    def load_data(self):
        # Discover sources and compute mtimes
        source_paths = self.cache_manager.gather_source_paths()
        current_meta = self.cache_manager.compute_mtimes(source_paths)

        # If cache valid, load it
        if self.cache_manager.is_cache_valid(current_meta):
            try:
                payload = self.cache_manager.load_cache()
                self._load_from_cache_payload(payload)
                return
            except Exception as e:
                print("Failed to load cache, will reload from sources. Error:", e)

        # Otherwise, load from sources
        # --- single files ---
        countries_path = source_paths.get('countries')
        if countries_path and os.path.exists(countries_path):
            self.countries_df = self._load_csv(countries_path, name="Countries")
        else:
            print("Countries file not found; countries_df left as None.")

        holidays_path = source_paths.get('global_holidays')
        if holidays_path and os.path.exists(holidays_path):
            self.holidays_df = self._load_csv(holidays_path, name="Global Holidays")
        else:
            print("Global holidays file not found; holidays_df left as None.")

        pop_path = source_paths.get('population_census')
        if pop_path and os.path.exists(pop_path):
            self.population_census_df = self._load_excel(pop_path, name="Population Census")
        else:
            print(f"Population census file not found at {pop_path}; population_census_df left as None.")

        # --- aviation files (2010..2019) ---
        aviation_paths = {k: v for k, v in source_paths.items() if k.startswith('aviation_') and v is not None and os.path.exists(v)}
        if aviation_paths:
            self.aviation_data = self._load_excels_parallel(aviation_paths, print_prefix="Aviation Data")
        else:
            print("No aviation files found to load.")

        # --- pilgrimage files ---
        pilgrimage_paths = {k: v for k, v in source_paths.items() if k.startswith('pilgrimage_') and v is not None and os.path.exists(v)}
        if pilgrimage_paths:
            self.pilgrimage_data = self._load_excels_parallel(pilgrimage_paths, print_prefix="Pilgrimage Data")
        else:
            print("No pilgrimage files found to load.")

        # Save cache
        try:
            payload = {
                "countries_df": self.countries_df,
                "holidays_df": self.holidays_df,
                "population_census_df": self.population_census_df,
                "aviation_data": self.aviation_data,
                "pilgrimage_data": self.pilgrimage_data,
                "saved_at": time.time()
            }
            self.cache_manager.save_cache(payload, source_paths)
        except Exception as e:
            print("Warning: failed to save cache:", e)

# -----------------------
# FilterData (unchanged)
# -----------------------
class FilterData():
    def __init__(self):
        pass

    def countriesMuslims(self, population_census_df):
        # Assumes 'population_census_df' has a column 'MuslimPopulation_PctOfPopWhoAreMuslim_pct_2024update' and 'Country'
        if population_census_df is None:
            raise ValueError("population_census_df is None. Did you load data correctly?")
        top16_muslim_countries = population_census_df.sort_values(
            by='MuslimPopulation_PctOfPopWhoAreMuslim_pct_2024update', ascending=False
        ).head(16)
        print("Top16 Muslim Countries by Population Percentage:")
        return top16_muslim_countries[['flagCode']]
    
    def print_aviation_head(self, aviation_data, year):
        df_year = aviation_data.get(f'aviation_{year}')
        if df_year is not None:
            columns = ["Destination", "Airport"]
            if "Month" in df_year.columns:
                columns.append("Month")
            return df_year[columns]
        else:
            print(f"Aviation {year} data not loaded.")

# -----------------------
# Usage example (main)
# -----------------------
if __name__ == "__main__":
    # Class implementation
    cache_mgr = CacheManager()
    loader = LoadData(cache_manager=cache_mgr)
    loader.load_data()
    filter = FilterData()

    # Executable code
    try:
        muslim_countries = filter.countriesMuslims(loader.population_census_df)
        print(muslim_countries)
        aviation_codes = filter.print_aviation_head(loader.aviation_data, 2010)

        # Transform the rank 2 array into a rank 1 array
        if aviation_codes is not None:
            results = []
            for idx, row in aviation_codes.iterrows():
                airport = row["Airport"]
                destination = row["Destination"]
                airport_code = None
                dest_code = None
                month = row["Month"] if "Month" in row else None
                # Get country codes for airport and destination
                if isinstance(airport, str) and len(airport) == 3 and airport.isalpha():
                    airport_info = airport_data.get_airport_by_iata(airport)
                    if airport_info:
                        airport_code = airport_info[0].get("country_code")
                if isinstance(destination, str) and len(destination) == 3 and destination.isalpha():
                    dest_info = airport_data.get_airport_by_iata(destination)
                    if dest_info:
                        dest_code = dest_info[0].get("country_code")
                # Only keep if both codes are not None
                if airport_code is not None and dest_code is not None:
                    results.append({
                        "Airport": airport,
                        "Destination": destination,
                        "Codes": [airport_code, dest_code],
                        "Month": month
                    })
            # Print as a list of [Airport, Destination, Month] tuples
            filtered_list = [
                [
                    airport_data.get_airport_by_iata(entry['Destination'])[0]["country_code"],
                    airport_data.get_airport_by_iata(entry['Airport'])[0]["country_code"],
                    entry["Month"]
                ]
                for entry in results
            ]
            print("Filtered list:")
            print(filtered_list)
            print(f"Length after filtering: {len(filtered_list)}")

            # Also return as a DataFrame
            filtered_df = pd.DataFrame([
                {
                    "DestinationCode": airport_data.get_airport_by_iata(entry['Destination'])[0]["country_code"],
                    "AirportCode": airport_data.get_airport_by_iata(entry['Airport'])[0]["country_code"],
                    "Month": entry["Month"]
                }
                for entry in results
            ])
            print("Filtered DataFrame:")
            print(filtered_df)

            # Save DataFrame to a separate file
            output_path = "_filtered_aviation_output.csv"
            filtered_df.to_csv(output_path, index=False)
            print(f"Filtered DataFrame saved to {output_path}")

    except Exception as e:
        print(aviation_codes)