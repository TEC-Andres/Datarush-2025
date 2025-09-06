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
import os
import json
import pickle
from pathlib import Path
import time

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
        top15_muslim_countries = population_census_df.sort_values(
            by='MuslimPopulation_PctOfPopWhoAreMuslim_pct_2024update', ascending=False
        ).head(15)
        print("Top15 Muslim Countries by Population Percentage:")
        print(top15_muslim_countries[['flagCode', 'MuslimPopulation_PctOfPopWhoAreMuslim_pct_2024update']])

# -----------------------
# Usage example (main)
# -----------------------
if __name__ == "__main__":
    cache_mgr = CacheManager()
    loader = LoadData(cache_manager=cache_mgr)
    # Uncomment to clear the cache at runtime:
    # cache_mgr.clear_cache()

    loader.load_data()

    filterer = FilterData()
    try:
        muslim_countries = filterer.countriesMuslims(loader.population_census_df)

    except Exception as e:
        raise RuntimeError(str(e))

    """
    “muslim-population-by-country-20.xlsl”
    Column flagCode
    With
    “countries.xlsx”
    Column alpha_2 
    """
    loader.countries_df.rename(columns={"name":"country_name"}, inplace=True)
    muslim_with_countries=muslim_countries.merge(loader.countries_df, left_on="flagCode",right_on="alpha_2",how="left")

    """
    “countries.xlsx”
    Column alpha_2 
    With
    “countries.xlsx”
    Column alpha_3
    """
    muslim_with_countries=muslim_with_countries[["country_name", "flagCode", "alpha_2", "alpha_3"]]

    """
    “countries.xlsx”
    Column alpha_3
    With
    “monthly_passengers”
    IS03
    """
    muslim_with_countries_with_flights=muslim_with_countries.merge(loader.monthly_passengers, left_on="alpha_3", right_on="IS03",how="left")
    print(muslim_with_countries_with_flights)
