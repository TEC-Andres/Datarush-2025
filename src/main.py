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
from matplotlib.widgets import Slider
from matplotlib import colors as mcolors

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
    # Aviation processing helpers (refactored)
    # -----------------------
    def _extract_codes(self, airport: str, destination: str):
        """Return (airport_country_code, destination_country_code) or (None, None)."""
        airport_code = None
        dest_code = None
        if isinstance(airport, str) and len(airport) == 3 and airport.isalpha():
            info = airport_data.get_airport_by_iata(airport)
            if info:
                airport_code = info[0].get("country_code")
        if isinstance(destination, str) and len(destination) == 3 and destination.isalpha():
            info = airport_data.get_airport_by_iata(destination)
            if info:
                dest_code = info[0].get("country_code")
        return airport_code, dest_code

    def _row_to_result(self, row):
        """Convert a DataFrame row to a normalized result dict or None if codes missing."""
        airport = row.get("Airport")
        destination = row.get("Destination")
        month = row.get("Month") if "Month" in row else None
        airport_code, dest_code = self._extract_codes(airport, destination)
        if airport_code is None or dest_code is None:
            return None
        return {
            "Airport": airport,
            "Destination": destination,
            "AirportCode": airport_code,
            "DestinationCode": dest_code,
            "Month": month
        }

    def process_aviation_df(self, df):
        """Process a given aviation DataFrame (subset) into a list of result dicts."""
        results = []
        for _, row in df.iterrows():
            r = self._row_to_result(row)
            if r:
                results.append(r)
        return results

    def build_filtered_list(self, results):
        """Return list of [DestinationCode, AirportCode, Month] from result dicts."""
        return [
            [r["DestinationCode"], r["AirportCode"], r.get("Month")] for r in results
        ]

    def results_to_dataframe(self, results):
        """Convert result dicts to DataFrame with consistent columns."""
        if not results:
            return pd.DataFrame(columns=["DestinationCode", "AirportCode", "Month"])
        return pd.DataFrame(results)[["DestinationCode", "AirportCode", "Month"]]

    def process_year(self, aviation_data: dict, year: int, head: int = None):
        """Process a single year; returns (filtered_list, filtered_df) or (None, None) if missing. If head is None, process all rows."""
        df_year = aviation_data.get(f"aviation_{year}")
        if df_year is None:
            print(f"[WARN] aviation_{year} not found; skipping.")
            return None, None
        cols = [c for c in ["Airport", "Destination", "Month"] if c in df_year.columns]
        subset = df_year[cols] if head is None else df_year[cols].head(head)
        results = self.process_aviation_df(subset)
        filtered_list = self.build_filtered_list(results)
        filtered_df = self.results_to_dataframe(results)
        print(f"Year {year}: processed {len(filtered_list)} records")
        return filtered_list, filtered_df

    def process_years(self, aviation_data: dict, years, output_dir: str = "_aviation_outputs", head: int = None):
        """Iterate over multiple years, saving one CSV per year; returns dict year->DataFrame. If head is None, process all rows."""
        os.makedirs(output_dir, exist_ok=True)
        year_map = {}
        for year in years:
            flist, fdf = self.process_year(aviation_data, year, head=head)
            if fdf is None:
                continue
            out_path = os.path.join(output_dir, f"filtered_aviation_{year}.csv")
            fdf.to_csv(out_path, index=False)
            print(f"Saved {out_path}")
            year_map[year] = fdf
        return year_map

class GraphData():
    def __init__(self):
        pass

    def _load_filtered_year(self, year: int, directory: str = "_aviation_outputs"):
        """Load a previously filtered aviation CSV for the given year.

        Expects a file named filtered_aviation_{year}.csv with at least the columns:
          - DestinationCode
          - Month (numeric 1-12 or strings convertible to 1-12)

        Returns a DataFrame or raises FileNotFoundError / ValueError on issues.
        """
        path = os.path.join(directory, f"filtered_aviation_{year}.csv")
        if not os.path.exists(path):
            raise FileNotFoundError(f"Filtered aviation file not found: {path}")
        df = pd.read_csv(path)
        required_cols = {"DestinationCode", "Month"}
        missing = required_cols - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns in {path}: {missing}")
        # Coerce Month to integers 1..12 where possible
        df["Month"] = df["Month"].apply(self._coerce_month)
        df = df.dropna(subset=["Month"])  # drop rows where month couldn't be interpreted
        df["Month"] = df["Month"].astype(int)
        return df

    def _coerce_month(self, m):
        if pd.isna(m):
            return None
        # Already numeric
        if isinstance(m, (int, float)):
            if 1 <= int(m) <= 12:
                return int(m)
            return None
        # Try common month name / abbreviation
        if isinstance(m, str):
            ms = m.strip().lower()
            month_map = {
                'jan': 1, 'january': 1,
                'feb': 2, 'february': 2,
                'mar': 3, 'march': 3,
                'apr': 4, 'april': 4,
                'may': 5,
                'jun': 6, 'june': 6,
                'jul': 7, 'july': 7,
                'aug': 8, 'august': 8,
                'sep': 9, 'sept': 9, 'september': 9,
                'oct': 10, 'october': 10,
                'nov': 11, 'november': 11,
                'dec': 12, 'december': 12,
            }
            if ms.isdigit():
                mv = int(ms)
                return mv if 1 <= mv <= 12 else None
            return month_map.get(ms)
        return None

    def plot_year_destination_heatmap(self, start_year=2010, end_year=2019, directory="_aviation_outputs", 
                                      normalize_by="year_total", cmap="viridis", figsize=(12, 8), flag_codes=None, log_scale=False):
        """
        Interactive heatmap (year slider) of destination code frequencies as a grid:
        Rows: Destination codes
        Columns: Months (Jan-Dec)
        Slider: Selects year (start_year to end_year)
        """
        import matplotlib.pyplot as plt
        from matplotlib.widgets import Slider
        import matplotlib.colors as mcolors
        import numpy as np
        import os
        import pandas as pd

        def load_counts(year):
            df = self._load_filtered_year(year, directory)
            if flag_codes is not None:
                normalized = [c.strip().upper() for c in flag_codes if isinstance(c, str) and c.strip()]
                df = df[df['DestinationCode'].str.upper().isin(normalized)]
            counts = (df.groupby(["DestinationCode", "Month"]).size().unstack(fill_value=0))
            for m in range(1, 13):
                if m not in counts.columns:
                    counts[m] = 0
            counts = counts[sorted(counts.columns)]
            return counts

        # Load all years' destination codes to get union for consistent y-axis
        all_dest_codes = set()
        for year in range(start_year, end_year + 1):
            try:
                df = self._load_filtered_year(year, directory)
                if flag_codes is not None:
                    normalized = [c.strip().upper() for c in flag_codes if isinstance(c, str) and c.strip()]
                    df = df[df['DestinationCode'].str.upper().isin(normalized)]
                all_dest_codes.update(df['DestinationCode'].unique())
            except Exception:
                continue
        all_dest_codes = sorted(list(all_dest_codes))

        # Preload counts for all years
        year_counts = {}
        for year in range(start_year, end_year + 1):
            try:
                c = load_counts(year)
                # Reindex to all_dest_codes for consistent rows
                c = c.reindex(all_dest_codes, fill_value=0)
                year_counts[year] = c
            except Exception:
                continue

        # Initial year
        initial_year = start_year
        counts = year_counts[initial_year]

        # Logarithmic transformation if requested
        def log_transform(arr):
            return np.log1p(arr)

        # Color normalization
        if log_scale:
            norm = mcolors.Normalize(vmin=0)
        else:
            norm = mcolors.Normalize(vmin=0)

        # Prepare figure
        fig, ax = plt.subplots(figsize=figsize)
        plt.subplots_adjust(left=0.25, bottom=0.25)

        data = counts.values
        if log_scale:
            data = log_transform(data)

        img = ax.imshow(data, aspect='auto', cmap=cmap, norm=norm)
        ax.set_title(f"Destination Code Frequency - {initial_year}")
        ax.set_xlabel("Month")
        ax.set_xticks(range(12))
        ax.set_xticklabels(["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"])
        ax.set_yticks(range(len(all_dest_codes)))
        ax.set_yticklabels(all_dest_codes)

        cbar = fig.colorbar(img, ax=ax, orientation='vertical', shrink=0.8)
        cbar.set_label('Log Occurrences' if log_scale else 'Occurrences')

        # Slider for year selection
        slider_ax = fig.add_axes([0.25, 0.1, 0.5, 0.03])
        year_slider = Slider(ax=slider_ax, label='Year', valmin=start_year, valmax=end_year, valinit=initial_year, valstep=1)

        # Helper: get Hajj month for each year
        hajj_months = {}
        for year in range(start_year, end_year + 1):
            try:
                hajj_start, _ = next_hajj(year)
                hajj_months[year] = hajj_start.month
            except Exception:
                continue

        # Draw initial jitter line for Hajj month
        def draw_hajj_jitter(ax, hajj_month):
            # Place a vertical line at the month index (0-based)
            ax.axvline(x=hajj_month-1, color='red', linestyle='--', linewidth=2, alpha=0.7, label='Hajj Month')

        # Initial year
        draw_hajj_jitter(ax, hajj_months.get(initial_year, None))

        def update(val):
            y = int(year_slider.val)
            new_data = year_counts[y].values
            if log_scale:
                new_data = log_transform(new_data)
            img.set_data(new_data)
            ax.set_title(f"Destination Code Frequency - {y}")
            # Remove previous hajj lines
            for l in ax.lines:
                l.remove()
            # Draw new hajj line
            hajj_month = hajj_months.get(y, None)
            if hajj_month:
                draw_hajj_jitter(ax, hajj_month)
            fig.canvas.draw_idle()

        year_slider.on_changed(update)

        ax.text(1.05, 1.02, "Grid: Destinations x Months. Slider: Year. Red dashed line = Hajj month.", transform=ax.transAxes, fontsize=9, va='bottom', wrap=True)

        plt.show()
        return fig, year_slider


# -----------------------
# Usage example (main)
# -----------------------
if __name__ == "__main__":
    cache_mgr = CacheManager()
    loader = LoadData(cache_manager=cache_mgr)
    loader.load_data()
    f = FilterData()
    try:
        muslim_countries = f.countriesMuslims(loader.population_census_df)
        print(muslim_countries)
        # Process multiple years; if head is None, process all rows
        #f.process_years(loader.aviation_data, years=range(2010, 2020))
        plotter = GraphData()
        flag_codes = ['EG', 'TR', 'IR', 'PK', 'ID', 'BD', 'IN', 'MY', 'NG', 'DZ', 'MA', 'IQ', 'AF', 'YE', 'SY']
        plotter.plot_year_destination_heatmap(start_year=2010, end_year=2019, normalize_by='year_total', cmap='plasma', flag_codes=flag_codes)
    except Exception as exc:
        print(f"Error during aviation processing: {exc}")