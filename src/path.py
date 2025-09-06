'''
#       Sesion 4: For and while loops
#       Andrés Rodríguez Cantú & Ethiel Favila Alvarado
#
#       Copyright (C) Tecnológico de Monterrey
#
#       File: src/path.py
#
#       Created:                09/05/2025
#       Last Modified:          09/05/2025
'''
import sys
import os 
from dotenv import load_dotenv
load_dotenv()

# Environment Variables Paths
DATARUSH_PATH = os.getenv("DATARUSH_PATH")
DB_AVIATION = os.getenv("DB_AVIATION")
DB_HAJJ = os.getenv("DB_HAJJ")
DB_POPULATION = os.getenv("DB_POPULATION")

# Paths from DataRush organizers
countries = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", DATARUSH_PATH, "countries.csv")
global_holidays = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", DATARUSH_PATH, "global_holidays.csv")
monthly_sales = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", DATARUSH_PATH, "monthly_sales.csv") # Not used

# Paths for aviation 
aviation_2010 = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", DB_AVIATION, "2010.xlsx")
aviation_2011 = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", DB_AVIATION, "2011.xlsx")
aviation_2012 = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", DB_AVIATION, "2012.xlsx")
aviation_2013 = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", DB_AVIATION, "2013.xlsx")
aviation_2014 = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", DB_AVIATION, "2014.xlsx")
aviation_2015 = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", DB_AVIATION, "2015.xlsx")
aviation_2016 = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", DB_AVIATION, "2016.xlsx")
aviation_2017 = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", DB_AVIATION, "2017.xlsx")
aviation_2018 = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", DB_AVIATION, "2018.xlsx")
aviation_2019 = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", DB_AVIATION, "2019.xlsx")

# Paths for Hajj Muslim Pilgrimage
# 1995-2015 data is in pilgrimage_2010 for consistency
pilgrimage_2010 = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", DB_HAJJ, "pilgrimage1.xlsx")
pilgrimage_2016 = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", DB_HAJJ, "pilgrimage2.xlsx")
# 2017-2018 combined data
pilgrimage_2018 = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", DB_HAJJ, "pilgrimage3.xlsx")
pilgrimage_2019 = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", DB_HAJJ, "pilgrimage4.xlsx")

# Paths for Muslim Population Census
population_census = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", DB_POPULATION, "population_census.xlsx")
