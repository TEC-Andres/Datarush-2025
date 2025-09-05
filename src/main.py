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
import pandas as pd
import os 
from dotenv import load_dotenv
load_dotenv()

DATARUSH_PATH = os.getenv("DATARUSH_PATH")

a = os.path.join(os.path.dirname(os.path.abspath(__file__)), DATARUSH_PATH, "global_holidays.csv")

class Main():
    def __init__(self):
        pass


if __name__ == "__main__":
    main = Main()
    print(a)
