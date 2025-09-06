#pip install hijridate (para instalar calendario musulman a Gregoriano)
import hijridate
from datetime import date

from hijridate import Hijri
from datetime import timedelta
class Hajj():
    def __init__(self, year):
        self.year = year
        self.start_date, self.end_date = self.next_hajj(year)

    def next_hajj(year):

        example_hijiri_year= 1446
        example_normal_year= 2025
        #diferencias entre años
        difference=example_normal_year-example_hijiri_year
        #print(difference)
        #año hijiri
        hijri_year=year-difference
        hajj_date = Hijri(hijri_year, 12, 8).to_gregorian()
        #print(hajj_date)
        try:
            hajj_date = Hijri(hijri_year,12,8).to_gregorian()
            end_hajj_date=hajj_date + timedelta(days=5)
            print(hajj_date," to ",end_hajj_date)
            return hajj_date, end_hajj_date

        except:
            return f"Dios nos ampare, no encontramos {year} :("

    """
    for i in range(2010,2020):
        print(i,": ",end="")
        next_hajj(i)"""
    def Hajj_year_month(self):
        if self.start_date:
            return self.start_date.month
        else:
            print(f"Lloro: {e}")
            return None
mes=Hajj(2010)
print(mes.Hajj_year_month())


