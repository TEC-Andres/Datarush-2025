#pip install hijridate (para instalar calendario musulman a Gregoriano)
from hijridate import Hijri
from datetime import timedelta

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

def main():
    for i in range(2010,2020):
        print(i,": ",end="")
        next_hajj(i)
main()