import pandas as pd
import matplotlib.pyplot as plt
# import hajj

# eth_filtered_aviation_output NOMBRE CVS EN PYCHARM CAMBIAR VS CODE
class tabla():

    def __init__(self,airport,year):
        self.airport = airport
        self.year = year

        path_aviation = 'eth_filtered_aviation_output.csv'
        path_aviation = pd.read_csv(path_aviation)

        #meses a numero
        path_aviation['Month'] = pd.to_datetime(path_aviation['Month'], format='%B').dt.month

        #meses para dividir años
        month_sequence = path_aviation[['Month']].drop_duplicates().reset_index(drop=True)
        month_sequence['year'] = (month_sequence['Month'] == 1).cumsum() + (year-1)

        #junatr
        path_aviation=path_aviation.merge(month_sequence, on='Month', how='left')
        #numero vuelos
        counter = path_aviation.groupby(['year', 'Month', 'AirportCode']).size().reset_index(name='flights')

        airport_code = self.airport
        subset = counter[counter['AirportCode'] == airport_code]

        plt.figure(figsize=(12, 6))
        for year in subset['year'].unique():
            year_data = subset[subset['year'] == year]
            plt.plot(year_data['Month'], year_data['flights'], label=str(year))

        plt.title(f"Flights from {airport} to SA {year}")
        plt.xlabel("Month")
        plt.ylabel("Flights")
        plt.legend()
        plt.show()


NL_2010=tabla("NL",2010)
#EG_2016=tabla("EG",2016)