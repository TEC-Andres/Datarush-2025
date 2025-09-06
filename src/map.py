import pandas as pd
import matplotlib.pyplot as plt

class TablaMultiple:
    def __init__(self, airports, start_year):
        self.airports = airports    
        self.start_year = start_year  

        df = pd.read_csv('eth_filtered_aviation_output.csv')

        #month to number
        df['Month'] = pd.to_datetime(df['Month'], format='%B').dt.month

        #count years
        year_counter = start_year - 1
        previous_month = 12
        years = []
        for month in df['Month']:
            if month == 1 and previous_month == 12:
                year_counter += 1
            years.append(year_counter)
            previous_month = month
        df['year'] = years

        #count flights per airport per month per year
        self.counter = df.groupby(['year', 'Month', 'AirportCode']).size().reset_index(name='flights')

        #filter
        self.subset = self.counter[self.counter['AirportCode'].isin(self.airports)]

    def plot(self, years_to_plot=None):
        plt.figure(figsize=(12, 6))

        # Filter years if specified
        data = self.subset
        if years_to_plot:
            data = data[data['year'].isin(years_to_plot)]

        # Plot each airport
        for airport in self.airports:
            airport_data = data[data['AirportCode'] == airport]
            for year in airport_data['year'].unique():
                year_data = airport_data[airport_data['year'] == year]
                if not year_data.empty:
                    plt.plot(
                        year_data['Month'],
                        year_data['flights'],
                        label=f"{airport}-{year}"
                    )

        plt.title("Flights from selected countries to Saudi Arabia")
        plt.xlabel("Month")
        plt.ylabel("Flights")
        plt.xticks(range(1, 13))  
        plt.legend()
        plt.grid(True)
        plt.show()


airports = ['EG', 'TR', 'IR', 'PK', 'ID', 'BD', 'IN', 'MY', 'NG', 'DZ', 'MA', 'IQ', 'AF', 'YE', 'SY']
tabla = TablaMultiple(airports, start_year=2011)

tabla.plot(years_to_plot=[2011])


