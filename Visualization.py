import requests
import ssl
import folium
import pandas as pd

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# Define a function to fetch the data needed for choropleth map
def choropleth_map(url):
    state_name = []
    state_population = []
    data = {}
    try:
        response = requests.get(url)

        # Check if the request was successful
        if response.status_code == 200:
            # Load the JSON data
            state_data = response.json()['data']  # data is a list of dictionaries

            for state in state_data:
                state_name.append(state['State'])
                state_population.append(state['Population'])
                data['State'] = state_name
                data['Population'] = state_population

            print(data)
            return data

        else:
            print(f"Unable to retrieve data: {response.status_code} - {response.text}")

    except Exception as e:
        print(f"An error occurred: {e}")

# The main choropleth map visualization function
def main():
    url = ("https://api.datausa.io/tesseract/data.jsonrecords?cube="
       "acs_yg_total_population_5&measures=Population&include=Year:2023&drilldowns=State,Year")

    data = choropleth_map(url)

    df = pd.DataFrame(data)

    # Create a map centered around the United States
    m = folium.Map(location=[37.1, -95.7], zoom_start=4)

    # Add a choropleth layer
    folium.Choropleth(
        geo_data='https://raw.githubusercontent.com/PublicaMundi/MappingAPI/master/data/geojson/us-states.json',
        data=df,
        columns=['State', 'Population'],
        key_on='feature.properties.name',
        fill_color='YlGn',
        fill_opacity=0.7,
        line_opacity=0.2,
    ).add_to(m)

    # Save the map to an HTML file
    m.save('choropleth_map.html')

if __name__ == "__main__":
    main()
    print("Main executed successfully.")



