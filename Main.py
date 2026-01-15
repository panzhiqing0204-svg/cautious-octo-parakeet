# THIS IS THE MAIN SCRIPT
import sqlite3
import json
import requests
from db_setup import create_database, retrieve_and_store_cube_names
from data_manager import fetch_data_from_api, parse_data, insert_data_to_db
import ssl

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# Function to fetch cube names from the database with ordering
def fetch_cube_names():
    conn = sqlite3.connect('datausa_analysis.sqlite')
    cursor = conn.cursor()
    cursor.execute("SELECT cube_name FROM cubes ORDER BY cube_id")  # Change 'id' to your ordering column
    names = [row[0].strip() for row in cursor.fetchall()]  # Strip whitespace if necessary
    conn.close()  # Close the connection after fetching
    return names

# Function to construct the API URL based on the cube name
def construct_api_url(cube_name):
    base_url = "https://api.datausa.io/tesseract/cubes/"
    return f"{base_url}{cube_name}"

# Main logic to fetch cube names and construct URLs
def main():
    # Step 1: Aet up the database
    create_database()

    # Step 2: Retrieve and store cube names
    api_url = "https://api.datausa.io/tesseract/cubes"
    retrieve_and_store_cube_names(api_url)

    # Step 3: Fetch cube names from the database and construct the URLs
    urls = []
    cube_names = fetch_cube_names()
    # Debugging output to check fetched cube names
    print("Fetched cube names:", cube_names)

    for name in cube_names:
        url = construct_api_url(name)
        urls.append(url)

    # Debugging output to check constructed URLs
    print("Constructed URLs:", urls)

    # Step 4: Processing the data from each URL
    for url in urls:
        api_data = fetch_data_from_api(url) # 4-1: Retrieve data
        if api_data is not None:
            parsed_data = parse_data(api_data) # 4-2: Parse data
            insert_data_to_db(parsed_data)
        else:
            print("No data found fetched from", url)


if __name__ == "__main__":
    main()
    print("Main executed successfully.")
