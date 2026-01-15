import sqlite3
import requests

def create_database():
    # Connect to sqlite database or create a new one
    conn = sqlite3.connect('datausa_analysis.sqlite')
    cursor = conn.cursor()

    # Create tables for cube names, dimensions, hierarchies
    cursor.executescript('''
    DROP TABLE IF EXISTS cubes;
    DROP TABLE IF EXISTS dimensions;
    DROP TABLE IF EXISTS hierarchies;
    DROP TABLE IF EXISTS measures;
    DROP TABLE IF EXISTS cube_dimensions;
    DROP TABLE IF EXISTS cube_measures;
    DROP TABLE IF EXISTS dimension_hierarchies;
    DROP TABLE IF EXISTS comprehensive;
    
    CREATE TABLE IF NOT EXISTS cubes (
        cube_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        cube_name TEXT UNIQUE,
        cube_topic TEXT  
    );
    
    CREATE TABLE IF NOT EXISTS dimensions (
        dimension_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        dimension_name TEXT UNIQUE  
    );
    
    CREATE TABLE IF NOT EXISTS hierarchies (
        hierarchy_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        hierarchy_name TEXT UNIQUE
    );
    
    CREATE TABLE measures (
        measure_id INTEGER PRIMARY KEY,
        measure_name TEXT UNIQUE
    );
    
    CREATE TABLE cube_dimensions (
        cube_id INTEGER,
        dimension_id INTEGER,
        PRIMARY KEY (cube_id, dimension_id),
        FOREIGN KEY (cube_id) REFERENCES cubes(cube_id),
        FOREIGN KEY (dimension_id) REFERENCES dimensions(dimension_id)
    );
    
    CREATE TABLE cube_measures (
        cube_id INTEGER,
        measure_id INTEGER,
        PRIMARY KEY (cube_id, measure_id),
        FOREIGN KEY (cube_id) REFERENCES cubes(cube_id),
        FOREIGN KEY (measure_id) REFERENCES measures(measure_id)
    );
    
    CREATE TABLE dimension_hierarchies (
        dimension_id INTEGER,
        hierarchy_id INTEGER,
        PRIMARY KEY (dimension_id, hierarchy_id),
        FOREIGN KEY (dimension_id) REFERENCES dimensions(dimension_id),
        FOREIGN KEY (hierarchy_id) REFERENCES hierarchies(hierarchy_id)
    );
    
    CREATE TABLE comprehensive (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
        cube_id INTEGER,
        dimension_ids TEXT,
        hierarchy_ids TEXT,
        measure_ids TEXT
    )
    ''')
    conn.commit()
    cursor.close()

def retrieve_and_store_cube_names(api_url):
# Make a GET request to the API
    try:
        response = requests.get(api_url)

        # Check if the request was successful
        if response.status_code == 200:
            # Load the JSON data
            api_data = response.json()

            # Extract cube names
            cube_names = []  # Initialize an empty list
            for cube in api_data['cubes']:
                cube_names.append((cube['name'],))  # Prepare list of tuples

            # Print the cube names
            print("All cube names retrieved.")
            print(len(cube_names))

            # Use executemany to insert all cube names at once
            conn = sqlite3.connect('datausa_analysis.sqlite')
            cursor = conn.cursor()

            cursor.executemany('''INSERT OR IGNORE INTO cubes (cube_name) 
            VALUES (?)''', cube_names)
            conn.commit()
        else:
            print(f"Unable to retrieve data: {response.status_code} - {response.text}")

    except Exception as e:
        print(f"An error occurred: {e}")

