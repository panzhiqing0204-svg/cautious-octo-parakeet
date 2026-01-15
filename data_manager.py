# THIS CONTAINS THE FUNCTIONS FOR FETCHING, PROCESSING AND INSERTING API DATA
import requests
import sqlite3
import json

# FUNCTION FOR RETRIEVING DATA
def fetch_data_from_api(url):
    """Fetch data from the API."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        api_data = response.json()
        return api_data
    except requests.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        return None

# FUNCTION FOR PARSING DATA
def parse_data(api_data):
    # Each cube is a dictionary with these keys: 'name', 'caption', 'annotations', 'dimensions', 'measures'
    # Get cube name and cube topic
    cube_name = api_data['name']
    cube_annotations_topic = api_data.get('annotations', {}).get('topic', None)
    cube_dimensions = api_data['dimensions']  # 'dimension' is a list
    cube_measures = api_data['measures']  # 'measures' is a list
    print('Cube name is:', cube_name)
    print('Cube topic is:', cube_annotations_topic)

    # Get the Dimensions
    hierarchies = {}
    for dimension in cube_dimensions:
        dimension_name = dimension['name']  # 'Geography', 'Year'

        # Initialize the list for the dimension if it doesn't exist
        if dimension_name not in hierarchies:
            hierarchies[dimension_name] = []

        for level in dimension['hierarchies']:
            # Append the name of each hierarchy level to the list
            hierarchies[dimension_name].append(level['name'])
    print('Cube hierarchies are:', hierarchies)
    # Prepare the string for insertion
    #dimensions_keys = ', '.join(hierarchies.keys())

    # Get the measures
    measures = []
    for measure in cube_measures:
        measure_name = measure['name']
        measures.append(measure_name)
    print('Cube measures are:', measures)
    # Prepare the string for insertion
    #measures_str = ', '.join(measures)

    ### At this point we have cube name, topic, dimensions, hierarchies, measures ###
    parsed_data = [cube_name, cube_annotations_topic, hierarchies, measures]
    return parsed_data  # Modify as needed

def insert_data_to_db(parsed_data):
    """Insert parsed data into the database."""
    conn = sqlite3.connect('datausa_analysis.sqlite')
    cursor = conn.cursor()

    # 1. Insert cube topic and get cube_id
    try:
        # Retrieve the cube_id based on the cube_name
        cursor.execute("SELECT cube_id FROM cubes WHERE cube_name = ?", (parsed_data[0],))
        result = cursor.fetchone()

        if result is None:
            print(f"Cube '{parsed_data[0]}' does not exist.")
            return  # Exit if the cube does not exist
        else:
            cube_id = result[0]  # Get the cube_id
            print(f"Found cube_id: {cube_id} for cube_name: {parsed_data[0]}")

            # Update the cube topic
            cursor.execute('''
                            UPDATE cubes
                            SET cube_topic = ?
                            WHERE cube_name = ?
                        ''', (parsed_data[1], parsed_data[0]))
            print(f"Updated topic for cube: {parsed_data[0]}")
        conn.commit()

    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
        return  # Exit if there's an error

    ### NOW WE HAVE CUBE_ID ###

    # 2. Insert dimensions and their hierarchies
    dimension_ids = []  # Initialize lists to store dimension ids
    hierarchy_map = {}  # New dictionary to map dimension_id to its hierarchy_ids

    # Insert dimensions and hierarchy, get their ids
    for dimension_name, levels in parsed_data[2].items():
        try:
            # Initialize dimension_id variable
            dimension_id = None

            # Check if the dimension already exists
            cursor.execute("SELECT dimension_id FROM dimensions WHERE dimension_name = ?", (dimension_name,))
            result = cursor.fetchone()

            # Insert the new dimension if it doesn't exist
            if result is None:
                cursor.execute("INSERT INTO dimensions (dimension_name) VALUES (?)", (dimension_name,))
                dimension_id = cursor.lastrowid  # Get the last inserted dimension ID
                print(f"Inserted dimension: {dimension_name}")
                conn.commit()
            else:
                dimension_id = result[0]
                print(f"Dimension already exists: {dimension_name}")

            # Collect the dimension_id in the list
            dimension_ids.append(dimension_id)
            hierarchy_map[dimension_id] = []  # Initialize an empty list for hierarchy IDs

            # Insert hierarchy levels for the dimension (new or existing)
            for level_name in levels:
                # Initialize hierarchy_id variable
                hierarchy_id = None

                # Check if the hierarchy already exists
                cursor.execute(
                    "SELECT hierarchy_id FROM hierarchies WHERE hierarchy_name = ?",
                    (level_name,))
                hierarchy_result = cursor.fetchone()

                # Insert the hierarchy if it doesn't exist
                if hierarchy_result is None:
                    cursor.execute("INSERT INTO hierarchies (hierarchy_name) VALUES (?)",
                                   (level_name,))
                    hierarchy_id = cursor.lastrowid  # Get the last inserted hierarchy ID
                    print(f"Inserted hierarchy: {level_name} for dimension: {dimension_name}")
                else:
                    hierarchy_id = hierarchy_result[0]  # Get the existing hierarchy ID
                    print(f"Hierarchy already exists: {level_name} for dimension: {dimension_name}")

                # Collect the hierarchy_id in the dictionary for the current dimension_id
                hierarchy_map[dimension_id].append(hierarchy_id)

            # Optionally commit after processing all hierarchies for the dimension
            conn.commit()

        except sqlite3.Error as e:
            print("An error occurred:", e)
            conn.rollback()  # Rollback in case of error

    # 3. Insert measures into the database
    measure_ids = []  # Initialize a list to store measure IDs
    for measure_name in parsed_data[3]:
        measure_id = None  # Initialize measure_id variable
        try:
            # Check if the measure already exists
            cursor.execute("SELECT measure_id FROM measures WHERE measure_name = ?", (measure_name,))
            result = cursor.fetchone()

            # Insert the new measure if it doesn't exist
            if result is None:
                cursor.execute("INSERT INTO measures (measure_name) VALUES (?)", (measure_name,))
                measure_id = cursor.lastrowid  # Get the last inserted measure ID
                print(f"Inserted measure: {measure_name} with ID: {measure_id}")
            else:
                measure_id = result[0]  # Get the existing measure ID
                print(f"Measure already exists: {measure_name} with ID: {measure_id}")

            # Collect the measure_id in the list
            measure_ids.append(measure_id)

        except sqlite3.Error as e:
            print(f"An error occurred: {e}")

    conn.commit()
    # Access the collected measure_ids after the loop
    print("All measure IDs:", measure_ids)

    # 4. Fill the comprehensive table
    # Now we convert the three lists to JSON format
    measure_ids_json = json.dumps(measure_ids)
    dimension_ids_json = json.dumps(dimension_ids)
    hierarchy_ids_json = json.dumps(hierarchy_map)

    # Insert into the comprehensive table
    cursor.execute("""INSERT INTO comprehensive (cube_id, dimension_ids, hierarchy_ids, measure_ids)
        VALUES (?, ?, ?, ?)""", (cube_id, dimension_ids_json, hierarchy_ids_json, measure_ids_json))

    conn.commit()
    cursor.close()  # Close the cursor
    conn.close()  # Close the connection

