# THIS CONTAINS THE FUNCTIONS FOR MAPPING
import sqlite3
import json

# We need cube_id, dimension_id, measure_id, hierarchy_id

# FUNCTION FOR FETCHING CUBE INFO FROM THE COMPREHENSIVE TABLE
def fetch_cube_info():
    conn = sqlite3.connect('datausa_analysis.sqlite')
    cursor = conn.cursor()

    # Example SQL query to fetch cube_id and IDs stored as JSON
    cursor.execute("SELECT cube_id, dimension_ids, measure_ids, hierarchy_ids FROM comprehensive")
    rows = cursor.fetchall()

    conn.close()
    return rows

# FUNCTION FOR PROCESSING THE CUBE INFO
def process_cube_info(rows):
    cube_info_list = []

    for row in rows:
        cube_id = row[0]
        dimension_ids = json.loads(row[1]) if row[1] else []  # Convert JSON string to list
        measure_ids = json.loads(row[2]) if row[2] else []    # Convert JSON string to list
        hierarchy_ids = json.loads(row[3]) if row[3] else []  # Convert JSON string to list

        cube_info = {
            'cube_id': cube_id,
            'dimension_ids': dimension_ids,
            'measure_ids': measure_ids,
            'hierarchy_ids': hierarchy_ids
        }
        cube_info_list.append(cube_info)

    return cube_info_list

# FUNCTION FOR MAPPING CUBE TO DIMENSIONS
def map_cube_to_dimensions(cursor, cube_id, dimension_id):
    try:
        cursor.execute("SELECT * FROM cube_dimensions WHERE cube_id = ? AND dimension_id = ?", (cube_id, dimension_id))
        if cursor.fetchone() is None:
            cursor.execute("INSERT INTO cube_dimensions (cube_id, dimension_id) VALUES (?, ?)", (cube_id, dimension_id))
            print(f"Linked cube {cube_id} to dimension {dimension_id}")
            return True  # Indicate that a new mapping was created
        else:
            print(f"Link already exists between cube {cube_id} and dimension {dimension_id}")
            return False  # Indicate that no new mapping was created
    except sqlite3.Error as e:
        print(f"An error occurred during mapping: {e}")
        return False  # Indicate failure

# FUNCTION FOR MAPPING DIMENSION TO HIERARCHIES
def map_dimension_to_hierarchies(cursor, dimension_id, hierarchy_id):
    try:
        print(f"Attempting to link dimension {dimension_id} to hierarchy {hierarchy_id}")

        # Check if the dimension_id exists
        cursor.execute("SELECT * FROM dimensions WHERE dimension_id = ?", (dimension_id,))
        if cursor.fetchone() is None:
            print(f"Error: dimension_id {dimension_id} does not exist in dimensions table.")
            return False

        # Check if the hierarchy_id exists
        cursor.execute("SELECT * FROM hierarchies WHERE hierarchy_id = ?", (hierarchy_id,))
        if cursor.fetchone() is None:
            print(f"Error: hierarchy_id {hierarchy_id} does not exist in hierarchies table.")
            return False

        # Check if the link already exists
        cursor.execute("SELECT * FROM dimension_hierarchies WHERE dimension_id = ? AND hierarchy_id = ?", (dimension_id, hierarchy_id))
        if cursor.fetchone() is None:
            cursor.execute("INSERT INTO dimension_hierarchies (dimension_id, hierarchy_id) VALUES (?, ?)", (dimension_id, hierarchy_id))
            print(f"Linked dimension {dimension_id} to hierarchy {hierarchy_id}")
            return True
        else:
            print(f"Link already exists between dimension {dimension_id} and hierarchy {hierarchy_id}")
            return False
    except sqlite3.Error as e:
        print(f"An error occurred while linking dimension to hierarchy: {e}")
        return False

# FUNCTION FOR MAPPING CUBE TO MEASURES
def map_cube_to_measures(cursor, cube_id, measure_id):
    try:
        cursor.execute("SELECT * FROM cube_measures WHERE cube_id = ? AND measure_id = ?", (cube_id, measure_id))
        if cursor.fetchone() is None:
            cursor.execute("INSERT INTO cube_measures (cube_id, measure_id) VALUES (?, ?)", (cube_id, measure_id))
            print(f"Linked cube {cube_id} to measure {measure_id}")
            return True
        else:
            print(f"Link already exists between cube {cube_id} and measure {measure_id}")
            return False
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
        return False

# MAIN FUNCTION
def mapping_main():
    conn = sqlite3.connect('datausa_analysis.sqlite')
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON;")  # Enable foreign key support

    try:
        # Clear previous mappings
        cursor.execute("DELETE FROM cube_dimensions")
        cursor.execute("DELETE FROM dimension_hierarchies")
        cursor.execute("DELETE FROM cube_measures")
        print("Existing mappings cleared from link tables.")

        # Fetch cube information from the database
        rows = fetch_cube_info()
        if not rows:
            print("No cube information fetched.")
            return  # Exit if no data is fetched

        # Process the fetched data
        cube_info = process_cube_info(rows)

        for info in cube_info:
            print("Processing cube info:", info)

            cube_id = info["cube_id"]
            dimension_ids = info["dimension_ids"]
            measure_ids = info["measure_ids"]
            hierarchy_map = info["hierarchy_ids"]

            # Step 1: Create mapping between cube_id and dimension_ids
            for dimension_id in dimension_ids:
                print(f"Mapping cube_id {cube_id} to dimension_id {dimension_id}")
                map_cube_to_dimensions(cursor, cube_id, dimension_id)

            # Step 2: Create mapping between dimension_id and hierarchy_id
            for dimension_id in dimension_ids:
                str_dimension_id = str(dimension_id)  # Convert to string for hierarchy_map lookup
                if str_dimension_id in hierarchy_map:
                    associated_hierarchy_ids = hierarchy_map[str_dimension_id]
                    print(f"Dimension ID {dimension_id} has associated hierarchy IDs: {associated_hierarchy_ids}")  # Debugging output
                    for hierarchy_id in associated_hierarchy_ids:
                        print(f"Linking dimension_id {dimension_id} to hierarchy_id {hierarchy_id}")  # Debugging output
                        success = map_dimension_to_hierarchies(cursor, dimension_id, hierarchy_id)
                        if not success:
                            print(f"Failed to link dimension {dimension_id} to hierarchy {hierarchy_id}")
                else:
                    print(f"No hierarchy mapping found for dimension_id {dimension_id}")  # Debugging output

            # Step 3: Create mapping between cube_id and measure_ids
            for measure_id in measure_ids:
                print(f"Mapping cube_id {cube_id} to measure_id {measure_id}")
                map_cube_to_measures(cursor, cube_id, measure_id)

            print("Mapping complete for cube_id:", cube_id)

        conn.commit()  # Commit all changes after processing

    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
        conn.rollback()  # Rollback in case of error
    finally:
        cursor.close()  # Ensure the cursor is closed
        conn.close()  # Ensure the connection is closed

if __name__ == "__main__":
    mapping_main()