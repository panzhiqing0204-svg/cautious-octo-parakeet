# cautious-octo-parakeet 
Here I store the files for my "Capstone: Retrieving, Processing, and Visualizing Data with Python" project.
The capstone project is about fetching data from a specific source and visualizing it. 
The data source I picked was Data USA. Instead of providing data directly, on its API page https://api.datausa.io/tesseract/cubes there are 124 "cubes".
Using the cube's name, dimensions, measures etc., we can construct a new URL which contains the actual data. 
For detailed requirements and/or other info of this capstone, please refer to the course's website on Coursera.

# What does this project contain?
0. db_setup.py and data_manager.py contain the functions used in Main.py;
1. Main.py (fetching and processing data, inserting processed data into SQLite database);
2. Mapping.py (mapping the links between cubes and dimensions, cubes and measures, dimensions and their respective hierarchies);
3. Visualization.py (making a choropleth map with the data from a specific URL)

# What can this project do?
1. The Main.py will create a table containing the cubes' names, topics, dimensions, measures etc.;
2. The Mapping.py will map the links between cubes and dimensions, cubes and measures etc.;
3. In the Visualization.py, I used an example URL to fetch state population data and made a choropleth map out of it.
*I chose choropleth map because this cube is about population. By combining different drilldowns (dimensions, hierarchies, measures), it's possible to retrieve data of other topics and create different charts.


I also attached screenshots of the database and choropleth map generated.

* I have only learned Python for exact two months when I finally finished this project. So it's quite basic and simple. 
