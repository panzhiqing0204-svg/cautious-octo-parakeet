# cautious-octo-parakeet
Here I store the files for my "Capstone: Retrieving, Processing, and Visualizing Data with Python" project.
The capstone project is about fetching data from a specific source and visualizing it. 
The data source I picked was Data USA. Instead of providing data directly, on its API page https://api.datausa.io/tesseract/cubes there are 124 "cubes".
Using the cube's name, dimensions, measures etc., we can construct a new URL which contains the actual data. 
For detailed requirements and/or other info of this capstone, please refer to the course's website on Coursera. 

What this project does:
1. The Main.py will create a table containing the cubes' names, topics, dimensions, measures etc.;
2. The Mapping.py will map the links between cubes and dimensions, cubes and measures etc.;
3. In the Visualization.py, I used an example URL to fetch state population data and made a choropleth map out of it.

* I have only learned Python for exact two months when I finally finished this project. So it's quite basic and simple. 
