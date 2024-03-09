import plotly as pt
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

import numpy as np

# Load the .npz file
data = np.load('file_1.npz')

# data.files will list all the arrays stored inside the .npz file
print("Arrays contained in the .npz file:", data.files)

# Iterate over each array in the .npz and print its name, shape, and data type
for file in data.files:
    arr = data[file]  # Extract array
    print(f"Array name: {file}")
    print(f"Shape: {arr.shape}")
    print(f"Data Type: {arr.dtype}")
    print(f"First few elements: {arr.flat[:10]}")  # Print first few elements
    print("-------------------------------------------")

from visualisations import visualise_functions

# Load the data
data = np.load('file_1.npz')
inputs = data['inputs']
labels = data['labels']

# Visualize inputs
# Note: Adjust the slicing as necessary based on the structure of your inputs.
# This example assumes inputs are structured correctly for the visualization functions.
visualise_functions.show_inputs(inputs[0])  # Visualize the first input sample

# Visualize outputs
# Similarly, adjust the slicing for labels if necessary
visualise_functions.show_outputs(labels[0])  # Visualize the first output label sample


from datetime import datetime
import folium
import ee

# Initialize Earth Engine (if not already initialized)
# ee.Initialize()

# Assuming your get_gpm_sequence function is defined somewhere in your script
from weather import data

def gpm_layer(image: ee.Image, label: str, i: int) -> folium.TileLayer:
    vis_params = {
        "bands": [f"{i}_precipitationCal"],
        "min": 0.0,
        "max": 20.0,
        "palette": [
            "000096", "0064ff", "00b4ff", "33db80",
            "9beb4a", "ffeb00", "ffb300", "ff6400",
            "eb1e00", "af0000",
        ],
    }
    image = image.mask(image.gt(0.1))
    return folium.TileLayer(
        name=f"[{label}] Precipitation",
        tiles=image.getMapId(vis_params)["tile_fetcher"].url_format,
        attr='Map Data &copy; <a href="https://earthengine.google.com/">Google Earth Engine</a>',
        overlay=True,
    )

dates = [datetime(2019, 9, 2, 18)]
image = data.get_gpm_sequence(dates)

map = folium.Map(location=[25, -90], zoom_start=5)
for i, date in enumerate(dates):
    gpm_layer(image, str(date), i).add_to(map)
folium.LayerControl().add_to(map)

# Save the map to an HTML file
map.save('my_precipitation_map.html')

# Run this in the terminal:
# open /Users/kayleedekker/PycharmProjects/DataEngineeringProject/flask_dashboard/my_precipitation_map.html
