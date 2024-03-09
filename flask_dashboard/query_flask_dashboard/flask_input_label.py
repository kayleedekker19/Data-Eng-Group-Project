from flask import Flask, render_template_string
import folium
import ee
from datetime import datetime, timedelta
from weather.data import get_inputs_image, get_labels_image, get_elevation, OUTPUT_HOUR_DELTAS
import os

# Ensure the 'templates' directory exists
if not os.path.exists('templates'):
    os.makedirs('templates')


app = Flask(__name__)

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


def goes16_layer(image: ee.Image, label: str, i: int) -> folium.TileLayer:
    vis_params = {
        "bands": [f"{i}_CMI_C02", f"{i}_CMI_C03", f"{i}_CMI_C01"],
        "min": 0.0,
        "max": 3000.0,
    }
    return folium.TileLayer(
        name=f"[{label}] Cloud and moisture",
        tiles=image.getMapId(vis_params)["tile_fetcher"].url_format,
        attr='Map Data &copy; <a href="https://earthengine.google.com/">Google Earth Engine</a>',
        overlay=True,
    )

def elevation_layer() -> folium.TileLayer:
    image = get_elevation()
    vis_params = {
        "bands": ["elevation"],
        "min": 0.0,
        "max": 3000.0,
        "palette": [
            "000000",
            "478FCD",
            "86C58E",
            "AFC35E",
            "8F7131",
            "B78D4F",
            "E2B8A6",
            "FFFFFF",
        ],
    }
    return folium.TileLayer(
        name="Elevation",
        tiles=image.getMapId(vis_params)["tile_fetcher"].url_format,
        attr='Map Data &copy; <a href="https://earthengine.google.com/">Google Earth Engine</a>',
        overlay=True,
    )

# Function to save a map as an HTML file and return the file path
def save_map(map, filename):
    static_path = 'static'
    if not os.path.exists(static_path):
        os.makedirs(static_path)
    filepath = f'{static_path}/{filename}.html'
    map.save(filepath)
    # Return a relative URL to the saved file
    return f'{filepath}'


def create_inputs_map():
    date = datetime(2019, 9, 2, 18)
    image = get_inputs_image(date)
    input_hour_deltas = [-4, -2, 0]

    map = folium.Map([25, -90], zoom_start=5)
    elevation_layer().add_to(map)
    for i, h in enumerate(input_hour_deltas):
        label = str(date + timedelta(hours=h))
        goes16_layer(image, label, i).add_to(map)
        gpm_layer(image, label, i).add_to(map)
    folium.LayerControl().add_to(map)
    return map

def create_labels_map():
    date = datetime(2019, 9, 3, 18)
    image = get_labels_image(date)
    map = folium.Map([25, -90], zoom_start=5)
    for i, h in enumerate(OUTPUT_HOUR_DELTAS):
        label = str(date + timedelta(hours=h))
        gpm_layer(image, label, i).add_to(map)
    folium.LayerControl().add_to(map)
    return map

@app.route('/')
def show_maps():
    # Create and save the maps, then get the paths
    inputs_map = create_inputs_map()
    labels_map = create_labels_map()

    # Save the maps to HTML files in the 'static' directory and get the file paths
    inputs_map_path = save_map(inputs_map, 'inputs_map')
    labels_map_path = save_map(labels_map, 'labels_map')

    # Note: The `save_map` function returns a path relative to the Flask app root,
    # and since we're saving to the 'static' directory, we need to adjust the src path for iframes.
    # Flask serves static files from the '/static' URL path, so we prepend '/static' to the filenames.
    inputs_map_url = '/' + inputs_map_path
    labels_map_url = '/' + labels_map_path

    # # Embed maps using iframes with the correct src URLs
    # embedded_maps_html = f'''
    # <!DOCTYPE html>
    # <html>
    # <head>
    #     <title>Maps Display</title>
    #     <style>
    #         .map-container {{
    #             display: flex;
    #             justify-content: space-around;
    #             flex-wrap: wrap;
    #         }}
    #         iframe {{
    #             width: 45%;
    #             height: 400px;
    #             border: none;
    #             margin: 10px;
    #         }}
    #     </style>
    # </head>
    # <body>
    #     <div class="map-container">
    #         <iframe src="{inputs_map_url}"></iframe>
    #         <iframe src="{labels_map_url}"></iframe>
    #     </div>
    # </body>
    # </html>
    # '''


    # Embed maps using iframes with the correct src URLs and add titles
    embedded_maps_html = f'''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Maps Display</title>
        <style>
            body, html {{
                height: 100%;
                margin: 0;
                display: flex;
                flex-direction: column;
                justify-content: center; /* Centers content vertically */
                align-items: center; /* Centers content horizontally */
            }}
            .map-container {{
                display: flex;
                justify-content: space-around;
                flex-wrap: wrap;
                align-items: flex-start; /* Align items at the top */
            }}
            iframe {{
                width: 45%;
                height: 400px;
                border: none;
                margin: 10px; 
            }}
            .map-title {{
                width: 45%;
                text-align: center;
                margin: 10px;
            }}
        </style>
    </head>
    <body>
        <div class="map-container">
            <div class="map-title"><h2>Input Data</h2></div>
            <div class="map-title"><h2>Label Data</h2></div>
            <iframe src="{inputs_map_url}"></iframe>
            <iframe src="{labels_map_url}"></iframe>
        </div>
    </body>
    </html>
    '''


    return render_template_string(embedded_maps_html)


if __name__ == '__main__':
    app.run(debug=True)

