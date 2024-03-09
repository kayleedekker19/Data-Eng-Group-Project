from flask import Flask, render_template_string
from datetime import datetime
import folium
import ee
from weather.data import get_gpm_sequence

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

@app.route('/')
def show_map():
    dates = [datetime(2019, 9, 2, 18)]
    image = get_gpm_sequence(dates)

    folium_map = folium.Map(location=[25, -90], zoom_start=5)
    for i, date in enumerate(dates):
        gpm_layer(image, str(date), i).add_to(folium_map)
    folium.LayerControl().add_to(folium_map)

    # Render the Folium map as HTML
    map_html = folium_map._repr_html_()

    # Use render_template_string to directly render the map with a simple template
    return render_template_string('<!DOCTYPE html><html><head><title>Precipitation Map</title></head><body>{{ map_html|safe }}</body></html>', map_html=map_html)

if __name__ == '__main__':
    app.run(debug=True)
