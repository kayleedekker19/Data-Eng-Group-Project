from flask import Flask, request, render_template_string, render_template
import folium
import ee
from datetime import datetime, timedelta
from weather.data import get_gpm_sequence, get_goes16_sequence, get_elevation, get_inputs_image, get_labels_image

# app = Flask(__name__)

# Define the functions for generating each layer, similar to your provided scripts
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

# @app.route('/', methods=['GET', 'POST'])
def show_map():
    selected_date = request.form.get('date', '2019-09-02')
    selected_time = request.form.get('time', '18:00')

    date_time_str = f"{selected_date} {selected_time}"
    date = datetime.strptime(date_time_str, '%Y-%m-%d %H:%M')

    map = folium.Map(location=[25, -90], zoom_start=5)
    gpm_image = get_gpm_sequence([date])
    gpm_layer(gpm_image, "GPM " + date.strftime('%Y-%m-%d %H:%M'), 0).add_to(map)
    goes16_image = get_goes16_sequence([date])
    goes16_layer(goes16_image, "GOES-16 " + date.strftime('%Y-%m-%d %H:%M'), 0).add_to(map)
    elevation_layer().add_to(map)
    folium.LayerControl().add_to(map)

    map_html = map._repr_html_()

    # Pass the map_html as a variable to the template
    return render_template('map_template.html', map_html=map_html)

# if __name__ == '__main__':
#     app.run(debug=True)
