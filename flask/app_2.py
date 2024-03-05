from flask import Flask, request, render_template_string
import folium
import ee
from datetime import datetime, timedelta
from weather.data import get_gpm_sequence, get_goes16_sequence, get_elevation, get_inputs_image, get_labels_image

app = Flask(__name__)

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


@app.route('/', methods=['GET', 'POST'])
def show_map():
    # Default date and time
    selected_date = request.form.get('date', '2019-09-02')
    selected_time = request.form.get('time', '18:00')

    # Convert string inputs to datetime
    date_time_str = f"{selected_date} {selected_time}"
    date = datetime.strptime(date_time_str, '%Y-%m-%d %H:%M')

    map = folium.Map(location=[25, -90], zoom_start=5)

    # Add GPM layer
    gpm_image = get_gpm_sequence([date])
    gpm_layer(gpm_image, "GPM " + date.strftime('%Y-%m-%d %H:%M'), 0).add_to(map)

    # Add GOES-16 layer
    goes16_image = get_goes16_sequence([date])
    goes16_layer(goes16_image, "GOES-16 " + date.strftime('%Y-%m-%d %H:%M'), 0).add_to(map)

    # Add Elevation layer
    elevation_layer().add_to(map)

    folium.LayerControl().add_to(map)

    map_html = map._repr_html_()

    # HTML form for date and time selection
    form_html = '''
    <form action="/" method="post">
      <label for="date">Date:</label>
      <input type="date" id="date" name="date" value="2019-09-02">
      <label for="time">Time (HH:MM):</label>
      <input type="time" id="time" name="time" value="18:00">
      <input type="submit" value="Update Map">
    </form>
    '''

    # Render map with form
    return render_template_string(
        f'<!DOCTYPE html><html><head><title>Earth Engine Visualizations</title></head><body>{form_html}{{{{ map_html|safe }}}}</body></html>',
        map_html=map_html)


if __name__ == '__main__':
    app.run(debug=True)


# @app.route('/')
# def show_map():
#     map = folium.Map(location=[25, -90], zoom_start=5)
#     date = datetime(2019, 9, 2, 18)
#
#     # Add GPM layer
#     gpm_image = get_gpm_sequence([date])
#     for i, _ in enumerate([date]):  # Using [date] to keep the interface consistent
#         gpm_layer(gpm_image, "GPM " + str(date + timedelta(hours=i)), i).add_to(map)
#
#     # Add GOES-16 layer
#     goes16_image = get_goes16_sequence([date])
#     for i, _ in enumerate([date]):
#         goes16_layer(goes16_image, "GOES-16 " + str(date + timedelta(hours=i)), i).add_to(map)
#
#     # Add Elevation layer
#     elevation_layer().add_to(map)
#
#     # Add Inputs layer (combining GPM and GOES-16 for demonstration, adjust as needed)
#     inputs_image = get_inputs_image(date)
#     input_hour_deltas = [-4, -2, 0]
#     for i, h in enumerate(input_hour_deltas):
#         label = str(date + timedelta(hours=h))
#         goes16_layer(inputs_image, "Inputs GOES-16 " + label, i).add_to(map)
#         gpm_layer(inputs_image, "Inputs GPM " + label, i).add_to(map)
#
#     # Add Labels layer (demonstration using GPM sequence, adjust as needed)
#     labels_date = datetime(2019, 9, 3, 18)  # Example label date
#     labels_image = get_labels_image(labels_date)
#     OUTPUT_HOUR_DELTAS = [2, 6]  # Example output deltas, adjust as needed
#     for i, h in enumerate(OUTPUT_HOUR_DELTAS):
#         label = str(labels_date + timedelta(hours=h))
#         gpm_layer(labels_image, "Labels " + label, i).add_to(map)
#
#     # Add layer control
#     folium.LayerControl().add_to(map)
#
#     # Render map
#     map_html = map._repr_html_()
#     return render_template_string('<!DOCTYPE html><html><head><title>Earth Engine Visualizations</title></head><body>{{ map_html|safe }}</body></html>', map_html=map_html)
#
# if __name__ == '__main__':
#     app.run(debug=True)


# @app.route('/')
# def show_map():
#     map_type = request.args.get('type', 'gpm')  # Default to 'gpm' if no type is provided
#     map = folium.Map(location=[25, -90], zoom_start=5)
#
#     # Based on the map type, add the respective layers
#     if map_type == 'gpm':
#         dates = [datetime(2019, 9, 2, 18)]
#         image = get_gpm_sequence(dates)
#         for i, date in enumerate(dates):
#             gpm_layer(image, str(date), i).add_to(map)
#
#     elif map_type == 'goes16':
#         dates = [datetime(2019, 9, 2, 18)]
#         image = get_goes16_sequence(dates)
#         for i, date in enumerate(dates):
#             goes16_layer(image, str(date), i).add_to(map)
#
#     elif map_type == 'elevation':
#         elevation_layer().add_to(map)
#
#     elif map_type == 'inputs':
#         # Similar logic to add inputs layer
#         date = datetime(2019, 9, 2, 18)
#         image = get_inputs_image(date)
#         # Add inputs layer code here
#         input_hour_deltas = [-4, -2, 0]  # Get 4 hours prior, 2 hours prior, and current time.
#         map = folium.Map([25, -90], zoom_start=5)  # Show map.
#         elevation_layer().add_to(map)
#         for i, h in enumerate(input_hour_deltas):
#             label = str(date + timedelta(hours=h))
#             goes16_layer(image, label, i).add_to(map)
#             gpm_layer(image, label, i).add_to(map)
#         folium.LayerControl().add_to(map)
#
#     elif map_type == 'labels':
#         # Similar logic to add labels layer
#         date = datetime(2019, 9, 3, 18)
#         image = get_labels_image(date)
#         # Add labels layer code here
#         map = folium.Map([25, -90], zoom_start=5)
#         for i, h in enumerate(OUTPUT_HOUR_DELTAS):
#             label = str(date + timedelta(hours=h))
#             gpm_layer(image, label, i).add_to(map)
#         folium.LayerControl().add_to(map)
#
#     folium.LayerControl().add_to(map)
#     map_html = map._repr_html_()
#
#     return render_template_string('<!DOCTYPE html><html><head><title>Earth Engine Visualizations</title></head><body>{{ map_html|safe }}</body></html>', map_html=map_html)
#
# if __name__ == '__main__':
#     app.run(debug=True)
