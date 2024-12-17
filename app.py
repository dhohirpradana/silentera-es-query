from flask import Flask, request, jsonify
from elasticsearch import Elasticsearch
from datetime import datetime
from collections import defaultdict
import math

app = Flask(__name__)

# Elasticsearch configuration
ES_CONFIG = {
    "host": "https://10.1.112.141:9200",
    "username": "elastic",
    "password": "zjJcB-i2EevG3L2eeBmT"
}

# Initialize Elasticsearch client
es_client = Elasticsearch(
    ES_CONFIG["host"],
    basic_auth=(ES_CONFIG["username"], ES_CONFIG["password"]),
    verify_certs=False,
    ssl_show_warn=False
)


# Fungsi untuk menghitung kecepatan angin
def calculate_wind_speed(u_component, v_component):
    """
    Calculate the wind speed (UGRD) from the U and V components of wind.
    """
    return math.sqrt(u_component**2 + v_component**2)

# Fungsi untuk menghitung arah angin


def calculate_wind_direction(u_component, v_component):
    """
    Calculate the wind direction in radians using atan2 function.
    """
    return math.atan2(-u_component, -v_component)  # Arah Angin = atan2(-UGRD, -VGRD)


def query_with_recursive_radius(es_client, index_name, latitude, longitude, starttime, endtime):
    """
    Query Elasticsearch with increasing radius until data is found.
    """
    radii = [5, 10, 20, 50]  # Radius in kilometers
    data_found = False
    result = {"location": None, "radius": None, "data": [], "count": None}

    for radius in radii:
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"range": {"timestamp": {"gte": starttime, "lte": endtime}}}
                    ],
                    "filter": {
                        "geo_distance": {
                            "distance": f"{radius}km",
                            "location": {"lat": latitude, "lon": longitude}
                        }
                    }
                }
            },
            "sort": [
                {"timestamp": {"order": "asc"}}
            ]
        }

        # Execute the query
        response = es_client.search(index=index_name, body=query, size=1000)

        hits = response.get("hits", {}).get("hits", [])
        doc_count = len(hits)

        if hits:
            # Data found, extract required details
            data_found = True
            result["radius"] = radius
            result["location"] = {"lat": latitude, "lon": longitude}
            result["count"] = doc_count

            # Deduplicate data by datetime
            unique_data = defaultdict(list)
            for hit in hits:
                datetime = hit["_source"]["timestamp"]
                value = hit["_source"]["value"]
                unique_data[datetime].append(value)

            # Process deduplicated data: take the first value (or average, etc.)
            result["data"] = [
                {
                    "datetime": datetime,
                    # Averaging duplicate values (or just taking the first one)
                    "value": sum(values) / len(values)
                }
                for datetime, values in unique_data.items()
            ]
            break  # Stop searching as data is found

    if not data_found:
        result["message"] = "No data found within the 50km radius."

    return result


# Mapping of type to corresponding CSV file
TYPE_TO_FILE_MAPPING = {
    "APCP": "total_precipitation",
    "DSWRF": "downward_short-wave_radiation_flux",
    "HCDC": "high_cloud_cover",
    "LCDC": "low_cloud_cover",
    "MCDC": "medium_cloud_cover",
    "TCDC": "total_cloud_cover",
    "TMAX": "maximum_temperature",
    "TMIN": "minimum_temperature",
    "DPT": "2_metre_dewpoint_temperature",
    "TMP": "2_metre_temperature",
    "UGRD": "10_metre_u_wind_component",
    "VGRD": "10_metre_v_wind_component",
    "GUST": "wind_speed_(gust)"
}


@app.route('/query', methods=['POST'])
def query_data():
    """
    Endpoint to query Elasticsearch with user input and calculate wind speed and direction.
    Example input:
    {
        "type": "UGRD",
        "latitude": -12.75,
        "longitude": 134.25,
        "starttime": "2024-12-14T00:00:00",
        "endtime": "2024-12-14T23:59:59"
    }
    """
    try:
        # Parse input JSON
        data = request.get_json()
        index_name = data.get("type")

        # Validate 'type' input
        if index_name not in TYPE_TO_FILE_MAPPING:
            return jsonify({"error": f"Invalid type. Allowed values are: {', '.join(TYPE_TO_FILE_MAPPING.keys())}"}), 400

        # Get the corresponding CSV file for the type
        csv_file = TYPE_TO_FILE_MAPPING[index_name]

        latitude = float(data.get("latitude"))
        longitude = float(data.get("longitude"))
        starttime = data.get("starttime")
        endtime = data.get("endtime")

        # Validate required fields
        if not all([index_name, latitude, longitude, starttime, endtime]):
            return jsonify({"error": "Missing required fields"}), 400

        # Jika tipe adalah UGRD, hitung kecepatan angin
        if index_name == "UGRD":
            # Ambil data dari komponen UGRD dan VGRD
            u_component_data = query_with_recursive_radius(
                es_client, "10_metre_u_wind_component", latitude, longitude, starttime, endtime)
            v_component_data = query_with_recursive_radius(
                es_client, "10_metre_v_wind_component", latitude, longitude, starttime, endtime)

            # Hitung arah angin menggunakan data komponen UGRD dan VGRD
            if u_component_data['data'] and v_component_data['data']:
                wind_speeds = [
                    {
                        "datetime": u_component["datetime"],
                        "value": calculate_wind_speed(u_component["value"], v_component["value"])
                    }
                    for u_component, v_component in zip(u_component_data['data'], v_component_data['data'])
                ]
                return jsonify({"location": {"lat": latitude, "lon": longitude}, "radius": 5, "data": wind_speeds, "count": len(wind_speeds)}), 200
            else:
                return jsonify({"error": "Data for UGRD or VGRD components not found."}), 404

        # Jika tipe adalah VGRD, hitung arah angin
        if index_name == "UGRD" or index_name == "VGRD":
            # Ambil data dari komponen UGRD dan VGRD
            u_component_data = query_with_recursive_radius(
                es_client, "10_metre_u_wind_component", latitude, longitude, starttime, endtime)
            v_component_data = query_with_recursive_radius(
                es_client, "10_metre_v_wind_component", latitude, longitude, starttime, endtime)

            # Hitung arah angin menggunakan data komponen UGRD dan VGRD
            if u_component_data['data'] and v_component_data['data']:
                wind_directions = [
                    {
                        "datetime": u_component["datetime"],
                        "value": calculate_wind_direction(u_component["value"], v_component["value"])
                    }
                    for u_component, v_component in zip(u_component_data['data'], v_component_data['data'])
                ]
                return jsonify({"location": {"lat": latitude, "lon": longitude}, "radius": 5, "data": wind_directions, "count": len(wind_directions)}), 200
            else:
                return jsonify({"error": "Data for UGRD or VGRD components not found."}), 404

        if index_name == "PV":
            ghi = query_with_recursive_radius(
                es_client, "downward_short-wave_radiation_flux", latitude, longitude, starttime, endtime)
            print(ghi['data'])

            if ghi['data']:
                pvs = [
                    {
                        "datetime": item["datetime"],
                        "value": 0.3 * int(item["value"])
                    }
                    for item in ghi['data']
                ]
                return jsonify({"location": {"lat": latitude, "lon": longitude}, "data": pvs, "count": len(pvs)}), 200
            else:
                return jsonify({"error": "Data for UGRD or VGRD components not found."}), 404

        # Query Elasticsearch untuk tipe lain
        result = query_with_recursive_radius(
            es_client, csv_file, latitude, longitude, starttime, endtime)

        return jsonify(result), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/')
def home():
    """Home endpoint."""
    return "Elasticsearch Query API is running. Use '/query' endpoint to query data."


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
