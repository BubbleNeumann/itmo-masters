import json
import pandas as pd

output_file = "sem1_datavis_hw2_co2_emissions_d3.html"
geojson_url = "https://raw.githubusercontent.com/holtzy/D3-graph-gallery/master/DATA/world.geojson"
co2_data_url = "https://raw.githubusercontent.com/owid/co2-data/master/owid-co2-data.csv"

# load geojson
import urllib.request
with urllib.request.urlopen(geojson_url) as response:
    geojson_data = json.load(response)

# read data
co2_data = pd.read_csv(co2_data_url)
co2_data_2019 = co2_data[co2_data['year'] == 2023][['country', 'co2']].dropna()

# map data to geojson features
co2_dict = co2_data_2019.set_index('country')['co2'].to_dict()

for feature in geojson_data['features']:
    country_name = feature['properties']['name']
    feature['properties']['co2'] = co2_dict.get(country_name, 0)

html_template = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CO2 Emissions Map</title>
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        body {{
            font-family: Arial, sans-serif;
        }}
        .map {{
            width: 100%;
            height: 90vh;
        }}
        .tooltip {{
            position: absolute;
            background: rgba(0, 0, 0, 0.7);
            color: #fff;
            padding: 5px;
            border-radius: 5px;
            pointer-events: none;
            font-size: 12px;
        }}
    </style>
</head>
<body>
    <h1>CO2 Emissions by Country (2023)</h1>
    <div id="map" class="map"></div>
    <div id="tooltip" class="tooltip" style="opacity: 0;"></div>
    <script>
        const width = 960;
        const height = 600;

        const geojson = {json.dumps(geojson_data)};

        const colorScale = d3.scaleThreshold()
            .domain([0, 10, 50, 100, 500, 1000])
            .range(["#FFEDA0", "#FEB24C", "#FD8D3C", "#FC4E2A", "#E31A1C", "#BD0026", "#800026"]);

        const svg = d3.select("#map").append("svg")
            .attr("width", width)
            .attr("height", height);

        const projection = d3.geoMercator()
            .scale(150)
            .translate([width / 2, height / 1.5]);

        const path = d3.geoPath().projection(projection);

        const tooltip = d3.select("#tooltip");

        svg.selectAll("path")
            .data(geojson.features)
            .join("path")
            .attr("d", path)
            .attr("fill", d => colorScale(d.properties.co2 || 0))
            .attr("stroke", "#333")
            .on("mouseover", function (event, d) {{
                d3.select(this).attr("stroke-width", 2);
                tooltip.transition().duration(200).style("opacity", 1);
                tooltip.html(`
                    <strong>${{d.properties.name}}</strong><br>
                    CO₂ Emissions: ${{d.properties.co2 || "No data"}} Mt
                `)
                .style("left", (event.pageX + 10) + "px")
                .style("top", (event.pageY - 30) + "px");
            }})
            .on("mousemove", function (event) {{
                tooltip.style("left", (event.pageX + 10) + "px")
                    .style("top", (event.pageY - 30) + "px");
            }})
            .on("mouseout", function () {{
                d3.select(this).attr("stroke-width", 1);
                tooltip.transition().duration(200).style("opacity", 0);
            }});
    </script>
</body>
</html>
"""

# save html
with open(output_file, "w", encoding="utf-8") as file:
    file.write(html_template)

print(f"Map saved as {output_file}. Open this file in a browser.")
