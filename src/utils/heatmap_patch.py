from folium.plugins import HeatMap

def add_heatmap_layer(mapa, df, lat_col, lon_col):
    pontos = df[[lat_col, lon_col]].dropna().values.tolist()

    HeatMap(
        pontos,
        radius=26,
        blur=18,
        min_opacity=0.35,
        max_zoom=15,
        gradient={
            0.05: "#e6e1ff",
            0.18: "#c9c2ff",
            0.32: "#9b8cff",
            0.48: "#63d7ff",
            0.62: "#49f0c1",
            0.78: "#7dff4d",
            0.90: "#ffe84a",
            1.00: "#ff3b1f",
        },
    ).add_to(mapa)

    return mapa
