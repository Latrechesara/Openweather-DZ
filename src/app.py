import os
import pandas as pd
import clickhouse_connect
import dash
from dash import dcc, html
from dash.dependencies import Output, Input
import dash_bootstrap_components as dbc
import plotly.express as px

# Point to localhost when running locally outside Docker
CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")

def get_ch_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, 
        port=8123, 
        username="default", 
        password="weather123"
    )

# Initialize Dash with DARKLY theme
app = dash.Dash(
    __name__, 
    external_stylesheets=[
        dbc.themes.DARKLY,
        "https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap"
    ]
)
app.title = "🇩🇿 MeteoFlow-DZ"

# ------------------------------------------------------------------------------
# LAYOUT
# ------------------------------------------------------------------------------
app.layout = dbc.Container([
    # Clean Header (No pipeline subtitle)
    dbc.Row([
        dbc.Col(
            html.Div([
                html.H2("🇩🇿 MeteoFlow-DZ", className="text-white fw-bold my-3"),
            ]), width=12
        )
    ]),

    # Main Dashboard Grid
    dbc.Row([
        # LEFT COLUMN: Algeria Map
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Algeria Geographic Telemetry"),
                dbc.CardBody([
                    dcc.Graph(
                        id='algeria-map', 
                        style={"height": "70vh"}, 
                        config={'displayModeBar': False}
                    )
                ])
            ], color="dark", inverse=True, className="border-secondary shadow-sm")
        ], width=7),

        # RIGHT COLUMN: Metrics Stack & Scatter
        dbc.Col([
            # KPI 1
            dbc.Card([
                dbc.CardBody([
                    html.H6("NATIONAL AVERAGE TEMPERATURE", className="text-muted small fw-bold mb-1"),
                    html.H2(id="kpi-avg-temp", className="text-primary fw-bold mb-0", style={"fontSize": "2.2rem"})
                ])
            ], color="dark", inverse=True, className="border-secondary mb-3 shadow-sm"),

            # KPI 2
            dbc.Card([
                dbc.CardBody([
                    html.H6("HOTTEST ALGERIAN LOCATION", className="text-muted small fw-bold mb-1"),
                    html.H3(id="kpi-max-temp", className="text-warning fw-bold mb-0", style={"fontSize": "1.3rem"})
                ])
            ], color="dark", inverse=True, className="border-secondary mb-3 shadow-sm"),

            # Scatter Plot Card
            dbc.Card([
                dbc.CardHeader("Humidity vs. Wind Dynamics"),
                dbc.CardBody([
                    dcc.Graph(
                        id='scatter-humidity-wind', 
                        style={"height": "32vh"}, 
                        config={'displayModeBar': False}
                    )
                ])
            ], color="dark", inverse=True, className="border-secondary shadow-sm")

        ], width=5)

    ], className="g-3"),

    # Refresh Interval (4s)
    dcc.Interval(
        id='interval-component',
        interval=4*1000, 
        n_intervals=0
    )
], fluid=True, style={"backgroundColor": "#0e1117", "minHeight": "100vh", "padding": "1rem 2rem 2rem 2rem"})


# ------------------------------------------------------------------------------
# CALLBACK
# ------------------------------------------------------------------------------
@app.callback(
    [
        Output('kpi-avg-temp', 'children'),
        Output('kpi-max-temp', 'children'),
        Output('algeria-map', 'figure'),
        Output('scatter-humidity-wind', 'figure')
    ],
    Input('interval-component', 'n_intervals')
)
def update_metrics(n):
    try:
        client = get_ch_client()
        query = """
            SELECT 
                city, 
                multiIf(
                    city = 'Algiers', 36.7538,
                    city = 'Oran', 35.6971,
                    city = 'Constantine', 36.365,
                    city = 'Annaba', 36.9,
                    city = 'Sétif', 36.19,
                    city = 'Ouargla', 31.9493,
                    city = 'Ghardaïa', 32.4909,
                    city = 'Tamanghasset', 22.785,
                    28.0339
                ) as lat,
                multiIf(
                    city = 'Algiers', 3.0588,
                    city = 'Oran', -0.6309,
                    city = 'Constantine', 6.6147,
                    city = 'Annaba', 7.7667,
                    city = 'Sétif', 5.41,
                    city = 'Ouargla', 5.3189,
                    city = 'Ghardaïa', 3.6734,
                    city = 'Tamanghasset', 5.5228,
                    1.6596
                ) as lon,
                round(argMax(temperature, timestamp), 1) as temp, 
                round(argMax(humidity, timestamp), 0) as humidity,
                round(argMax(wind_speed, timestamp), 1) as wind_speed
            FROM weather_analytics.weather_metrics
            GROUP BY city
        """
        df = client.query_df(query)
    except Exception as e:
        print(f"Database query error: {e}")
        df = pd.DataFrame(columns=['city', 'lat', 'lon', 'temp', 'humidity', 'wind_speed'])

    if df.empty:
        empty_fig = px.scatter()
        empty_fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        return "N/A", "N/A", empty_fig, empty_fig

    # KPIs
    avg_temp = f"{df['temp'].mean():.1f}°C"
    hottest_city_row = df.loc[df['temp'].idxmax()]
    max_temp_kpi = f"{hottest_city_row['city']} ({hottest_city_row['temp']}°C)"

    # Label for map pins
    df['map_label'] = df['city'] + ": " + df['temp'].astype(str) + "°C"

    # 1. Interactive Map
    fig_map = px.scatter_map(
        df,
        lat="lat",
        lon="lon",
        text="map_label",
        color="temp",
        size="humidity",
        color_continuous_scale="thermal",
        size_max=20,
        zoom=4.7,
        center={"lat": 28.0339, "lon": 2.5},
        map_style="carto-darkmatter"
    )

    fig_map.update_traces(
        textposition='top center',
        textfont=dict(size=12, color='#ffffff')
    )

    fig_map.update_layout(
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="white"),
        coloraxis_showscale=False
    )

    # 2. Scatter Plot
    fig_scatter = px.scatter(
        df,
        x="humidity",
        y="wind_speed",
        color="city",
        size="temp",
        size_max=14,
        labels={"humidity": "Humidity (%)", "wind_speed": "Wind (m/s)"},
        template="plotly_dark"
    )

    fig_scatter.update_layout(
        margin={"r": 10, "t": 10, "l": 10, "b": 10},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="white", size=10),
        xaxis=dict(gridcolor="#30363d", zeroline=False),
        yaxis=dict(gridcolor="#30363d", zeroline=False),
        showlegend=False
    )

    return avg_temp, max_temp_kpi, fig_map, fig_scatter


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8050, debug=True)