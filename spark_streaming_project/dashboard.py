import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import pandas as pd
import time
from bitcoin_stream import fetch_bitcoin_price

# Initialize the Dash app
app = dash.Dash(__name__)

# Layout of the dashboard
app.layout = html.Div([
    html.H1("Real-Time Bitcoin Price Dashboard"),
    dcc.Graph(id="live-graph", animate=True),
    dcc.Interval(
        id="interval-component",
        interval=5 * 1000,  # Update every 5 seconds
        n_intervals=0
    )
])

# Empty DataFrame to hold the data
df = pd.DataFrame(columns=["Time", "Price"])

# Callback to update the graph at regular intervals
@app.callback(
    Output("live-graph", "figure"),
    [Input("interval-component", "n_intervals")]
)
def update_graph_live(n_intervals):
    global df

    # Fetch the current Bitcoin price
    price = fetch_bitcoin_price()
    current_time = time.strftime("%Y-%m-%d %H:%M:%S")

    # Append new data to the DataFrame
    new_data = pd.DataFrame([[current_time, price]], columns=["Time", "Price"])
    df = pd.concat([df, new_data], ignore_index=True)

    # Create the plot
    fig = go.Figure(
        data=[go.Scatter(
            x=df["Time"],
            y=df["Price"],
            mode="lines+markers",
            name="Bitcoin Price"
        )],
        layout=go.Layout(
            title="Live Bitcoin Price",
            xaxis_title="Time",
            yaxis_title="Price (USD)",
            showlegend=True
        )
    )
    return fig

# Run the dashboard
if __name__ == "__main__":
    app.run_server(debug=True)
