import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import pandas as pd
import time
from confluent_kafka import Consumer, KafkaException
 
# Kafka consumer configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'dash-consumer-group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe(['bitcoin-prices'])
 
# Initialize the Dash app
app = dash.Dash(__name__)
 
# Layout of the dashboard
app.layout = html.Div([
    html.H1("Real-Time Bitcoin Price Dashboard", style={'text-align': 'center'}),
    dcc.Graph(id="live-graph", style={'width': '100%', 'height': '80vh'}),
    dcc.Interval(
        id="interval-component",
        interval=5 * 1000,  # Update every 5 seconds
        n_intervals=0
    )
])
 
# Initialize empty DataFrame to store incoming data
df = pd.DataFrame(columns=["Time", "Price"])
 
# Callback to update the graph every 5 seconds
# @app.callback(
#     Output("live-graph", "figure"),
#     [Input("interval-component", "n_intervals")]
# )
@app.callback(
    Output("live-graph", "figure"),
    [Input("interval-component", "n_intervals")]
)
def update_graph_live(n_intervals):
    global df
    try:
        # Poll Kafka for new messages
        msg = consumer.poll(1.0)
        if msg is None:
            print("No new messages received from Kafka.")
            return dash.no_update
        if msg.error():
            raise KafkaException(msg.error())
 
        # Decode and log the Kafka message
        print(f"Received message from Kafka: {msg.value().decode('utf-8')}")
        data = eval(msg.value().decode('utf-8'))
        current_time = data["timestamp"]
        price = data["price"]
 
        # Append the new data to the DataFrame
        df = pd.concat([df, pd.DataFrame([[current_time, price]], columns=["Time", "Price"])], ignore_index=True)
    except Exception as e:
        print(f"Error: {e}")
        return dash.no_update
 
    # Create the plotly figure
    fig = go.Figure(
        data=[go.Scatter(
            x=df["Time"],
            y=df["Price"],
            mode="lines+markers",
            name="Bitcoin Price",
            line=dict(color='blue')
        )],
        layout=go.Layout(
            title="Live Bitcoin Price",
            xaxis_title="Time",
            yaxis_title="Price (USD)",
            xaxis=dict(showline=True, showgrid=False),
            yaxis=dict(showline=True, showgrid=True),
            margin=dict(l=40, r=40, t=40, b=40)
        )
    )
 
    return fig
 
 
# Run the Dash app
if __name__ == "__main__":
    app.run_server(debug=False)