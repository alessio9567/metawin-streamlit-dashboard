import requests
import plotly.express as px
import pandas as pd

time_period = 30

url = "https://api.alternative.me/fng/?limit={}".format(time_period)

response = requests.get(url)

if response.status_code == 200:
    df = pd.DataFrame(response.json()['data'])

    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')

    df['value'] = df['value'].astype(int)

    fig = px.line(
        df,
        x="timestamp",
        y="value",
        title="Fear and Greed (Last {} days)".format(time_period),
        width=800,
        height=400
    )
    fig.show()

else:
    print("Failed to get JSON Data from URL")

