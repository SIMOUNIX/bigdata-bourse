# FILE THAT CONTAINS THE FUNCTION TO GENERATE RANDOM DATA FOR THE DASHBOARD

import pandas as pd
import numpy as np

from dash import dcc 
import plotly.graph_objects as go

np.random.seed(0)
num_days = 100

def generate_random_data():
    window_size = 20
    dates = pd.date_range(start='2024-01-01', periods=num_days)
    prices = np.cumsum(np.random.randn(num_days)) + 100

    rolling_mean = pd.Series(prices).rolling(window=window_size).mean()
    rolling_std = pd.Series(prices).rolling(window=window_size).std()

    # Calculate upper and lower Bollinger Bands
    upper_band = rolling_mean + 2 * rolling_std
    lower_band = rolling_mean - 2 * rolling_std

    # Create DataFrame
    data = {
        'Date': dates,
        'Close': prices,
        'SMA': rolling_mean,
        'UB': upper_band,
        'LB': lower_band
    }
    df = pd.DataFrame(data)
    return df

def build_bollinger_content():
    df = generate_random_data()
    
    fig = go.Figure()
    
    # Adding traces for each line
    fig.add_trace(go.Scatter(x=df['Date'], y=df['Close'], mode='lines', name='Close'))
    fig.add_trace(go.Scatter(x=df['Date'], y=df['SMA'], mode='lines', name='SMA', line=dict(color='blue', width=2, dash='dot')))
    fig.add_trace(go.Scatter(x=df['Date'], y=df['UB'], mode='lines', name='Upper Band', line=dict(color='green', width=1, dash='dash')))
    fig.add_trace(go.Scatter(x=df['Date'], y=df['LB'], mode='lines', name='Lower Band', line=dict(color='red', width=1, dash='dash'), fill='tonexty', fillcolor='rgba(255, 0, 0, 0.1)'))
    
    # Adding hover information
    fig.update_traces(hoverinfo='x+y', hovertemplate='%{x}<br>%{y:.2f}')
    
    # Adjusting axis labels and title
    fig.update_layout(title='Bollinger Bands', xaxis_title='Date', yaxis_title='Price')
    
    # Setting y-axis range
    min_price = min(df['LB'].min(), df['Close'].min())
    max_price = max(df['UB'].max(), df['Close'].max())
    fig.update_yaxes(range=[min_price, max_price])
    
    # Adding annotations for crossing points
    for index, row in df.iterrows():
        if row['Close'] > row['UB']:
            fig.add_annotation(x=row['Date'], y=row['Close'], text="Above Upper Band", showarrow=True, arrowhead=1, arrowcolor='red', ax=0, ay=-40)
        elif row['Close'] < row['LB']:
            fig.add_annotation(x=row['Date'], y=row['Close'], text="Below Lower Band", showarrow=True, arrowhead=1, arrowcolor='green', ax=0, ay=40)
    
    return dcc.Graph(figure=fig)

