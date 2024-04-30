# FILE THAT CONTAINS THE FUNCTION TO GENERATE RANDOM DATA FOR THE DASHBOARD

import pandas as pd
import numpy as np

from dash import dcc, html
import plotly.graph_objects as go

np.random.seed(0)
num_days = 100

def generate_random_data(num_days, volatility, trend, noise_level):
    window_size = 20
    dates = pd.date_range(start='2024-01-01', periods=num_days)
    prices = np.cumsum(np.random.randn(num_days) * volatility) + trend

    rolling_mean = pd.Series(prices).rolling(window=window_size).mean()
    rolling_std = pd.Series(prices).rolling(window=window_size).std()

    # Calculate upper and lower Bollinger Bands
    upper_band = rolling_mean + 2 * rolling_std
    lower_band = rolling_mean - 2 * rolling_std

    # Add noise
    prices += np.random.randn(num_days) * noise_level

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

def generate_every_year_total_sales():
    years = ['2018', '2019', '2020', '2021', '2022']
    sales = [500, 600, 700, 800, 900]

    data = {
        'Year': years,
        'Sales': sales
    }
    df = pd.DataFrame(data)
    return df

def build_every_year_total_sales_content():
    df = generate_every_year_total_sales()

    # Selector to choose the year
    year_selector = dcc.Dropdown(
        id='year-selector',
        options=[{'label': year, 'value': year} for year in df['Year']],
        value=df['Year'].iloc[0]
    )

    # Display the value of the selected year
    selected_year_text = html.Div(id='selected-year-text')

    return year_selector, selected_year_text, df

