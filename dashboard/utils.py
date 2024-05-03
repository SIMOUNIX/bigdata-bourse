import pandas as pd
import numpy as np

from dash import dcc, html
import plotly.graph_objects as go
import sqlalchemy

# DATABASE_URI = 'timescaledb://ricou:monmdp@db:5432/bourse'    # inside docker
DATABASE_URI = "timescaledb://ricou:monmdp@localhost:5432/bourse"  # outside docker
engine = sqlalchemy.create_engine(DATABASE_URI)

np.random.seed(0)
num_days = 100

def get_companies(mid):
    """
    Get companies from a market
    """
    query = f"SELECT * FROM companies WHERE mid = '{mid}'"
    df = pd.read_sql(query, engine)
    return df

def get_daystocks(cid, start_date, end_date):
    """
    Get daystocks from a company between starting and ending dates
    """
    query = f"SELECT * FROM daystocks WHERE cid = '{cid}' AND date >= '{start_date}' AND date <= '{end_date}'"
    df = pd.read_sql(query, engine)
    return df

def get_multiple_daystocks(cids, start_date, end_date):
    """
    Get daystocks from multiple companies between starting and ending dates
    cids: list of companies ids
    """
    query = f"SELECT * FROM daystocks WHERE cid IN {tuple(cids)} AND date >= '{start_date}' AND date <= '{end_date}'" # tuple(cids) to avoid SQL injection
    df = pd.read_sql(query, engine)
    return df

def generate_random_data(num_days, volatility, trend, noise_level):
    window_size = 20
    dates = pd.date_range(start="2024-01-01", periods=num_days)
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
        "Date": dates,
        "Close": prices,
        "SMA": rolling_mean,
        "UB": upper_band,
        "LB": lower_band,
    }
    df = pd.DataFrame(data)
    return df


def get_data_bollinger_bands(market):
    query = f"SELECT * FROM daystocks WHERE cid = '{market}'"
    df = pd.read_sql(query, engine)
    return df


def generate_every_year_total_sales():
    years = ["2018", "2019", "2020", "2021", "2022"]
    sales = [500, 600, 700, 800, 900]

    data = {"Year": years, "Sales": sales}
    df = pd.DataFrame(data)
    return df


def build_every_year_total_sales_content():
    df = generate_every_year_total_sales()

    # Selector to choose the year
    year_selector = dcc.Dropdown(
        id="year-selector",
        options=[{"label": year, "value": year} for year in df["Year"]],
        value=df["Year"].iloc[0],
    )

    # Display the value of the selected year
    selected_year_text = html.Div(id="selected-year-text")

    return year_selector, selected_year_text, df


def generate_data_candlestick(num_days):
    dates = pd.date_range(start="2024-01-01", periods=num_days)
    prices = np.cumsum(np.random.randn(num_days))

    data = {
        "Date": dates,
        "Open": prices,
        "High": prices + np.random.rand(num_days),
        "Low": prices - np.random.rand(num_days),
        "Close": prices + np.random.randn(num_days),
    }
    df = pd.DataFrame(data)
    return df


def generate_menu_buttons(active_button_id):
    menu_buttons = [
        html.Button(
            "Overview", id="btn-dashboard", n_clicks=0, className="btn btn-width"
        ),
        html.Button(
            "Share Price", id="btn-share-price", n_clicks=0, className="btn btn-width"
        ),
        html.Button(
            "Bollinger Bands",
            id="btn-bollinger-bands",
            n_clicks=0,
            className="btn btn-width",
        ),
        html.Button(
            "Raw Data", id="btn-raw-data", n_clicks=0, className="btn btn-width"
        ),
    ]
    for button in menu_buttons:
        if button.id == active_button_id:
            button.className += " active"

    return menu_buttons


def build_bollinger_content():
    # Generate random data for multiple markets
    df = generate_random_data(100, volatility=0.5, trend=0.1, noise_level=0.1)
    df1 = generate_random_data(100, volatility=0.5, trend=0.3, noise_level=0.2)
    df2 = generate_random_data(100, volatility=0.5, trend=0.2, noise_level=0.3)

    # Associate name to each market
    df["Market"] = "Market 1"
    df1["Market"] = "Market 2"
    df2["Market"] = "Market 3"

    # Combine dataframes
    combined_df = pd.concat([df, df1, df2])

    # Dropdown to select market
    market_selector = dcc.Dropdown(
        id="market-selector",
        options=[
            {"label": market, "value": market}
            for market in combined_df["Market"].unique()
        ],
        value=combined_df["Market"].iloc[0],
        style={"width": "50%"},
    )

    date_picker = dcc.DatePickerRange(
        id="date-picker",
        start_date=combined_df["Date"].iloc[0],
        end_date=combined_df["Date"].iloc[-1],
        minimum_nights=20,  # Minimum number of nights between start and end date, 20 because we need information for the Bollinger Bands
    )

    selector_div = html.Div(
        [market_selector, date_picker], className="selector-div main-content-children"
    )

    return html.Div(
        [
            selector_div,
            dcc.Graph(id="bollinger-graph", className="main-content-children"),
        ]
    )


def build_candlestick_content():
    # Generate random data for multiple markets
    df = generate_data_candlestick(100)
    df1 = generate_data_candlestick(100)
    df2 = generate_data_candlestick(100)

    # Associate name to each market
    df["Market"] = "Market 1"
    df1["Market"] = "Market 2"
    df2["Market"] = "Market 3"

    # Combine dataframes
    combined_df = pd.concat([df, df1, df2])

    # Dropdown to select markets
    market_checkboxes = dcc.Checklist(
        id="candlestick-selector",
        options=[
            {"label": market, "value": market}
            for market in combined_df["Market"].unique()
        ],
        value=[],
    )

    date_picker = dcc.DatePickerRange(
        id="date-picker",
        start_date=combined_df["Date"].iloc[0],
        end_date=combined_df["Date"].iloc[-1],
    )

    selector_div = html.Div(
        [market_checkboxes, date_picker], className="selector-div main-content-children"
    )

    return html.Div(
        [
            selector_div,
            dcc.Graph(id="candlestick-graph", className="main-content-children"),
        ]
    )
