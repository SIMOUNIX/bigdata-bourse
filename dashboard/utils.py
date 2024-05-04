import pandas as pd
import numpy as np

from dash import dcc, html
import plotly.graph_objects as go
import sqlalchemy

DATABASE_URI = 'timescaledb://ricou:monmdp@db:5432/bourse'    # inside docker
# DATABASE_URI = "timescaledb://ricou:monmdp@localhost:5432/bourse"  # outside docker
engine = sqlalchemy.create_engine(DATABASE_URI)

np.random.seed(0)
num_days = 100


def get_markets():
    """function to get all markets

    Returns:
        pd.DataFrame: all the markets
    """
    markets = [
        {"mid": 7, "name": "Paris compartiment A", "alias": "compA"},
        {"mid": 8, "name": "Paris compartiment B", "alias": "compB"},
        {"mid": 1, "name": "Euronext", "alias": "euronx"},
        {"mid": 6, "name": "Amsterdam", "alias": "amsterdam"},
    ]
    df = pd.DataFrame(markets)

    return df


def get_companies(mid):
    """function to get all companies from a market

    Args:
        mid (int): market id

    Returns:
        pd.DataFrame: companies
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
    """function to get multiple daystocks from multiple companies between starting and ending dates

    Args:
        cids (list[int]): list of company ids 
        start_date (timestamp): the start date
        end_date (timestamp): the end date

    Returns:
        pd.DatFrame: daystocks
    """
    query = f"SELECT * FROM daystocks WHERE cid IN {tuple(cids)} AND date >= '{start_date}' AND date <= '{end_date}'"  # tuple(cids) to avoid SQL injection
    df = pd.read_sql(query, engine)
    return df


def get_start_end_dates_for_selected_companies(cids):
    """function to get the start and end dates of the daystocks table for selected companies

    Returns:
        pd.DataFrame: the start and end dates
    """
    query = f"SELECT MIN(date) as start_date, MAX(date) as end_date FROM daystocks WHERE cid IN {tuple(cids)}"
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
    """function to build the initial content of the Bollinger Bands page

    Returns:
        html.Div: the initial content of the Bollinger Bands page
    """
    # get markets
    markets = get_markets()

    # dropdown to select market
    market_selector = dcc.Dropdown(
        id="market-selector",
        options=[
            {"label": market["name"], "value": market["mid"]}
            for index, market in markets.iterrows()
        ],
        value=markets["mid"].iloc[0],
        style={"width": "50%"},
    )

    # get companies
    companies = get_companies(market_selector.value)

    # companies selector
    # search bar to filter companies and select only one at a time
    # use DataList
    company_selector = html.Div([
        html.Datalist(
            id="company-selector",
            children=[html.Option(value=company["cid"], label=company["name"])
                      for index, company in companies.iterrows()]
        ),
        dcc.Input(
            id="company-input",
            list="company-selector",
            value=''
        )]
    )

    # get start and end dates
    start_date, end_date = get_start_end_dates_for_selected_companies(
        companies["cid"].tolist()).values[0]  # initial dates init with the first company
    # TODO: still have to check if the dates are correct

    # date selector
    date_picker = dcc.DatePickerRange(
        id="date-picker",
        start_date=start_date,
        end_date=end_date,
        minimum_nights=20,  # Minimum number of nights between start and end date, 20 because we need information for the Bollinger Bands
    )

    selector_div = html.Div(
        [market_selector, company_selector,
            date_picker], className="selector-div main-content-children"
    )

    return html.Div(
        [
            selector_div,
            dcc.Graph(id="bollinger-graph", className="main-content-children"),
        ]
    )


def build_candlestick_content():
    """function to build the initial content of the Candlestick page

    Returns:
        html.Div: the initial content of the Candlestick page
    """
    # get markets
    markets = get_markets()

    # dropdown to select market
    market_selector = dcc.Dropdown(
        id="market-selector",
        options=[
            {"label": market["name"], "value": market["mid"]}
            for index, market in markets.iterrows()
        ],
        value=markets["mid"].iloc[0],
        style={"width": "50%"},
    )

    # get companies
    companies = get_companies(market_selector.value)

    # companies selector
    # search bar to filter companies and can select multiple companies
    # use DataList
    # we can remove selected companies
    company_selector = html.Div([
        html.Datalist(
            id="company-selector",
            children=[html.Option(value=company["cid"], label=company["name"])
                      for index, company in companies.iterrows()]
        ),
        dcc.Input(
            id="company-input",
            list="company-selector",
            value=''
        ),
        # add section to display selected companies and remove them
        html.Div(id="selected-companies")]
    )

    # get start and end dates
    start_date, end_date = get_start_end_dates_for_selected_companies(
        companies["cid"].tolist()).values[0]  # initial dates init with the first company

    date_picker = dcc.DatePickerRange(
        id="date-picker",
        start_date=start_date,
        end_date=end_date,
    )

    selector_div = html.Div(
        [market_selector, company_selector,
            date_picker], className="selector-div main-content-children"
    )

    return html.Div(
        [
            selector_div,
            dcc.Graph(id="candlestick-graph",
                      className="main-content-children"),
        ]
    )
