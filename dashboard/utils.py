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

def create_markets_options(markets_df):
    """function to create markets options for dropdown

    Args:
        markets_df (pd.DataFrame): markets

    Returns:
        list[dict]: markets options
    """
    markets_options = []
    for index, market in markets_df.iterrows():
        markets_options.append(
            {"label": market["name"], "value": market["mid"]}
        )
    
    return markets_options


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

def create_companies_options(companies_df):
    """function to create companies options for dropdown

    Args:
        companies_df (pd.DataFrame): companies

    Returns:
        list[dict]: companies options
    """
    companies_options = []
    for index, company in companies_df.iterrows():
        companies_options.append(
            {"label": company["name"], "value": company["id"]}
        )
        
    # sort companies by label
    companies_options = sorted(companies_options, key=lambda x: x["label"])
    return companies_options


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
    
    cids_str = ','.join(str(cid) for cid in cids)
    
    # select all daystocks from the selected companies between the start and end dates
    query = f"SELECT * FROM daystocks WHERE cid IN ({cids_str}) AND date >= '{start_date}' AND date <= '{end_date}'"
    df = pd.read_sql(query, engine)
    return df


def get_start_end_dates_for_selected_companies(cids):
    """function to get the start and end dates of the daystocks table for selected companies

    Returns:
        pd.DataFrame: the start and end dates
    """
    if not cids:
        return pd.DataFrame({"start_date": pd.Timestamp.now(), "end_date": pd.Timestamp.now()})
    
    # format cids into a string
    cids_str = ','.join(str(cid) for cid in cids)

    # use parameterized query to avoid SQL injection
    query = "SELECT MIN(date) as start_date, MAX(date) as end_date FROM daystocks WHERE cid IN (%s)"
    df = pd.read_sql_query(query % cids_str, engine)
    return df

def get_start_end_dates_for_company(cid):
    """function to get the start and end dates of the daystocks table for a company

    Returns:
        pd.DataFrame: the start and end dates
    """
    query = f"SELECT MIN(date) as start_date, MAX(date) as end_date FROM daystocks WHERE cid = '{cid}'"
    df = pd.read_sql(query, engine)
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
    markets_options = create_markets_options(markets)
    
    # dropdown to select market
    market_selector = dcc.Dropdown(
        id="market-selector-bollinger",
        options=markets_options,
        value=markets_options[0]["value"],
        style={"width": "50%"},
        placeholder="Séléctionner un marché",
        clearable=False,
        multi=False
    )

    # get companies
    companies = get_companies(market_selector.value)
    companies_options = create_companies_options(companies)  
    
    # companies selector
    # search bar to filter companies and select only one at a time
    company_selector = dcc.Dropdown(
        id="company-selector-bollinger",
        options=companies_options,
        value=companies_options[0]["value"],
        style={"width": "50%"},
        placeholder="Séléctionner une entreprise",
        clearable=False,
        multi=False
    )

    # instantiate the date picker with the start and end dates of the selected company
    start_date, end_date = get_start_end_dates_for_company(companies_options[0]["value"]).values[0]

    # date selector
    date_picker = dcc.DatePickerRange(
        id="date-picker-bollinger",
        start_date=start_date,
        end_date=end_date,
        minimum_nights=20,  # Minimum number of nights between start and end date, 20 because we need information for the Bollinger Bands
    )

    selector_div = html.Div(
        [market_selector, company_selector,date_picker], 
        className="selector-div main-content-children"
    )

    return html.Div(
        [
            selector_div,
            dcc.Graph(id="bollinger-graph", className="main-content-children"),
            html.Div(id="bollinger-debug",
                     className="main-content-children",
                     children=["Debug info"]),
        ]
    )

def build_candlestick_content():
    """function to build the initial content of the Candlestick page

    Returns:
        html.Div: the initial content of the Candlestick page
    """
    # get markets
    markets = get_markets()
    markets_options = create_markets_options(markets)

    # dropdown to select market
    market_selector = dcc.Dropdown(
        id="market-selector-candlestick",
        options=markets_options,
        value=markets_options[0]["value"],
        style={"width": "50%"},
        clearable=False,
        multi=False
    )

    # get companies
    companies = get_companies(market_selector.value)
    companies_options = create_companies_options(companies)

    # companies selector
    # we can select multiple companies
    # we can remove selected companies
    company_selector = dcc.Dropdown(
        id="company-selector-candlestick",
        options=companies_options,
        value=[companies_options[0]["value"]],
        style={"width": "50%"},
        placeholder="Séléctionner une ou plusieurs entreprises",
        clearable=True,
        multi=True,
    )

    # instantiate the date picker with the today date
    start_date, end_date = get_start_end_dates_for_company(companies_options[0]["value"]).values[0]
    
    date_picker = dcc.DatePickerRange(
        id="date-picker-candlestick",
        start_date=start_date,
        end_date=end_date,
    )

    selector_div = html.Div(
        [market_selector, company_selector, date_picker],
        className="selector-div main-content-children"
    )

    return html.Div(
        [
            selector_div,
            dcc.Graph(id="candlestick-graph",
                      className="main-content-children"),
            html.Div(id="candlestick-debug",
                        className="main-content-children",
                        children=["Debug info"]),
        ]
    )
