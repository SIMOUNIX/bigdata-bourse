import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import pandas as pd
import sqlalchemy
import plotly.graph_objects as go
from datetime import date

from utils import (
    get_markets,
    get_companies,
    create_companies_options,
    get_daystocks,
    generate_menu_buttons,
    build_bollinger_content,
    build_candlestick_content,
    get_start_end_dates_for_company
)

external_stylesheets = [
    "https://codepen.io/chriddyp/pen/bWLwgP.css",
    "docker/dashboard/dashboard/assets/lookgood.css",
]

DATABASE_URI = 'timescaledb://ricou:monmdp@db:5432/bourse'    # inside docker
# DATABASE_URI = "timescaledb://ricou:monmdp@localhost:5432/bourse"  # outside docker
engine = sqlalchemy.create_engine(DATABASE_URI)

app = dash.Dash(
    __name__,
    title="Bourse",
    suppress_callback_exceptions=True,
    external_stylesheets=external_stylesheets,
)
server = app.server

# define layout
app.layout = html.Div(
    [
        html.Div(
            [
                html.Div(
                    [
                        html.Button(
                            "Overview",
                            id="btn-dashboard",
                            n_clicks=0,
                            className="btn btn-width",
                        ),
                        html.Button(
                            "Share Price",
                            id="btn-share-price",
                            n_clicks=0,
                            className="btn btn-width",
                        ),
                        html.Button(
                            "Bollinger Bands",
                            id="btn-bollinger-bands",
                            n_clicks=0,
                            className="btn btn-width",
                        ),
                        html.Button(
                            "Raw Data",
                            id="btn-raw-data",
                            n_clicks=0,
                            className="btn btn-width",
                        ),
                    ],
                    id="menu",
                    className="navbar",
                ),
            ],
            className="vertical-navbar",
        ),
        html.Div([html.Div(id="page-content")], className="main-content"),
    ],
    className="layout-container",
)

# Callback to update page content and active button
@app.callback(
    [Output("page-content", "children"), Output("menu", "children")],
    [
        Input("btn-dashboard", "n_clicks"),
        Input("btn-share-price", "n_clicks"),
        Input("btn-bollinger-bands", "n_clicks"),
        Input("btn-raw-data", "n_clicks"),
    ],
    [
        State("btn-dashboard", "id"),
        State("btn-share-price", "id"),
        State("btn-bollinger-bands", "id"),
        State("btn-raw-data", "id"),
    ],
)
def update_page_content(
    btn_home,
    btn_share_price,
    btn_bollinger_bands,
    btn_raw_data,
    id_home,
    id_share_price,
    id_bollinger_bands,
    id_raw_data,
):
    ctx = dash.callback_context
    if not ctx.triggered:
        button_id = "btn-dashboard"
    else:
        button_id = ctx.triggered[0]["prop_id"].split(".")[0]

    content, active_button_id = get_page_content(
        button_id, id_home, id_share_price, id_bollinger_bands, id_raw_data
    )

    menu_buttons = generate_menu_buttons(active_button_id)

    return content, menu_buttons


def get_page_content(
    button_id, id_home, id_share_price, id_bollinger_bands, id_raw_data
):
    if button_id == "btn-dashboard":
        content = html.Div([
                dcc.Textarea(
                    id='sql-query',
                    value='''
                        SELECT * FROM pg_catalog.pg_tables
                            WHERE schemaname != 'pg_catalog' AND 
                                  schemaname != 'information_schema';
                    ''',
                    style={'width': '100%', 'height': 100},
                    ),
                html.Button('Execute', id='execute-query', n_clicks=0),
                html.Div(id='query-result')
             ])
        active_button_id = id_home
    elif button_id == "btn-share-price":
        content = build_candlestick_content()
        active_button_id = id_share_price
    elif button_id == "btn-bollinger-bands":
        content = build_bollinger_content()
        active_button_id = id_bollinger_bands
    elif button_id == "btn-raw-data":
        content = "Raw Data Content"
        active_button_id = id_raw_data
    else:
        content = "Select an option from the menu"
        active_button_id = "btn-dashboard"

    return content, active_button_id

@app.callback( Output('query-result', 'children'),
               Input('execute-query', 'n_clicks'),
               State('sql-query', 'value'),
             )
def run_query(n_clicks, query):
    if n_clicks > 0:
        try:
            result_df = pd.read_sql_query(query, engine)
            return html.Pre(result_df.to_string())
        except Exception as e:
            return html.Pre(str(e))
    return "Enter a query and press execute."


# ------------------ Bollinger Bands ------------------

@app.callback(
    Output("company-selector-bollinger", "options"),
    Output("company-selector-bollinger", "value"),
    Output("bollinger-debug", "children"),
    Input("market-selector-bollinger", "value"),
    State("bollinger-debug", "children")
)
def update_bollinger_companies(market_id, old_debug_info):
    companies = get_companies(market_id)
    companies_options = create_companies_options(companies)
    
    # set default value to the first company
    default_company = companies_options[0]["value"]
    
    # update debug info
    debug_message = f"Market {market_id} selected"
    debug_info = old_debug_info + [html.Div(debug_message)]
    
    return companies_options, default_company, debug_info

@app.callback(
    Output("date-picker-bollinger", "start_date"),
    Output("date-picker-bollinger", "end_date"),
    # Output("bollinger-debug", "children"),
    Input("company-selector-bollinger", "value"),
    # State("bollinger-debug", "children")
)
def update_bollinger_date_range(company_id):
    start_date, end_date = get_start_end_dates_for_company(company_id).values[0]
    
    # # update debug info
    # debug_message = f"Company {company_id} selected"
    # debug_info = old_debug_info + [html.Div(debug_message)]
    
    return start_date, end_date#, debug_info

@app.callback(
    Output("bollinger-graph", "figure"),
    Input("company-selector-bollinger", "value"),
    Input("date-picker-bollinger", "start_date"),
    Input("date-picker-bollinger", "end_date"),
)
def update_bollinger_graph(company_id, start_date, end_date):
    df = get_daystocks(company_id, start_date, end_date)
    
    # order by date
    df = df.sort_values("date")
    
    # compute data for Bollinger Bands on all the period
    df["20_sma"] = df["close"].rolling(window=20).mean()
    df["20_std"] = df["close"].rolling(window=20).std()
    df["upper_band"] = df["20_sma"] + (df["20_std"] * 2)
    df["lower_band"] = df["20_sma"] - (df["20_std"] * 2)
    
    fig = go.Figure()
    
    # add traces for Close Price, 20-day SMA, Upper Band, and Lower Band
    fig.add_trace(go.Scatter(x=df["date"], y=df["close"], mode="lines", name="Close Price"))
    fig.add_trace(go.Scatter(x=df["date"], y=df["20_sma"], mode="lines", name="20-day SMA"))
    fig.add_trace(go.Scatter(x=df["date"], y=df["upper_band"], mode="lines", name="Upper Band"))
    fig.add_trace(go.Scatter(x=df["date"], y=df["lower_band"], mode="lines", name="Lower Band"))

    # update title
    fig.update_layout(title_text=f"Bollinger Bands for {company_id}", title_x=0.5)

    # update layout with axis titles, legend, etc.
    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Price",
        plot_bgcolor="white",
        font=dict(family="Arial", size=12),
        margin=dict(l=50, r=50, t=80, b=50),
        hovermode="x",
        showlegend=True,
        legend=dict(
            font=dict(family="Arial", size=10),
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
        xaxis=dict(gridcolor="lightgrey"),
        yaxis=dict(gridcolor="lightgrey"),
    )

    # Add range selector buttons for easy zooming
    fig.update_layout(
        xaxis=dict(
            rangeselector=dict(
                buttons=list([
                    dict(count=1, label="1m", step="month", stepmode="backward"),
                    dict(count=6, label="6m", step="month", stepmode="backward"),
                    dict(count=1, label="YTD", step="year", stepmode="todate"),
                    dict(count=1, label="1y", step="year", stepmode="backward"),
                    dict(step="all")
                ])
            ),
            rangeslider=dict(visible=True),
            type="date"
        )
    )

    return fig
    

# @app.callback(
#     Output("candlestick-graph", "figure"),
#     [
#         Input("candlestick-selector", "value"),
#         Input("date-picker", "start_date"),
#         Input("date-picker", "end_date"),
#     ],
# )
# def update_candlestick_graph(selected_markets, start_date, end_date):
#     if not selected_markets:
#         return {}

#     dfs = generate_data_for_selected_markets(selected_markets)

#     fig = go.Figure()
#     for df, color in dfs:
#         filtered_df = df[(df["Date"] >= start_date) & (df["Date"] <= end_date)]
#         decreasing_color = lighten_color(color)
#         fig.add_trace(
#             go.Candlestick(
#                 x=filtered_df["Date"],
#                 open=filtered_df["Open"],
#                 high=filtered_df["High"],
#                 low=filtered_df["Low"],
#                 close=filtered_df["Close"],
#                 name=filtered_df["Market"].iloc[0],
#                 increasing_line_color=color,
#                 decreasing_line_color=decreasing_color,
#                 line=dict(width=1),  # Customize candlestick width
#             )
#         )

#     fig.update_layout(
#         title="Candlestick Chart",
#         xaxis_title="Date",
#         yaxis_title="Price",
#         plot_bgcolor="white",  # Set plot background color
#         font=dict(family="Arial", size=12),
#         margin=dict(l=50, r=50, t=80, b=50),  # Adjust margins
#         hovermode="x",
#         showlegend=True,
#         legend=dict(
#             font=dict(family="Arial", size=10),
#             orientation="h",
#             yanchor="bottom",
#             y=1.02,
#             xanchor="right",
#             x=1,
#         ),
#         xaxis=dict(gridcolor="lightgrey"),  # Customize grid lines
#         yaxis=dict(gridcolor="lightgrey"),  # Customize grid lines
#     )

#     return fig


# def generate_data_for_selected_markets(selected_markets):
#     dfs = []
#     colors = {"Market 1": "#636EFA", "Market 2": "#EF553B", "Market 3": "#00CC96"}

#     for market in selected_markets:
#         if market == "Market 1":
#             df = generate_data_candlestick(100)
#             df["Market"] = "Market 1"
#         elif market == "Market 2":
#             df = generate_data_candlestick(100)
#             df["Market"] = "Market 2"
#         elif market == "Market 3":
#             df = generate_data_candlestick(100)
#             df["Market"] = "Market 3"
#         dfs.append((df, colors[market]))

#     return dfs


def lighten_color(color, factor=0.5):
    """Lighten the given color."""
    color = color.lstrip("#")
    rgb = tuple(int(color[i : i + 2], 16) for i in (0, 2, 4))
    lightened_rgb = tuple(int((255 - c) * factor + c) for c in rgb)
    lightened_color = "#{:02x}{:02x}{:02x}".format(*lightened_rgb)
    return lightened_color


if __name__ == "__main__":
    app.run_server(debug=True, port=8050)
