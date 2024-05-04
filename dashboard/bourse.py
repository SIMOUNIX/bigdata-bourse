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
    get_daystocks,
    get_multiple_daystocks,
    get_start_end_dates_for_selected_companies,
    generate_menu_buttons,
    build_bollinger_content,
    build_candlestick_content,
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
        content = "Dashboard Content"
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

@app.callback(
    [
        Output("date-picker", "start_date"),
        Output("date-picker", "end_date"),
    ],
    [
        Input("market-selector", "value"),
        Input("company-input", "value"),
    ],
)
def update_date_range_for_company_selection(market_id, company_id):
    # get the start and end dates for the selected company
    start_date, end_date = get_start_end_dates_for_selected_companies(company_id).values[0]
    return start_date, end_date

@app.callback(
    Output("bollinger-graph", "figure"),
    [
        Input("market-selector", "value"),
        Input("company-input", "value"),
        Input("date-picker", "start_date"),
        Input("date-picker", "end_date"),
    ],
)
def update_bollinger_graph(market_id, company_id, start_date, end_date):
    # retrieve data for the selected market, company, and date range
    daystocks_data = get_daystocks(company_id, start_date, end_date)
    
    # compute the mean and standard deviation of the closing prices
    rolling_mean = daystocks_data["close"].rolling(window=20).mean()
    rolling_std = daystocks_data["close"].rolling(window=20).std()
    upper_band = rolling_mean + (rolling_std * 2)
    lower_band = rolling_mean - (rolling_std * 2)

    # update the Bollinger Bands graph figure
    
    # add the price line to the figure
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=daystocks_data["date"],
            y=daystocks_data["close"],
            mode="lines",
            name="Close",
            line=dict(color="royalblue"),
        )
    )
    
    # add the SMA
    fig.add_trace(
        go.Scatter(
            x=daystocks_data["date"],
            y=rolling_mean,
            mode="lines",
            name="SMA",
            line=dict(color="darkorange", width=2, dash="dot"),
        )
    )
    
    # add the highest values
    fig.add_trace(
        go.Scatter(
            x=daystocks_data["date"],
            y=upper_band,
            mode="lines",
            name="Upper Band",
            line=dict(color="forestgreen", width=1, dash="dash"),
        )
    )
    
    # add the lowest values
    fig.add_trace(
        go.Scatter(
            x=daystocks_data["date"],
            y=lower_band,
            mode="lines",
            name="Lower Band",
            line=dict(color="firebrick", width=1, dash="dash"),
            fill="tonexty",
            fillcolor="rgba(255, 0, 0, 0.1)",
        )
    )
    
    # update layout: style the figure
    fig.update_layout(
        title="Bollinger Bands",
        xaxis_title="Date",
        yaxis_title="Price",
        font=dict(family="Arial", size=12),
        plot_bgcolor="white",  # Set plot background color
        margin=dict(l=50, r=50, t=80, b=50),  # Adjust margins
        hovermode="x",
        xaxis=dict(gridcolor="lightgrey"),  # Customize grid lines
        yaxis=dict(gridcolor="lightgrey"),  # Customize grid lines
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
