import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import pandas as pd
import sqlalchemy
import plotly.graph_objects as go
from datetime import date

from utils import (
    get_companies,
    create_companies_options,
    get_company_name,
    get_daystocks,
    get_multiple_daystocks,
    generate_menu_buttons,
    build_bollinger_content,
    build_candlestick_content,
    get_start_end_dates_for_company,
    get_start_end_dates_for_selected_companies,
    build_raw_data_content,
    build_dashboard_overview,
    build_sp500_ytd_content
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
                        html.Button(
                            "YTD",
                            id="btn-sp500-ytd",
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

# callback to update page content and active button
@app.callback(
    [Output("page-content", "children"), Output("menu", "children")],
    [
        Input("btn-dashboard", "n_clicks"),
        Input("btn-share-price", "n_clicks"),
        Input("btn-bollinger-bands", "n_clicks"),
        Input("btn-raw-data", "n_clicks"),
        Input("btn-sp500-ytd", "n_clicks"),
    ],
    [
        State("btn-dashboard", "id"),
        State("btn-share-price", "id"),
        State("btn-bollinger-bands", "id"),
        State("btn-raw-data", "id"),
        State("btn-sp500-ytd", "id"),
    ],
)
def update_page_content(btn_home, btn_share_price, btn_bollinger_bands, btn_raw_data, btn_sp500_ytd, id_home, id_share_price, id_bollinger_bands, id_raw_data, id_sp500_ytd):
    ctx = dash.callback_context
    if not ctx.triggered:
        button_id = "btn-dashboard"
    else:
        button_id = ctx.triggered[0]["prop_id"].split(".")[0]

    content, active_button_id = get_page_content(button_id, id_home, id_share_price, id_bollinger_bands, id_raw_data, id_sp500_ytd)

    menu_buttons = generate_menu_buttons(active_button_id)

    return content, menu_buttons

def get_page_content(button_id, id_home, id_share_price, id_bollinger_bands, id_raw_data, id_sp500_ytd):
    if button_id == "btn-dashboard":
        content = build_dashboard_overview()
        active_button_id = id_home
    elif button_id == "btn-share-price":
        content = build_candlestick_content()
        active_button_id = id_share_price
    elif button_id == "btn-bollinger-bands":
        content = build_bollinger_content()
        active_button_id = id_bollinger_bands
    elif button_id == "btn-raw-data":
        content = build_raw_data_content()
        active_button_id = id_raw_data
    elif button_id == "btn-sp500-ytd":
        content = build_sp500_ytd_content()
        active_button_id = id_sp500_ytd
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
    Input("company-selector-bollinger", "value"),
)
def update_bollinger_date_range(company_id):
    start_date, end_date = get_start_end_dates_for_company(company_id).values[0]
    return start_date, end_date

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
    fig.update_layout(title_text=f"Bollinger Bands for {get_company_name(company_id)}", title_x=0.5)

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
    
# ------------------ End Bollinger Bands ------------------

# ------------------ Candlestick Chart --------------------

@app.callback(
    Output("company-selector-candlestick", "options"),
    Output("company-selector-candlestick", "value"),
    Input("market-selector-candlestick", "value"),
)
def update_candlestick_companies(market_id):
    companies = get_companies(market_id)
    companies_options = create_companies_options(companies)
    
    # set default value to the first company
    default_company = [companies_options[0]["value"]]
    
    return companies_options, default_company

@app.callback(
    Output("date-picker-candlestick", "start_date"),
    Output("date-picker-candlestick", "end_date"),
    Output("candlestick-debug", "children"),
    Input("company-selector-candlestick", "value"),
    State("candlestick-debug", "children"),
)
def update_candlestick_date_range(companies_ids, old_debug_info):
    companies_list = list(companies_ids)
    
    start_date, end_date = get_start_end_dates_for_selected_companies(companies_list).values[0]
    # add the selected companies to the debug info
    debug_info = old_debug_info + [html.Div(f"Companies selected: {companies_list}")]
    return start_date, end_date, debug_info

@app.callback(
    Output("candlestick-graph", "figure"),
    Input("company-selector-candlestick", "value"),
    Input("date-picker-candlestick", "start_date"),
    Input("date-picker-candlestick", "end_date"),
    Input("graph-type-selector", "value"),
)
def update_candlestick_graph(companies_ids, start_date, end_date, graph_type):
    companies_list = list(companies_ids)

    if not companies_list:
        return {}    

    df = get_multiple_daystocks(companies_list, start_date, end_date)
    
    # order by date
    df = df.sort_values("date")
    
    # add different color for each company
    colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf", "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf"]
    for i, company_id in enumerate(companies_list):
        df.loc[df["cid"] == company_id, "color"] = colors[i]
    
    fig = go.Figure()
    for company_id in companies_list:
        company_df = df[df["cid"] == company_id]
        
        if graph_type == 'candlestick':
            fig.add_trace(go.Candlestick(
                x=company_df["date"],
                open=company_df["open"],
                high=company_df["high"],
                low=company_df["low"],
                close=company_df["close"],
                name=get_company_name(company_id),
                increasing_line_color=company_df["color"].iloc[0],
                decreasing_line_color=lighten_color(company_df["color"].iloc[0]),
            ))
        elif graph_type == 'line':
            fig.add_trace(go.Scatter(
                x=company_df["date"],
                y=company_df["close"],
                mode="lines",
                name=get_company_name(company_id),
                line=dict(color=company_df["color"].iloc[0]),
            ))
            
        
    fig.update_layout(
        title="Candlestick Chart",
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
    
    return fig

def lighten_color(color, factor=0.5):
    """Lighten the given color."""
    color = color.lstrip("#")
    rgb = tuple(int(color[i : i + 2], 16) for i in (0, 2, 4))
    lightened_rgb = tuple(int((255 - c) * factor + c) for c in rgb)
    lightened_color = "#{:02x}{:02x}{:02x}".format(*lightened_rgb)
    return lightened_color

# ------------------ End Candlestick Chart ----------------

# ------------------ Raw Data -----------------------------

@app.callback(
    Output("company-selector-raw-data", "options"),
    Output("company-selector-raw-data", "value"),
    Input("market-selector-raw-data", "value"),
)
def update_raw_data_companies(market_id):
    companies = get_companies(market_id)
    companies_options = create_companies_options(companies)
    
    # set default value to the first company
    default_company = [companies_options[0]["value"]]
    
    return companies_options, default_company

@app.callback(
    Output("date-picker-raw-data", "start_date"),
    Output("date-picker-raw-data", "end_date"),
    Output("raw-data-debug", "children"),
    Input("company-selector-raw-data", "value"),
    State("raw-data-debug", "children"),
)
def update_raw_data_date_range(companies_ids, old_debug_info):
    companies_list = list(companies_ids)
    
    start_date, end_date = get_start_end_dates_for_selected_companies(companies_list).values[0]
    # add the selected companies to the debug info
    debug_info = old_debug_info + [html.Div(f"Companies selected: {companies_list}")]
    return start_date, end_date, debug_info

@app.callback(
    Output("raw-data-table", "data"),
    Input("company-selector-raw-data", "value"),
    Input("date-picker-raw-data", "start_date"),
    Input("date-picker-raw-data", "end_date"),
)
def update_raw_data_table(companies_ids, start_date, end_date):
    companies_list = list(companies_ids)

    if not companies_list:
        return []    

    df = get_multiple_daystocks(companies_list, start_date, end_date)
    
    # format the date column: yyyy/mm/dd   
    df["date"] = df["date"].dt.strftime("%Y/%m/%d")

    # add company name column
    df["name"] = df["cid"].apply(get_company_name)
    
    # reorder columns
    df = df[["date", "name", "open", "close", "high", "low", "volume"]] # include cid?
    
    # order by date
    df = df.sort_values("date")
    
    return df.to_dict("records")

# ------------------ End Raw Data -------------------------

# ------------------ SP500 & YTD --------------------------

@app.callback(
    Output("company-selector-sp500-ytd", "options"),
    Output("company-selector-sp500-ytd", "value"),
    Input("market-selector-sp500-ytd", "value"),
)
def update_sp500_ytd_companies(market_id):
    companies = get_companies(market_id)
    companies_options = create_companies_options(companies)
    
    # set default value to the first company
    default_company = companies_options[0]["value"]
    
    return companies_options, default_company

@app.callback(
    Output("sp500-ytd-graph", "figure"),
    Output("sp500-ytd-debug", "children"),
    Input("company-selector-sp500-ytd", "value"),
    State("sp500-ytd-debug", "children"),
)
def update_sp500_ytd_graph(company_id, old_debug_info):
    df = get_daystocks(company_id, None, None)
    # order by date
    df = df.sort_values("date")
    
    # compute the YTD value for each year
    df["year"] = df["date"].dt.strftime("%Y")
    df["ytd"] = (df["close"] - df.groupby("year")["close"].transform("first")) / df.groupby("year")["close"].transform("first") * 100
    
    fig = go.Figure()

    # determine the color based on the latest YTD value
    latest_ytd = df["ytd"].iloc[-1]
    line_color = "green" if latest_ytd >= 0 else "red"
    # above is not working

    fig.add_trace(go.Scatter(
        x=df["date"], 
        y=df["ytd"], 
        mode="lines", 
        name="YTD",
        line=dict(color=line_color)
    ))
    
    # add a horizontal line at y=0
    fig.add_shape(
        type="line",
        x0=df["date"].min(),
        y0=0,
        x1=df["date"].max(),
        y1=0,
        line=dict(color="black", width=1, dash="dash")
    )

    company_name = get_company_name(company_id)
    
    fig.update_layout(
        title=f"YTD for {company_name}",
        xaxis_title="Date",
        yaxis_title="YTD (%)",
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

    new_debug_info = old_debug_info + [html.Div(f"Company {company_id} selected")]
    
    return fig, new_debug_info    

if __name__ == "__main__":
    app.run_server(debug=True, port=8050)
