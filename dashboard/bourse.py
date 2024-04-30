import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import pandas as pd
import sqlalchemy
import plotly.graph_objects as go

from utils import build_every_year_total_sales_content, generate_random_data

external_stylesheets = [
    'https://codepen.io/chriddyp/pen/bWLwgP.css',
    'docker/dashboard/dashboard/assets/lookgood.css'
]

# DATABASE_URI = 'timescaledb://ricou:monmdp@db:5432/bourse'    # inside docker
DATABASE_URI = 'timescaledb://ricou:monmdp@localhost:5432/bourse'  # outside docker
engine = sqlalchemy.create_engine(DATABASE_URI)

app = dash.Dash(__name__, title="Bourse", suppress_callback_exceptions=True, external_stylesheets=external_stylesheets)
server = app.server

# Define the layout
app.layout = html.Div([
    html.Div([
        html.Div([
            html.Button('Overview', id='btn-dashboard', n_clicks=0, className='btn btn-width'),
            html.Button('Share Price', id='btn-share-price', n_clicks=0, className='btn btn-width'),
            html.Button('Bollinger Bands', id='btn-bollinger-bands', n_clicks=0, className='btn btn-width'),
            html.Button('Raw Data', id='btn-raw-data', n_clicks=0, className='btn btn-width')
        ], id='menu', className='navbar'),
    ], className='vertical-navbar'),
    html.Div([
        html.Div(id='page-content')
    ], className='main-content')
], className='layout-container')

# Callback to update page content and active button
@app.callback(
    [Output('page-content', 'children'),
     Output('menu', 'children')],
    [Input('btn-dashboard', 'n_clicks'),
     Input('btn-share-price', 'n_clicks'),
     Input('btn-bollinger-bands', 'n_clicks'),
     Input('btn-raw-data', 'n_clicks')],
    [State('btn-dashboard', 'id'),
     State('btn-share-price', 'id'),
     State('btn-bollinger-bands', 'id'),
     State('btn-raw-data', 'id')]
)
def update_page_content(btn_home, btn_share_price, btn_bollinger_bands, btn_raw_data, id_home, id_share_price, id_bollinger_bands, id_raw_data):
    ctx = dash.callback_context
    if not ctx.triggered:
        button_id = 'btn-dashboard'
    else:
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]

    if button_id == 'btn-dashboard':
        content = build_every_year_total_sales_content()
        active_button_id = id_home
    elif button_id == 'btn-share-price':
        content = "Share Price Content"
        active_button_id = id_share_price
    elif button_id == 'btn-bollinger-bands':
        content = build_bollinger_content()
        active_button_id = id_bollinger_bands
    elif button_id == 'btn-raw-data':
        content = "Raw Data Content"
        active_button_id = id_raw_data
    else:
        content = "Select an option from the menu"
        active_button_id = 'btn-dashboard'

    # Update class names of buttons
    menu_buttons = [
        html.Button('Overview', id='btn-dashboard', n_clicks=0, className='btn btn-width'),
        html.Button('Share Price', id='btn-share-price', n_clicks=0, className='btn btn-width'),
        html.Button('Bollinger Bands', id='btn-bollinger-bands', n_clicks=0, className='btn btn-width'),
        html.Button('Raw Data', id='btn-raw-data', n_clicks=0, className='btn btn-width')
    ]
    for button in menu_buttons:
        if button.id == active_button_id:
            button.className += ' active'

    return content, menu_buttons

def build_bollinger_content():
    # generate random data for multiple markets
    df = generate_random_data(100, volatility=0.5, trend=0.1, noise_level=0.1)
    df1 = generate_random_data(100, volatility=0.5, trend=0.3, noise_level=0.2)
    df2 = generate_random_data(100, volatility=0.5, trend=0.2, noise_level=0.3)

    # associate name to each market
    df['Market'] = 'Market 1'
    df1['Market'] = 'Market 2'
    df2['Market'] = 'Market 3'

    # Combine dataframes
    combined_df = pd.concat([df, df1, df2])

    # Dropdown to select market
    market_selector = dcc.Dropdown(
        id='market-selector',
        options=[{'label': market, 'value': market} for market in combined_df['Market'].unique()],
        value=combined_df['Market'].iloc[0]
    )

    return html.Div([market_selector, dcc.Graph(id='bollinger-graph')])

@app.callback(
    Output('bollinger-graph', 'figure'),
    [Input('market-selector', 'value')]
)
def update_bollinger_graph(selected_market):
    # generate random data for multiple markets
    df = generate_random_data(100, volatility=0.5, trend=0.1, noise_level=0.1)
    df1 = generate_random_data(100, volatility=0.5, trend=0.3, noise_level=0.2)
    df2 = generate_random_data(100, volatility=0.5, trend=0.2, noise_level=0.3)

    # associate name to each market
    df['Market'] = 'Market 1'
    df1['Market'] = 'Market 2'
    df2['Market'] = 'Market 3'

    # Combine dataframes
    combined_df = pd.concat([df, df1, df2])

    # Filter data based on selected market
    selected_market_data = combined_df[combined_df['Market'] == selected_market]

    fig = go.Figure()

    # Adding traces for each line
    fig.add_trace(go.Scatter(x=selected_market_data['Date'], y=selected_market_data['Close'], mode='lines', name='Close'))
    fig.add_trace(go.Scatter(x=selected_market_data['Date'], y=selected_market_data['SMA'], mode='lines', name='SMA', line=dict(color='blue', width=2, dash='dot')))
    fig.add_trace(go.Scatter(x=selected_market_data['Date'], y=selected_market_data['UB'], mode='lines', name='Upper Band', line=dict(color='green', width=1, dash='dash')))
    fig.add_trace(go.Scatter(x=selected_market_data['Date'], y=selected_market_data['LB'], mode='lines', name='Lower Band', line=dict(color='red', width=1, dash='dash'), fill='tonexty', fillcolor='rgba(255, 0, 0, 0.1)'))

    # Adding hover information
    fig.update_traces(hoverinfo='x+y', hovertemplate='%{x}<br>%{y:.2f}')

    # Adjusting axis labels and title
    fig.update_layout(title='Bollinger Bands', xaxis_title='Date', yaxis_title='Price')

    # Setting y-axis range
    min_price = min(selected_market_data['LB'].min(), selected_market_data['Close'].min())
    max_price = max(selected_market_data['UB'].max(), selected_market_data['Close'].max())
    fig.update_yaxes(range=[min_price, max_price])

    # Adding annotations for crossing points
    for index, row in selected_market_data.iterrows():
        if row['Close'] > row['UB']:
            fig.add_annotation(x=row['Date'], y=row['Close'], text="Above Upper Band", showarrow=True, arrowhead=1, arrowcolor='red', ax=0, ay=-40)
        elif row['Close'] < row['LB']:
            fig.add_annotation(x=row['Date'], y=row['Close'], text="Below Lower Band", showarrow=True, arrowhead=1, arrowcolor='green', ax=0, ay=40)

    return fig

if __name__ == '__main__':
    app.run_server(debug=True, port=8050)
