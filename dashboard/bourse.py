import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import pandas as pd
import sqlalchemy

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css', 'docker/dashboard/dashboard/assets/lookgood.css']

# DATABASE_URI = 'timescaledb://ricou:monmdp@db:5432/bourse'    # inside docker
DATABASE_URI = 'timescaledb://ricou:monmdp@localhost:5432/bourse'  # outisde docker
engine = sqlalchemy.create_engine(DATABASE_URI)

app = dash.Dash(__name__,  title="Bourse", suppress_callback_exceptions=True, external_stylesheets=external_stylesheets)
server = app.server

# Define the layout
app.layout = html.Div([
    html.Div([
        html.Div([
            html.Button('Dashboard', id='btn-dashboard', n_clicks=0, className='btn btn-width'),
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
        content = "Home Page Content"
        active_button_id = id_home
    elif button_id == 'btn-share-price':
        content = "Share Price Content"
        active_button_id = id_share_price
    elif button_id == 'btn-bollinger-bands':
        content = "Bollinger Bands Content"
        active_button_id = id_bollinger_bands
    elif button_id == 'btn-raw-data':
        content = "Raw Data Content"
        active_button_id = id_raw_data
    else:
        content = "Select an option from the menu"
        active_button_id = 'btn-dashboard'

    # Update class names of buttons
    menu_buttons = [
        html.Button('Home', id='btn-dashboard', n_clicks=0, className='btn btn-width'),
        html.Button('Share Price', id='btn-share-price', n_clicks=0, className='btn btn-width'),
        html.Button('Bollinger Bands', id='btn-bollinger-bands', n_clicks=0, className='btn btn-width'),
        html.Button('Raw Data', id='btn-raw-data', n_clicks=0, className='btn btn-width')
    ]
    for button in menu_buttons:
        if button.id == active_button_id:
            button.className += ' active'

    return content, menu_buttons

if __name__ == '__main__':
    app.run_server(debug=True, port=8050)
