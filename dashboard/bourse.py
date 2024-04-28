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
            html.Button('Home', id='btn-home', n_clicks=0, className='btn btn-width active'),
            html.Button('Share Price', id='btn-share-price', n_clicks=0, className='btn btn-width'),
            html.Button('Bollinger Bands', id='btn-bollinger-bands', n_clicks=0, className='btn btn-width'),
            html.Button('Raw Data', id='btn-raw-data', n_clicks=0, className='btn btn-width')
        ], className='navbar'),
    ], className='vertical-navbar'),
    html.Div([
            dcc.Textarea(
                id='sql-query',
                value='''
                    SELECT * FROM pg_catalog.pg_tables
                        WHERE schemaname != 'pg_catalog' AND 
                              schemaname != 'information_schema';
                ''',
                style={'width': '100%', 'height': '200px'}
            )
        ], className='container'),
    html.Div(id='dashboard-content', className='container')
])

# Callback to update dashboard content based on button click
@app.callback(
    Output('dashboard-content', 'children'),
    [Input('btn-share-price', 'n_clicks'),
     Input('btn-bollinger-bands', 'n_clicks'),
     Input('btn-raw-data', 'n_clicks')],
    [State('sql-query', 'value')]
)
def update_dashboard_content(btn_share_price, btn_bollinger_bands, btn_raw_data, query):
    changed_id = [p['prop_id'] for p in dash.callback_context.triggered][0]
    if 'btn-share-price' in changed_id:
        return html.Div([
            dcc.Graph(id='share-price-graph'),
            # Add additional components for share price analysis if needed
        ])
    elif 'btn-bollinger-bands' in changed_id:
        return html.Div([
            # Add components for displaying Bollinger Bands
        ])
    elif 'btn-raw-data' in changed_id:
        return html.Div([
            html.Pre(id='raw-data-output')
        ])
    else:
        return html.Div("Select an option from the menu")

# Callback to update share price graph based on selection
@app.callback(
    Output('share-price-graph', 'figure'),
    [Input('execute-query', 'n_clicks')],
    [State('sql-query', 'value')]
)
def update_share_price_graph(n_clicks, query):
    # Update this function to fetch share price data and plot accordingly
    # Example code:
    if n_clicks > 0:
        try:
            result_df = pd.read_sql_query(query, engine)
            # Generate plot using result_df
            # Example: fig = px.line(result_df, x='Date', y='Price', title='Share Price')
            # return fig
        except Exception as e:
            return html.Pre(str(e))
    return "Enter a query and press execute."

# Callback to display raw data from actions
@app.callback(
    Output('raw-data-output', 'children'),
    [Input('execute-query', 'n_clicks')],
    [State('sql-query', 'value')]
)
def display_raw_data(n_clicks, query):
    # Update this function to display raw data from actions
    if n_clicks > 0:
        try:
            result_df = pd.read_sql_query(query, engine)
            return html.Pre(result_df.to_string())
        except Exception as e:
            return html.Pre(str(e))
    return "Enter a query and press execute."

if __name__ == '__main__':
    app.run_server(debug=True, port=8050)
