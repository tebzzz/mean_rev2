import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import dash
from dash import dcc, html, Input, Output, dash_table
import json
import csv
import sqlite3 

conn = sqlite3.connect('test.db', check_same_thread=False)
d = conn.cursor()

def get_backtest_data():
    return pd.read_sql_query("SELECT * FROM backtest_data", conn)

def get_trades():
    return pd.read_sql_query("SELECT * FROM trades", conn)

def get_discovery():
    return pd.read_sql_query("SELECT * FROM discovery", conn)

def get_optimizaton():
    return pd.read_sql_query("SELECT * FROM optimize", conn)

def csv_to_json(csvFilePath):
    jsonArray = []
      
    with open(csvFilePath, encoding='utf-8') as csvf: 
        csvReader = csv.DictReader(csvf)  

        for row in csvReader: 
            jsonArray.append(row)
  
    jsonString = json.dumps(jsonArray, indent=4)
    return jsonString

def blank_fig():
    fig = go.Figure(go.Scatter(x=[], y = []))
    fig.update_layout(template = None,
                     plot_bgcolor="rgba(0,0,0, 0)",
                     paper_bgcolor="rgba(0,0,0, 0)",)
    fig.update_xaxes(showgrid = False, showticklabels = False, zeroline=False)
    fig.update_yaxes(showgrid = False, showticklabels = False, zeroline=False)

    return fig

config = {'displaylogo': False,
         'modeBarButtonsToAdd':['drawline',
                                'drawopenpath',
                                'drawclosedpath',
                                'drawcircle',
                                'drawrect',
                                'eraseshape'
                               ]}

app = dash.Dash(__name__, 
update_title=None, 
external_stylesheets = ['https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.8.1/css/all.min.css']
)

server = app.server
#add the title that appears in the browser tab
app.title = 'Backtest'

trades = get_trades()

app.layout = html.Div([

    dcc.Tabs([ 
         dcc.Tab(label='Backtest', children=[

            dcc.Store(id = 'data'),
            dcc.Store(id = 'data2'),
            dcc.Interval(id = 'interval', interval = 30000, n_intervals = 0),
            html.Div([
                html.Div([
                    #add the title that appears in the dashboard
                    html.P('Backtest', id = 'title')
                ], id = 'title_container', ),
            ], id = 'header'),
            html.Div([
                html.Div([
                    dcc.Graph(id = 'fig1', config = config, figure = blank_fig()),
                    html.I(className='fa fa-expand'),
                ], id = 'graph-1', className = 'container'),
                html.Div([
                    dcc.Graph(id = 'fig2', config = config, figure = blank_fig()),
                    html.I(className='fa fa-expand'),
                ], id = 'graph-2', className = 'container'),
                html.Div([
                    dcc.Graph(id = 'fig3', config = config, figure = blank_fig()),
                    html.I(className='fa fa-expand'),
                ], id = 'graph-3', className = 'container'),
                html.Div([
                    html.Div([
                        dash_table.DataTable(id = 'table1', 
                        columns = [{"name": col, "id": col} for col in trades.columns], 
                        virtualization = True,
                        fixed_rows = {'headers': True})
                    ], id = 'table_inner_container'),
                ], id = 'table', className = 'container'),
            ], id = 'main', className = 'parent')


        ], id = 'layout'),
        dcc.Tab(label='Optimization', children=[

            html.H1(children='Optimization Data', style={'textAlign': 'center'}),

            dcc.Graph(id='graph'),

        ]),

    dcc.Interval(
    id='interval-component',
    interval=1000, # in milliseconds
    n_intervals=5
    )
])

])

@app.callback(
    Output('data', 'data'),
    Input('interval', 'n_intervals')
)
def update_data(n_intervals):
    data1 = get_backtest_data() #***format this into json 
    return data1.to_json()


@app.callback(
    Output('data2', 'data'),
    Input('interval', 'n_intervals')
)
def update_data(n_intervals):
    trades = get_trades() #***format this to json 
    return trades.to_json()

@app.callback(
    Output('fig1', 'figure'),
    Input('data', 'data')
)
def update_fig1(data):
    dff = pd.read_json(data)
    dff['index'] = range(len(dff))
    dff = pd.melt(dff, id_vars=['index'], value_vars=['Z-Score', 'Upper Bollinger Band', 'Lower Bollinger Band', 'Moving Average'])
    try:
        fig = px.line(dff, x = 'index', y = 'value', color = 'variable')
    except:
        fig = px.line(dff, x = 'index', y = 'value', color = 'variable')

    fig.update_layout(margin = dict(t = 20, b = 20, l = 20, r = 20), 
    plot_bgcolor = 'rgba(0, 0, 0, 0)', 
    paper_bgcolor = 'rgba(0, 0, 0, 0)',
    legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.48,
            title = None,
            font = dict(size = 15)
            )
    )
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='rgb(230, 245, 250)')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='rgb(230, 245, 250)')
    return fig


@app.callback(
    Output('fig2', 'figure'),
    Input('data', 'data')
)
def update_fig2(data):
    dff = pd.read_json(data)
    dff['index'] = range(len(dff))
    try:
        fig = px.line(dff, x = 'index', y = 'Spread')
    except:
        fig = px.line(dff, x = 'index', y = 'Spread')
    fig.update_layout(margin = dict(t = 40, b = 20, l = 20, r = 20), 
    plot_bgcolor = 'rgba(0, 0, 0, 0)', 
    paper_bgcolor = 'rgba(0, 0, 0, 0)',
    title = 'Spread',
    title_x = 0.5)
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='rgb(230, 245, 250)')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='rgb(230, 245, 250)')
    return fig

@app.callback(
    Output('fig3', 'figure'),
    Input('data', 'data')
)
def update_fig3(data):
    dff = pd.read_json(data)
    dff['index'] = range(len(dff))
    try:
        fig = px.line(dff, x = 'index', y = 'Equity')
    except:
        fig = px.line(dff, x = 'index', y = 'Equity')
    fig.update_layout(margin = dict(t = 40, b = 20, l = 20, r = 20), 
    plot_bgcolor = 'rgba(0, 0, 0, 0)', 
    paper_bgcolor = 'rgba(0, 0, 0, 0)',
    title = 'Equity',
    title_x = 0.5)
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='rgb(230, 245, 250)')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='rgb(230, 245, 250)')
    return fig

@app.callback(
    Output('table1', 'data'),
    Input('data2', 'data')
)
def update_table(data):
    trades = get_trades()
    return trades.to_dict('records')

@app.callback(  Output("graph", "figure"), 
                Input("interval-component", "n_intervals"))
def optimization_chart(n):
    df = get_optimizaton()
    fig = px.line(df, x='X', y='returns')
    dff = pd.melt(df, id_vars=['X'], value_vars=['returns', 'sharpe'])
    try:
        fig = px.line(dff, x = 'X', y = 'value', color = 'variable')
    except:
        fig = px.line(dff, x = 'X', y = 'value', color = 'variable')
    return fig

if __name__ == "__main__":
    app.run_server(debug = True, port=8051)
