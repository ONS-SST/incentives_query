import os
import dash
import dash_table
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

from pyspark.sql import SparkSession, DataFrame
import pandas as pd

from generate_dummy_tables import generate_demo_tables


spark_session = SparkSession.builder.getOrCreate()

if os.environ.get("INCENTIVES_DEMO") is not None:
    generate_demo_tables(spark_session)

def df_to_list(df: DataFrame):
    # Assumption: df always has 1 column
    return df.toPandas().iloc[:,0].to_list()

def create_options(option_values: list, key_names: list):
    return [{key: id for key in key_names} for id in option_values]

app = dash.Dash(__name__)

def get_column_options(table):
    df = spark_session.sql(f'SHOW COLUMNS IN {table}')
    col_names_list = df_to_list(df)
    return create_options(col_names_list, ["name", "id"])

def get_category_options(column):
    df = spark_session.sql(f"SELECT DISTINCT {column} FROM summary")
    category_list = df_to_list(df)
    return create_options(category_list, ["label", "value"])


app.layout = html.Div([
    html.Label('Selection'),
        dcc.Dropdown(
            options=get_category_options('ons_household_id'),
            value=None,
            id="selection-household",
            placeholder="Select a Household ID"
        ),
        dcc.Dropdown(
            options=get_category_options('participant_id'),
            value=None,
            id="selection-participant",
            placeholder="Select a Participant ID"
        ),
        dcc.Input(
            id="full-name-input",
            type='text',
            placeholder="Input participant name",
            debounce = True,
            value = None
        ),
 dcc.Tabs([
        dcc.Tab(label='summary', children=[
             dash_table.DataTable(
                id='summary-table',
                columns=get_column_options('summary'),
                data=None,
            )
        ]),        
        dcc.Tab(label='visits', children=[
             dash_table.DataTable(
                id='visits-table',
                columns=get_column_options('visits'),
                data=None,
            )
        ]),        
        dcc.Tab(label='echeques', children=[
             dash_table.DataTable(
                id='echeques-table',
                columns=get_column_options('e_cheques'),
                data=None,
            )
        ]),        
        dcc.Tab(label='pcheques', children=[
             dash_table.DataTable(
                id='pcheques-table',
                columns=get_column_options('p_cheques'),
                data=None,
            )
        ]),
    ]),
])

@app.callback(
    Output(component_id='summary-table', component_property='data'),
    Input(component_id='selection-household', component_property='value'),
    Input(component_id='selection-participant', component_property='value'),
    Input(component_id='full-name-input', component_property='value')
)
def update_summary_table(household_id, participant_id, fullname):
    if any(_ is not None for _ in [household_id, participant_id, fullname]):
        query = "SELECT * FROM summary WHERE "
        where_statements = []
        if household_id is not None:
            where_statements.append(f'ons_household_id = "{household_id}"')
        if participant_id is not None:
            where_statements.append(f'participant_id = "{participant_id}"')
        if fullname is not None:
            where_statements.append(f'fullname LIKE "%{fullname}%"')
        print(where_statements)
        return spark_session.sql(query + " AND ".join(where_statements)).toPandas().to_dict("records")

@app.callback(
    Output(component_id='visits-table', component_property='data'),
    Input(component_id='selection-household', component_property='value'),
    Input(component_id='selection-participant', component_property='value')
)
def update_visits_table(input_value):
    if input_value is not None:
        return spark_session.sql(f'SELECT * FROM visits WHERE ons_household_id = "{input_value}"').toPandas().to_dict("records")

@app.callback(
    Output(component_id='echeques-table', component_property='data'),
    Input(component_id='selection-household', component_property='value'),
    Input(component_id='selection-participant', component_property='value')
)
def update_echeques_table(input_value):
    if input_value is not None:
        return spark_session.sql(f'SELECT * FROM e_cheques WHERE ons_household_id = "{input_value}"').toPandas().to_dict("records")

@app.callback(
    Output(component_id='pcheques-table', component_property='data'),
    Input(component_id='selection-household', component_property='value'),
    Input(component_id='selection-participant', component_property='value')
)
def update_pcheques_table(input_value):
    if input_value is not None:
        return spark_session.sql(f'SELECT * FROM p_cheques WHERE ons_household_id = "{input_value}"').toPandas().to_dict("records")       


if __name__ == '__main__':
    port = os.environ.get("CDSW_PUBLIC_PORT")
    ip = os.environ.get("CDSW_IP_ADDRESS")
    debug = True if os.environ.get("INCENTIVES_DEMO") is not None else False
    app.run_server(port=port, host=ip, debug=debug, use_reloader=False)  # Reloaded causes multiple launches in CDSW
