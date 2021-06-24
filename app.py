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
    """
    Get a list of dictionary objects, using the same value for multiple keys.
    Format used by dash layout objects.
    """
    return [{key: id for key in key_names} for id in option_values]

def get_column_options(table):
    """Get a list of dictionary objects to show column labels."""
    df = spark_session.sql(f'SHOW COLUMNS IN {table}')
    col_names_list = df_to_list(df)
    return create_options(col_names_list, ["name", "id"])

def get_category_options(column):
    """
    Get a list of dictionary objects for each category option,
    used for dropdown choices.
    """
    df = spark_session.sql(f"SELECT DISTINCT {column} FROM summary")
    category_list = df_to_list(df)
    return create_options(category_list, ["label", "value"])

def query_table(query, where_statements):
    """
    Submit a select query with multiple where statement strings.
    Returns data as list of dicts, as required by dashtables.
    """
    return spark_session.sql(query + " AND ".join(where_statements)).toPandas().to_dict("records")

def get_base_query(table_name, name_field, household_id, participant_id, full_name):
    """Get base query string, used in all callbacks for table data."""
    query = f"SELECT * FROM {table_name} WHERE "
    where_statements = []
    if household_id is not None:
        where_statements.append(f'ons_household_id = "{household_id}"')
    if participant_id is not None:
        where_statements.append(f'participant_id = "{participant_id}"')
    if len(full_name) > 2:
        where_statements.append(f'LOWER({name_field}) LIKE "%{full_name.lower()}%"')
    return query, where_statements

def table_tab_by_name(table_name):
    return dash_table.DataTable(
        id=f'{table_name}-table',
        columns=get_column_options(table_name),
        data=None,
        sort_action="native",
        cell_selectable=False
    )

app = dash.Dash("Incentives query tool")

app.layout = html.Div([
    html.H2('Selection'),
    dcc.Dropdown(
        options=get_category_options('ons_household_id'),
        value=None,
        id="selection-household",
        placeholder="Select a Household ID",
        style={"width": "15rem", 'margin': "1rem 0rem"}
    ),
    dcc.Dropdown(
        options=get_category_options('participant_id'),
        value=None,
        id="selection-participant",
        placeholder="Select a Participant ID",
        style={"width": "15rem", 'margin': "1rem 0rem"}
    ),
    html.Div(children='Press enter after entering either name or cheque number.'),
    dcc.Input(
        id="full-name-input",
        type='text',
        placeholder="Participant name",
        debounce = True,
        value = "",
        style={"width": "15rem", 'height': "1.5rem", "display": "block", 'margin': "0.75rem 0rem"}
    ),
    dcc.Input(
        id="cheque-number-input",
        type='text',
        placeholder="Cheque number (electronic or paper)",
        debounce = True,
        value = "",
        style={"width": "15rem", 'height': "1.5rem", "display": "block", 'margin': "0.75rem 0rem"}
    ),
    html.Button('Export to Excel', id='submit-val', style={"display": "block", 'margin': "0.75rem 0rem"}),
    html.H2('Data'),
    dcc.Tabs([
            dcc.Tab(label='Summary', children=[table_tab_by_name("summary")]),
            dcc.Tab(label='Visits', children=[table_tab_by_name("visits")]),
            dcc.Tab(label='Electronic cheques', children=[table_tab_by_name("e_cheques")]),
            dcc.Tab(label='Paper cheques', children=[table_tab_by_name("p_cheques")]),
    ]),
])

@app.callback(
    Output(component_id='summary-table', component_property='data'),
    Input(component_id='selection-household', component_property='value'),
    Input(component_id='selection-participant', component_property='value'),
    Input(component_id='full-name-input', component_property='value')
)
def update_summary_table(household_id, participant_id, full_name):
    if any(_ not in ["", None] for _ in [household_id, participant_id, full_name]):
        query, where_statements = get_base_query("summary", "fullname", household_id, participant_id, full_name)
        return query_table(query, where_statements)

@app.callback(
    Output(component_id='visits-table', component_property='data'),
    Input(component_id='selection-household', component_property='value'),
    Input(component_id='selection-participant', component_property='value'),
    Input(component_id='full-name-input', component_property='value')
)
def update_visits_table(household_id, participant_id, full_name):
    if any(_ not in ["", None] for _ in [household_id, participant_id, full_name]):
        query, where_statements = get_base_query("visits", "fullname", household_id, participant_id, full_name)
        return query_table(query, where_statements)

@app.callback(
    Output(component_id='e_cheques-table', component_property='data'),
    Input(component_id='selection-household', component_property='value'),
    Input(component_id='selection-participant', component_property='value'),
    Input(component_id='full-name-input', component_property='value'),
    Input(component_id='cheque-number-input', component_property='value')
)
def update_echeques_table(household_id, participant_id, full_name, cheque_number):
    if any(_ not in ["", None] for _ in [household_id, participant_id, full_name, cheque_number]):
        query, where_statements = get_base_query("e_cheques", "recipient_name", household_id, participant_id, full_name)
        if cheque_number:
            where_statements.append(f'cheque_number = "{cheque_number}"')
        return query_table(query, where_statements)

@app.callback(
    Output(component_id='p_cheques-table', component_property='data'),
    Input(component_id='selection-household', component_property='value'),
    Input(component_id='selection-participant', component_property='value'),
    Input(component_id='full-name-input', component_property='value'),
    Input(component_id='cheque-number-input', component_property='value')
)
def update_pcheques_table(household_id, participant_id, full_name, cheque_number):
    if any(_ not in ["", None] for _ in [household_id, participant_id, full_name, cheque_number]):
        query, where_statements = get_base_query("p_cheques", "recipient_name", household_id, participant_id, full_name)
        if cheque_number:
            where_statements.append(f'order_no = "{cheque_number}"')
        return query_table(query, where_statements)

if __name__ == '__main__':
    port = os.environ.get("CDSW_PUBLIC_PORT")
    ip = os.environ.get("CDSW_IP_ADDRESS")
    debug = True if os.environ.get("INCENTIVES_DEMO") is not None else False
    app.run_server(port=port, host=ip, debug=debug, use_reloader=False)  # Reloader causes ipython to launch in CDSW
