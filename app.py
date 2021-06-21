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

ons_hh_id = spark_session.sql("SELECT DISTINCT ons_household_id FROM summary")
summary_col_names = spark_session.sql("SHOW COLUMNS IN summary")


def df_to_list(df: DataFrame):
    # Assumption: df always has 1 column
    return df.toPandas().iloc[:,0].to_list()

def create_options(option_values: list, key_names: list):
    return [{key: id for key in key_names} for id in option_values]

ons_hh_id_list = df_to_list(ons_hh_id)
hh_id_options = create_options(ons_hh_id_list, ["label", "value"])

summary_col_names_list = df_to_list(summary_col_names)
summary_col_options = create_options(summary_col_names_list, ["name", "id"])


app = dash.Dash(__name__)

app.layout = html.Div([
    html.Label('Household ID Selection'),
        dcc.Dropdown(
            options=hh_id_options,
            value=None,
            multi=True,
            id="selection"
        ),
    dash_table.DataTable(
        id='table',
        columns=summary_col_options,
        data=None,
    )

])

# @app.callback(
#     Output(component_id='table', component_property='data'),
#     Input(component_id='selection', component_property='value')
# )
# def update_output_div(input_value):
#     if input_value is not None:
#         return df[df[selection_col].isin(input_value)].to_dict("records")


if __name__ == '__main__':
    port = os.environ.get("CDSW_PUBLIC_PORT")
    ip = os.environ.get("CDSW_IP_ADDRESS")
    debug = True if os.environ.get("INCENTIVES_DEMO") is not None else False
    app.run_server(port=port, host=ip, debug=debug, use_reloader=False)  # Reloaded causes multiple launches in CDSW
