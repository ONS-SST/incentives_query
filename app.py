import dash
import dash_table
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import pandas as pd

df = pd.DataFrame({"Household ID": ["000", "000", "111"], "Name": ["John", "Jane", "Alfred"],"Visit Date": ["", "", ""], "Visit type": ["First", "Follow up", "First"]})
df = pd.concat([df, df])
selection_col = "Household ID"

hh_ids = [{key: id for key in ["label", "value"]} for id in df[selection_col].unique()]

app = dash.Dash(__name__)

app.layout = html.Div([
    html.Label('Household ID Selection'),
        dcc.Dropdown(
            options=hh_ids,
            value=None,
            multi=True,
            id="selection"
        ),
    dash_table.DataTable(
        id='table',
        columns=[{"name": i, "id": i} for i in df.columns],
        data=None,
    )

])

@app.callback(
    Output(component_id='table', component_property='data'),
    Input(component_id='selection', component_property='value')
)
def update_output_div(input_value):
    if input_value is not None:
        return df[df[selection_col].isin(input_value)].to_dict("records")



if __name__ == '__main__':
    app.run_server(debug=True)
