import os
from typing import Callable
import yaml
from mimesis.schema import Field, Schema
import pyspark
from pyspark.sql import SparkSession, DataFrame
import pandas as pd

_ = Field('en-gb', seed=42)

#$set INCENTIVES_CONFIG_PATH=./config.yaml
with open(os.environ.get("INCENTIVES_CONFIG_PATH")) as f:
    config = yaml.safe_load(f)

def define_summary_schema():
    return lambda: {
        'ons_household_id': _('random.custom_code', mask='COV ############', digit='#'),
        'participant_id': _('random.custom_code', mask='DHR-############', digit='#'),
        'fullname': _('person.full_name'),
        'due_now': _('random.randints', amount=1)[0]
    }


def schema_to_dataframe(session: SparkSession, schema_definition: Callable, num_records: int):

    schema = Schema(schema=schema_definition)

    data = schema.create(iterations=num_records)

    pd_df = pd.DataFrame(data)

    ps_df = session.createDataFrame(pd_df)

    return ps_df


def write_to_temp(df: DataFrame, name: str):

    df.createOrReplaceTempView(name)




def generate_temp_table(session: SparkSession, schema_definition: Callable, name: str):

    summary_df = schema_to_dataframe(session, schema_definition, 100)

    write_to_temp(summary_df, name)
    



def generate_demo_tables(session: SparkSession):

    generate_temp_table(session, define_summary_schema(), "summary")

    generate_temp_table(session, define_summary_schema(), "summary2")



if __name__ == "__main__":
    #print(generate_summary_table().create(iterations=4))

    #$set PYSPARK_PYTHON=D:\Repositories\incentives-env\Scripts\python.exe

    spark_session = SparkSession.builder.getOrCreate()

    summary_schema = define_summary_schema()

    summary_df = schema_to_dataframe(spark_session, summary_schema, 5)

    write_to_temp(summary_df, "summary")

    spark_session.sql("SELECT * FROM summary").show()