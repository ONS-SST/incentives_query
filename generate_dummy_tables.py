import os
from typing import Callable
import yaml
from mimesis.schema import Field, Schema
import pyspark
from pyspark.sql import SparkSession, DataFrame
import pandas as pd

_ = Field('en-gb', seed=42)

def define_summary_schema():
    return lambda: {
        'ons_household_id': _('random.custom_code', mask='COV 0000000000##', digit='#'),
        'participant_id': _('random.custom_code', mask='DHR-000000000###', digit='#'),
        'fullname': _('person.full_name'),
        'email': _('person.email', domains=['gsnail.ac.uk']),
        'street': _('address.street_name'),
        'city': _('address.city'),
        'county': _('address.state'),
        'postcode': _('address.postal_code'),
        'earned': _('random.randints', amount=1)[0],
        'p_paid': _('random.randints', amount=1)[0],
        'e_paid': _('random.randints', amount=1)[0],
        'due_now': _('random.randints', amount=1)[0],
    }

def define_visits_schema():
    return lambda: {
        'ons_household_id': _('random.custom_code', mask='COV 0000000000##', digit='#'),
        'participant_id': _('random.custom_code', mask='DHR-000000000###', digit='#'),
        'fullname': _('person.full_name'),
        'visit_date': _('datetime.formatted_datetime', fmt="%Y-%m-%d", start=1800, end=1802),
        'visit_type': _('choice', items=["First Visit", "Follow-up Visit"]),
        'participant_visit_status': _('choice', items=["Completed", "Cancelled"])
    }

def define_e_cheques_schema():
    return lambda: {
        'ons_household_id': _('random.custom_code', mask='COV 0000000000##', digit='#'),
        'participant_id': _('random.custom_code', mask='DHR-000000000###', digit='#'),
        'recipient_name': _('person.full_name'),
        'cheque_number': _('random.custom_code', mask='########', digit='#'),
        'value_issued': _('random.randints', amount=1, a=1, b=100)[0],
        'date_issued': _('datetime.formatted_datetime', fmt="%Y-%m-%d", start=1800, end=1802),
    }

def define_p_cheques_schema():
    return lambda: {
        'ons_household_id': _('random.custom_code', mask='COV 0000000000##', digit='#'),
        'participant_id': _('random.custom_code', mask='DHR-000000000###', digit='#'),
        'recipient_name': _('person.full_name'),
        'full_name': _('person.full_name'),
        'order_no': _('random.custom_code', mask='########', digit='#'),
        'date_issued': _('datetime.formatted_datetime', fmt="%Y-%m-%d", start=1800, end=1802),
        'value': _('random.randints', amount=1, a=1, b=100)[0]
    }

def schema_to_dataframe(session: SparkSession, schema_definition: Callable, num_records: int):
    """Get PySpark DataFrame from a mimesis schema."""
    schema = Schema(schema=schema_definition)
    data = schema.create(iterations=num_records)
    pd_df = pd.DataFrame(data)
    ps_df = session.createDataFrame(pd_df)
    return ps_df


def generate_temp_table(session: SparkSession, schema_definition: Callable, name: str, num_records: int):
    """Generate temporary table with specified name."""
    df = schema_to_dataframe(session, schema_definition, num_records)
    df.createOrReplaceTempView(name)
    
def generate_demo_tables(session: SparkSession):
    generate_temp_table(session, define_summary_schema(), "summary", 500)
    generate_temp_table(session, define_visits_schema(), "visits", 500)
    generate_temp_table(session, define_e_cheques_schema(), "e_cheques", 500)
    generate_temp_table(session, define_p_cheques_schema(), "p_cheques", 500)


if __name__ == "__main__":
    spark_session = SparkSession.builder.getOrCreate()
    generate_demo_tables(spark_session)
    spark_session.sql("SELECT * FROM summary").show()
