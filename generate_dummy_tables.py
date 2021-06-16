import os
import yaml
from mimesis.schema import Field, Schema
import pyspark

_ = Field('en-gb', seed=42)

with open(os.environ.get("INCENTIVES_CONFIG_PATH")) as f:
    config = yaml.safe_load(f)

def generate_summary_table():
    lab_bloods_description = lambda: {
        'ons_household_id': _('random.custom_code', mask='COV ############', digit='#'),
        'participant_id': _('random.custom_code', mask='DHR-############', digit='#'),
        'fullname': _('person.full_name'),
        'due_now': _('random.randints', amount=1)[0]
    }

    schema = Schema(schema=lab_bloods_description)
    return schema

if __name__ == "__main__":
    print(generate_summary_table().create(iterations=4))
