
from code.read_process_dim_data import read_dimension_data
from code.read_trns_fact_data import read_facts_data


def taxisource_etl(processed_month):
    #read the data from raw layer

    read_dimension_data()
    read_facts_data(processed_month)

   
