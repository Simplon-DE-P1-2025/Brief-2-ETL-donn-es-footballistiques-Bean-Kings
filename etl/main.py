import etl_clean_1930_2010 as etl_base
import etl_inserter_2014 as inserter
import etl_create_view as etl_view
import etl_2014 as etl_2014
import etl_2018 as etl_2018
import etl_2022 as etl_2022
import db_creation as db_creator
import pandas as pd


def merge_data():
    big_df = etl_base.get_cleaned_1930_data()
    df_2014 = etl_2014.get_cleaned_2014_data()
    df_2018 = etl_2018.get_cleaned_2018_data('data/data_2018.json')
    df_2022 = etl_2022.get_cleaned_2022_data()
    big_df = pd.concat([big_df, df_2014, df_2018, df_2022], ignore_index=True)
    big_df["Datetime"] = pd.to_datetime(big_df["Datetime"], errors="coerce")
    print(big_df.info())
    return big_df

db_creator.create_db_schema()
inserter.load_matches(merge_data())
etl_view.create_view()