import os
import shutil
import fake_data
import pandas as pd
from dagster import pipeline, solid, RepositoryDefinition
from dagster_training_partitions import define_partitions


PROCESSED='dagster_training_data/PROCESSED'

@solid(config={'start_date': str, 'end_date':  str, 'period': str})
def sales_data_by_category(context):

    period_filename = os.path.join(PROCESSED, context.solid_config['period'])

    if not os.path.exists(period_filename):
        os.makedirs(period_filename)
    
    baskets = fake_data.load()

    start_date = context.solid_config['start_date']
    end_date = context.solid_config['end_date']

    filtered_baskets = baskets.loc[(baskets['timestamp'] >= start_date) & (baskets['timestamp'] < end_date), :]

    sales_aggregated = filtered_baskets.groupby('category').count()['basket_id']
 
    filename = start_date + '_' + end_date + '.csv'
    filename = os.path.join(period_filename, filename)
    sales_aggregated.to_csv(filename)

@pipeline
def sales_by_period_and_category_pipeline():
    sales_data_by_category()

def define_repo():
    return RepositoryDefinition(
        name='sales_repo',
        pipeline_defs=[sales_by_period_and_category_pipeline],
        partition_set_defs=define_partitions(),
    )
