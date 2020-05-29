import csv
import os
import shutil
import fake_data
import pandas as pd
import uuid
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dagster import execute_pipeline, pipeline, solid
from dagster import Partition, PartitionSetDefinition, repository_partitions
from dagster import pipeline, RepositoryDefinition
from dagster.utils.partitions import date_partition_range


PROCESSED='dagster_training_data/PROCESSED'

@solid(config={'start_date': str, 'end_date':  str})
def sales_data_by_category(context):

    if not os.path.exists(PROCESSED):
        os.makedirs(PROCESSED)
    else:
        shutil.rmtree(PROCESSED)

    baskets = fake_data.load()

    start_date = context.solid_config['start_date']
    end_date = context.solid_config['end_date']

    filtered_baskets = baskets.loc[(baskets['timestamp'] >= start_date) & (baskets['timestamp'] < end_date), :]

    sales_aggregated = filtered_baskets.groupby('category').count()['basket_id']
 
    filename = start_date + '_' + end_date + '.csv'
    filename = os.path.join(PROCESSED, filename)
    sales_aggregated.to_csv(filename)


def environment_dict_fn_for_day_date(partition):
    start_date = partition.value
    day_after_start_date = start_date + relativedelta(days=1)

    return {
        'solids': {
            'sales_data_by_category': {
                'config': {
                    'start_date': start_date.strftime("%Y-%m-%d"),
                    'end_date': day_after_start_date.strftime("%Y-%m-%d"),
                }
            }
        }
    }

def environment_dict_fn_for_week_date(partition):
    start_date = partition.value
    week_after_start_date = start_date + relativedelta(weeks=1)

    return {
        'solids': {
            'sales_data_by_category': {
                'config': {
                    'start_date': start_date.strftime("%Y-%m-%d"),
                    'end_date': week_after_start_date.strftime("%Y-%m-%d"),
                }
            }
        }
    }

def environment_dict_fn_for_month_date(partition):
    date = partition.value
    first_day_of_month_of_start_date = date.replace(day=1)
    first_day_of_next_month = date + relativedelta(months=1)

    return {
        'solids': {
            'sales_data_by_category': {
                'config': {
                    'start_date': first_day_of_month_of_start_date.strftime("%Y-%m-%d"),
                    'end_date': first_day_of_next_month.strftime("%Y-%m-%d"),
                }
            }
        }
    }

start_part_date = datetime.combine(fake_data.archive_start_date(), datetime.min.time())
end_part_date = datetime.now()

sales_data_month_partition_set = PartitionSetDefinition(
    name="sales_data_month_partition_set",
    pipeline_name="sales_by_period_and_category_pipeline",
    # What format will the archive start date return? This may need to be parsed to fit the following format: "%Y-%m-%d"
    partition_fn=date_partition_range(
        start=start_part_date,
        end=end_part_date,
        delta=relativedelta(months=1),
    ),
    environment_dict_fn_for_partition=environment_dict_fn_for_month_date,
)

sales_data_week_partition_set = PartitionSetDefinition(
    name="sales_data_week_partition_set",
    pipeline_name="sales_by_period_and_category_pipeline",
    partition_fn=date_partition_range(
        start=start_part_date,
        end=end_part_date,
        delta=relativedelta(weeks=1),
    ),
    environment_dict_fn_for_partition=environment_dict_fn_for_week_date,
)

sales_data_day_partition_set = PartitionSetDefinition(
    name="sales_data_day_partition_set",
    pipeline_name="sales_by_period_and_category_pipeline",
    partition_fn=date_partition_range(
        start=start_part_date,
        end=end_part_date,
        delta=relativedelta(days=1),
    ),
    environment_dict_fn_for_partition=environment_dict_fn_for_day_date,
)

def define_partitions():
    return [sales_data_day_partition_set, sales_data_week_partition_set, sales_data_month_partition_set]

@pipeline
def sales_by_period_and_category_pipeline():
    sales_data_by_category()

def define_repo():
    return RepositoryDefinition(
        name='sales_repo',
        pipeline_defs=[sales_by_period_and_category_pipeline],
        partition_set_defs=define_partitions(),
    )

