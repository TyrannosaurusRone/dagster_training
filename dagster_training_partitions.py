import fake_data
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dagster import Partition, PartitionSetDefinition, pipeline, PipelineDefinition
from dagster.utils.partitions import date_partition_range


def environment_dict_fn_for_day_date(partition):
    start_date = partition.value
    day_after_start_date = start_date + relativedelta(days=1)

    return {
        'solids': {
            'sales_data_by_category': {
                'config': {
                    'start_date': start_date.strftime("%Y-%m-%d"),
                    'end_date': day_after_start_date.strftime("%Y-%m-%d"),
                    'period': 'daily',
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
                    'period': 'weekly',
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
                    'period': 'monthly',
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