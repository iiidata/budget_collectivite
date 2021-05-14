import argparse
import importlib
import logging
import sys

from config import engine


logger = logging.getLogger(__name__)
INTEGRATION_SEQUENCES = {}

JOBS = {
    'init_db': 'InitDB',
    'dim_date': 'DateDimensionWarehousing',
    'stg_city_ref': 'CityReferentialFileStaging',
    'stg_insee_siren_ref': 'SirenInseeCityReferentialFileStaging',
    'stg_acc_aggregates': 'AggregatesAccountancyFileStaging',
    'dim_city': 'CityDimensionWarehousing',
    'fact_balance': 'BalanceFactWarehousing'
}


def main(argv):
    # Parse arguments
    parser = argparse.ArgumentParser(description="data+8 datawarehouse integration services")
    parser.add_argument('--job', '-j', type=str, choices=JOBS.keys(), required=True,
                        help='Please input integration process name to run')
    parser.add_argument('--dataset', '-d', type=str, nargs = "+", required=False,
                        help='Input dataset files path')

    args = vars(parser.parse_args(argv))

    # Get params
    job_name = args.get('job')
    job_class = JOBS.get(job_name)
    if job_class is None:
        print(f'Job `{job_name}` doesnt exists')

    dataset_files = args.get('dataset')
    kwargs = {'dataset': dataset_files}

    etl_jobs_module = importlib.import_module('jobs_etl')

    class_ = getattr(etl_jobs_module, job_class)
    job_instance = class_(dwh_db=engine, **kwargs)
    try:
        job_instance.run()
        print('Job `{}` processing is succeed'.format(job_name))
        exit(0)
    except Exception as e:
        logger.exception(e)
        print('Job `{}` processing has encountered an error (show logs)'.format(job_name))
        exit(1)


if __name__ == '__main__':
    main(sys.argv[1:])

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
