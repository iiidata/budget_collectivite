import datetime
import logging
import os

import pandas
from sqlalchemy import inspect, text

from config import sql_scripts_folder

logger = logging.getLogger(__name__)


class Job:
    """
    Super class used to data integration job management
    """

    def __init__(self, dwh_db, **kwargs):
        """
        Expose datawarehouse database to load tranformed datasource
        :param database: connection to datawarehouse database
        :param kwargs:
        """
        self.dwh_db = dwh_db
        self.engine_inspector = inspect(self.dwh_db)

    def run(self):
        raise NotImplementedError('Job must implement this method')


class InitDB(Job):
    """
    Instantiate database model
    """

    script_sql = os.path.join(sql_scripts_folder, "init_datawarehouse.sql")

    def run(self):

        file = open(self.script_sql)
        query = text(file.read())
        self.dwh_db.execute(query)


class AggregatesAccountancyFileStaging(Job):
    """
    Load aggregated city accountancy data into the staging area
    """

    table = 'STG_CITY_AGGREGATES_ACCOUNTANCY'

    def __init__(self, dataset, **kwargs):
        """
        Load aggregated city accountancy per year csv files
        :param files: list of files aggregated by city
        :param database:
        :param kwargs:
        """
        super().__init__(**kwargs)
        assert dataset is not None and len(dataset)>0, f'You must specify csv files to load aggregated city accountancy '
        for file in dataset:
            assert os.path.exists(file), f'The csv file {file} doesnt exists'
        self.data_files = dataset

    def run(self):
        """
        Load aggregates accountancy data
        :return:
        """

        # Remove existing data
        if self.engine_inspector.has_table(self.table):
            logger.info("Truncate table {}".format(self.table))
            query = "DROP TABLE {}".format(self.table)
            self.dwh_db.execute(query)

        # Load dataset
        for file in self.data_files:
            logger.info("Load dataset `{}`".format(file))
            df_agregates_year = pandas.read_csv(file, sep=';', encoding='utf-8')

            # convert columns names to upper case
            df_agregates_year = df_agregates_year.rename(columns=str.upper)

            required_cols = {'SIREN','INSEE','LBUDG','CBUDG','EXER',
                             'CFT','CF1','CF2','CF3','CF4','CF5','CF51','CF52',
                             'PFT','PF1','PF2','PF3','PF4','PF45',
                             'DIT','DI1','DI2','DI3','DI4','DI5','DI6','DI7','DI8',
                             'RIT','RI1','RI2','RI3','RI4','RI5','RI6','RI7','RI8'}
            miss_cols = []
            for col in required_cols:
                if col not in df_agregates_year.columns:
                    miss_cols.append(col)

            assert len(miss_cols) == 0, f'Columns {miss_cols} seems missing in accountancy aggregates file {file}.'

            # filter dataset only on 'communes'
            df_agregates_year = df_agregates_year[df_agregates_year['CATEG'] == 'Commune']
            df_agregates_year = df_agregates_year[list(required_cols)]

            # change type for insee code
            convert_dict = {'INSEE': str}
            df_agregates_year = df_agregates_year.astype(convert_dict)

            df_agregates_year['Integration_Timestamp'] = datetime.datetime.now()
            df_agregates_year.to_sql(name=self.table, if_exists='append', con=self.dwh_db, index=False)
            logger.info("Dataset `{}` is loaded".format(file))

        logger.info("End loading city accountancy aggregates")


class CityReferentialFileStaging(Job):

    table = 'STG_CITY_REFERENTIAL'

    def __init__(self, dataset, **kwargs):
        """
        Load city hierarchy
        :param dataset: file containing french cities
        :param kwargs:
        """
        super().__init__(**kwargs)
        assert dataset is not None and len(dataset) == 1, f'You must specify csv file to load city referential '
        assert os.path.exists(dataset[0]), f'The csv file {dataset[0]} doesnt exists'
        self.city_referential_file = dataset[0]

    def run(self):
        logger.info("Start dataset `{}` loading into table {}".format(self.city_referential_file, self.table))
        df_cities = pandas.read_csv(self.city_referential_file, sep=';', encoding='utf-8')
        required_cols = {'Nom de la commune','Code INSEE Commune','Population'}
        assert required_cols.issubset(df_cities.columns), f'Some columns in {required_cols} seems missing in city referential file.'

        df_cities['Integration_Timestamp'] = datetime.datetime.now()
        df_cities.to_sql(name=self.table, if_exists='replace', con=self.dwh_db, index=False)
        logger.info("End dataset `{}` loading into table {}".format(self.city_referential_file, self.table))


class SirenInseeCityReferentialFileStaging(Job):

    table = 'STG_INSEE_SIREN'

    def __init__(self, dataset, **kwargs):
        """
        Load insee siren transcoding
        :param dataset: file containing french siren / insee code
        :param kwargs:
        """
        super().__init__(**kwargs)
        assert dataset is not None and len(dataset) == 1, f'You must specify csv file to load siren / insee transco '
        assert os.path.exists(dataset[0]), f'The csv file {dataset[0]} doesnt exists'
        self.siren_insee_file = dataset[0]

    def run(self):
        logger.info("Start dataset `{}` loading into table {}".format(self.siren_insee_file, self.table))
        df_cities = pandas.read_csv(self.siren_insee_file, sep=';', encoding='utf-8')
        required_cols = {'INSEE_COM','SIREN'}
        assert required_cols.issubset(df_cities.columns), f'Some columns in {required_cols} seems missing in city insee / siren file.'

        df_cities['Integration_Timestamp'] = datetime.datetime.now()
        df_cities.to_sql(name=self.table, if_exists='replace', con=self.dwh_db, index=False)
        logger.info("End dataset `{}` loading into table {}".format(self.siren_insee_file, self.table))


class DateDimensionWarehousing(Job):
    """
    Generate date dimension
    """

    table = 'DIM_DATE'

    def __init__(self, dwh_db, **kwargs):
        super().__init__(dwh_db, **kwargs)
        self.start_date = '2000-01-01'
        self.end_date = '2050-12-31'

    def run(self):
        # Remove existing data
        if self.engine_inspector.has_table(self.table):
            logger.info("Truncate table {}".format(self.table))
            query = "DELETE FROM {}".format(self.table)
            self.dwh_db.execute(query)

        logger.info("Start loading for date dimension table {}".format(self.table))
        df = pandas.DataFrame({"Date": pandas.date_range(self.start_date, self.end_date)})
        df["Day"] = df.Date.dt.day_name()
        df["id"] = df.Date.dt.strftime("%Y%m%d").astype(int)
        df["Week"] = df.Date.dt.isocalendar().week
        df["Quarter"] = df.Date.dt.quarter
        df["Year"] = df.Date.dt.year
        df["Year_half"] = (df.Quarter + 1) // 2
        df['Integration_Timestamp'] = datetime.datetime.now()

        df.to_sql(name=self.table, if_exists='append', con=self.dwh_db, index=False)


class CityDimensionWarehousing(Job):
    """
    Generate city budget annual balance
    """
    script_sql = os.path.join(sql_scripts_folder,"load_DIM_CITY.sql")
    table = "DIM_CITY"

    def run(self):
        # Remove existing data
        if self.engine_inspector.has_table(self.table):
            logger.info("Truncate table {}".format(self.table))
            query = "DELETE FROM {}".format(self.table)
            self.dwh_db.execute(query)

        file = open(self.script_sql)
        query = text(file.read())
        df_cities = pandas.read_sql(query, con=self.dwh_db)
        df_cities['Integration_Timestamp'] = datetime.datetime.now()

        df_cities.to_sql(name=self.table, if_exists='append', con=self.dwh_db, index=False)


class BalanceFactWarehousing(Job):

    script_sql = os.path.join(sql_scripts_folder, "load_FACT_BUDGET_BALANCE_ACCOUNT.sql")
    table = "FACT_BUDGET_BALANCE_ACCOUNT"

    def run(self):
        # Remove existing data
        if self.engine_inspector.has_table(self.table):
            logger.info("Truncate table {}".format(self.table))
            query = "DELETE FROM {}".format(self.table)
            self.dwh_db.execute(query)

        file = open(self.script_sql)
        query = text(file.read())
        df_facts = pandas.read_sql(query, con=self.dwh_db)
        df_facts['Integration_Timestamp'] = datetime.datetime.now()

        df_facts.to_sql(name=self.table, if_exists='append', con=self.dwh_db, index=False)