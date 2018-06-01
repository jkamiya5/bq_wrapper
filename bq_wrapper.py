import datetime
import traceback
from logging import DEBUG, StreamHandler, getLogger

import pandas as pd
from apiclient.discovery import build
from oauth2client.client import GoogleCredentials

logger = getLogger(__name__)
handler = StreamHandler()
handler.setLevel(DEBUG)
logger.setLevel(DEBUG)
logger.addHandler(handler)
logger.propagate = False


class BqWrapper(object):

  __instance = None

  def __new__(cls, *args, **keys):
    if cls.__instance is None:
      cls.__instance = object.__new__(cls)
    return cls.__instance

  def __init__(self):
    logger.debug("init")

  def get_dates_of_date_specified_table(self, project_id, dataset_id, table_name, reverse=True, dialect='standard'):

    def proc(project_id, dataset_id, table_name, dialect, target_dates=[], n=2):
      try:
        if n == 0:
          return target_dates
        year_prefix = str(n)
        table_name_ = '`{}.{}.{}_{}*`'.format(project_id, dataset_id, table_name, year_prefix)
        sql = """
                SELECT
                    CONCAT("{year_prefix}", _TABLE_SUFFIX) AS date_
                FROM
                    {table_name_}
                GROUP BY
                    date_
                    """\
            .format(table_name_=table_name_, year_prefix=year_prefix)

        logger.debug(sql)
        df = pd.read_gbq(sql, project_id, dialect=dialect)
        column_name_list = df.columns
        result = list(df[column_name_list[0]])
        target_dates.extend(result)
        return proc(project_id, dataset_id, table_name, dialect, target_dates, n - 1)
      except:
        return target_dates

    target_dates = proc(project_id, dataset_id, table_name, dialect)
    target_dates_ = sorted(target_dates, reverse=reverse)
    return target_dates_

  def get_missed_dates_of_date_specified_table(self, start_date, end_date, **arguments):
    dates = self.get_dates_of_date_specified_table(**arguments)

    def daterange(start_date, end_date):
      for n in range(int((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)

    start_date_ = datetime.datetime.strptime(start_date, '%Y%m%d')
    start_date__ = datetime.date(start_date_.year, start_date_.month, start_date_.day)
    end_date_ = datetime.datetime.strptime(end_date, '%Y%m%d')
    end_date__ = datetime.date(end_date_.year, end_date_.month, end_date_.day)
    comp_dates = [x.strftime("%Y%m%d") for x in daterange(start_date__, end_date__)]
    ret = list(set(comp_dates) - set(dates))
    return sorted(ret, reverse=False)
