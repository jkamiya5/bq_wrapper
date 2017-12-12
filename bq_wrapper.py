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
