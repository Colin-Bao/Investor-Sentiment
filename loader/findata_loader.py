from utils.sql import DB
import pandas as pd
from sqlalchemy import types
from tqdm import tqdm


class TuShare(DB):
    """
    Tusahre接口
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__TOKEN = '56a12424870cd0953907cde2c660b498c8fe774145b7f17afdc746dd'
        import tushare as ts
        ts.set_token(self.__TOKEN)
        self.TS_API = ts
        self.PRO_API = ts.pro_api()


class DownLoader(TuShare):

    def __init__(self, SHAREINDEX_LIST=()):

        super(DownLoader, self).__init__()
        self.SHAREINDEX_LIST = SHAREINDEX_LIST
        from threading import Lock
        self.lock = Lock()
        # self.tasks_total = len(self.get_index_members())
        self.tasks_completed = 0

    def load_index(self):
        def load_index_daily(index):
            return self.TS_API.pro_bar(ts_code=index, adj='qfq', asset='I',
                                       start_date=self.START_DATE, end_date=self.END_DATE)

        def load_index_weight(index):
            date_list = [date.strftime('%Y%m%d') for date in
                         pd.date_range(str(int(self.START_DATE) - 10000), str(int(self.END_DATE) + 10000),
                                       freq='3M').to_list()]
            df_weight_con = pd.DataFrame()
            i = 0
            for date in tqdm(date_list):
                if i + 1 >= len(date_list):
                    break
                df_weight = self.PRO_API.index_weight(index_code=index, start_date=date, end_date=date_list[i + 1])
                df_weight_con = pd.concat([df_weight_con, df_weight], axis=0)
                # print(date, date_list[i + 1], '\n', df_weight.shape)
                i += 1
            self.save_sql(df_weight_con.sort_values('trade_date', ascending=False), index + '_weight')

        def load():
            for idx in self.SHAREINDEX_LIST:
                if idx + '_weight' in self.TABLE_LIST:
                    continue
                load_index_daily(idx)
                load_index_weight(idx)

        load()

    def load_code(self, code):
        if code not in self.TABLE_LIST:
            try:
                df_code = self.TS_API.pro_bar(ts_code=code, adj='qfq', asset='E',
                                              start_date=self.START_DATE, end_date=self.END_DATE, )
                self.save_sql(df_code, code)
                # self.ENGINE.execute(f"CREATE INDEX ix_{code.replace('.', '')}_trade_date ON '{code}' (trade_date)")
            except Exception as e:
                print(e)

    def load_index_members(self, ):

        from concurrent.futures import ThreadPoolExecutor

        def progress_indicator(future):
            # obtain the lock
            with self.lock:
                # update the counter
                self.tasks_completed += 1
                # report progress
                print(
                    f'{self.tasks_completed}/{self.tasks_total} completed, {self.tasks_total - self.tasks_completed} remain.')

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(self.load_code, table) for table in self.get_index_members()]
            for future in futures:
                future.add_done_callback(progress_indicator)
            # for code in tqdm(self.get_index_members()):
            #     try:
            #         self.load_code(code)
            #     except Exception as e:
            #         print(e, '\n')
            #         continue

    def load_stock_basic(self):
        """
        下载基本股票信息
        :return:
        """
        df = self.PRO_API.query('stock_basic', exchange='', list_status='L',
                                fields='ts_code,symbol,name,area,industry,list_date').set_index('ts_code')
        # df2 = pd.read_sql_table('gzhs', con=self.ENGINE, schema='WECHAT_GZH')
        # print(df2)
        df.to_sql('stock_basic', self.ENGINE, index=True,
                  if_exists='append',
                  dtype={'trade_date': types.NVARCHAR(length=100),
                         'ts_code': types.NVARCHAR(length=100)},
                  schema='FIN_BASIC')

    def load_shibor(self):
        if 'shibor' in self.TABLE_LIST:
            return
        df_1 = self.PRO_API.shibor(start_date=self.START_DATE, end_date=str(int(self.START_DATE) + 30000))
        df_2 = self.PRO_API.shibor(start_date=str(int(self.START_DATE) + 30000), end_date=self.END_DATE)
        df = pd.concat([df_1, df_2], axis=0).rename(columns={'date': 'trade_date'}).sort_values('trade_date',
                                                                                                ascending=False)
        # print(df)

        df.to_sql('')


DownLoader().load_stock_basic()
# print(DownLoader())
