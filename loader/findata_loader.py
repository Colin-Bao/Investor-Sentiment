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
        self.tasks_total = 0
        self.tasks_completed = 0

    def load_stock_basic(self):
        """
        下载基本股票信息
        :return:
        """
        df = self.PRO_API.query('stock_basic', exchange='', list_status='L',
                                fields='ts_code,symbol,name,area,industry,list_date').set_index('ts_code')
        # df2 = pd.read_sql_table('gzhs', con=self.ENGINE, schema='WECHAT_GZH')
        df.to_sql('stock_basic', self.ENGINE, index=True,
                  if_exists='fail',
                  dtype={'trade_date': types.NVARCHAR(length=100),
                         'ts_code': types.NVARCHAR(length=100)},
                  schema='FIN_BASIC')

    def load_all_code_daily(self):
        """
        从stock_basic中下载所有的股票代码
        """

        # 获取股票列表
        def get_code_list():
            code_list = pd.read_sql_table('stock_basic', self.ENGINE, schema='FIN_BASIC', columns=['ts_code'])[
                'ts_code'].to_list()
            self.tasks_total = len(code_list)
            return code_list

        # 每只股票的下载程序
        def load_code(code):
            try:
                df_code = self.TS_API.pro_bar(ts_code=code, adj='qfq', asset='E', ).set_index('trade_date')
                df_code.to_sql(code, self.ENGINE, if_exists='fail', index=True, schema='FIN_DAILY_TUSHARE',
                               dtype={'trade_date': types.NVARCHAR(length=100),
                                      'ts_code': types.NVARCHAR(length=100)},
                               method='multi')

            except Exception as e:
                print(e)

        # 迭代下载
        def load_multi():
            from concurrent.futures import ThreadPoolExecutor
            # 回调
            def progress_indicator(future):
                with self.lock:  # obtain the lock
                    self.tasks_completed += 1
                    print(
                        f'{self.tasks_completed}/{self.tasks_total} completed, {self.tasks_total - self.tasks_completed} remain.')

            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = [executor.submit(load_code, code) for code in get_code_list()]
                for future in futures:
                    future.add_done_callback(progress_indicator)

        load_multi()

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

    def load_shibor(self):
        if 'shibor' in self.TABLE_LIST:
            return
        df_1 = self.PRO_API.shibor(start_date=self.START_DATE, end_date=str(int(self.START_DATE) + 30000))
        df_2 = self.PRO_API.shibor(start_date=str(int(self.START_DATE) + 30000), end_date=self.END_DATE)
        df = pd.concat([df_1, df_2], axis=0).rename(columns={'date': 'trade_date'}).sort_values('trade_date',
                                                                                                ascending=False)
        # print(df)

        df.to_sql('')


if __name__ == '__main__':
    DownLoader().load_all_code_daily()
