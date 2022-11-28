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

    def __init__(self, **kwargs):
        super(DownLoader, self).__init__(**kwargs)
        from threading import Lock
        self.lock = Lock()
        self.tasks_total = 0
        self.tasks_completed = 0
        self.pbar = tqdm()
        # 用于下载的线程数量
        self.MAX_CORE = kwargs.get('MAX_CORE', 4)

    def load_stock_basic(self):
        """
        下载基本所有股票信息表
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

    def load_all_code_daily(self, daily_type, to_schema):
        """
        从stock_basic中下载所有的股票代码
        """

        exist_code = pd.read_sql(f'SHOW TABLES FROM {to_schema}', self.ENGINE).iloc[:, 0].to_list()

        # 获取股票列表
        def get_code_list():

            code_list = pd.read_sql_table('stock_basic', self.ENGINE, schema='FIN_BASIC', columns=['ts_code'])[
                'ts_code'].to_list()
            self.tasks_total = len(code_list)
            self.pbar = tqdm(range(self.tasks_total))
            self.pbar.update(len(exist_code))
            self.pbar.refresh()
            return code_list

        # 每只股票的下载程序
        def load_code(code):
            try:
                if code in exist_code:
                    return

                if daily_type == 'pro_bar':
                    df_code = self.TS_API.pro_bar(ts_code=code, adj='qfq', asset='E', ).set_index('trade_date')
                elif daily_type == 'daily_basic':
                    df_code = self.PRO_API.daily_basic(ts_code=code).set_index('trade_date')
                else:
                    return

                df_code.to_sql(code, self.ENGINE, if_exists='fail', index=True, schema=to_schema,
                               dtype={'trade_date': types.NVARCHAR(length=100),
                                      'ts_code': types.NVARCHAR(length=100)},
                               )

            except Exception as e:
                print(e)

        # 迭代下载
        def load_multi():
            from concurrent.futures import ThreadPoolExecutor
            # 回调
            def progress_indicator(future):
                with self.lock:  # obtain the lock
                    self.tasks_completed += 1
                    self.pbar.update(1)
                    self.pbar.refresh()
                    # print(
                    #     f'{self.tasks_completed}/{self.tasks_total} completed, {self.tasks_total - self.tasks_completed} remain.')

            with ThreadPoolExecutor(max_workers=self.MAX_CORE) as executor:
                futures = [executor.submit(load_code, code) for code in get_code_list()]
                for future in futures:
                    future.add_done_callback(progress_indicator)

        load_multi()

    def load_all_code_base(self):
        pass

        # 迭代合并

    def start_multi_task(self, func, task_list: list):
        """
        多进程处理
        """
        #
        self.pbar = tqdm(total=len(task_list))
        # self.pbar.refresh()

        from concurrent.futures import ThreadPoolExecutor

        # 回调
        def progress_indicator(future):
            with self.lock:  # obtain the lock
                self.tasks_completed += 1
                self.pbar.update(1)
                # self.pbar.refresh()

        #
        with ThreadPoolExecutor(max_workers=self.MAX_CORE) as executor:
            futures = [executor.submit(func, task) for task in tqdm(task_list)]
            for future in futures:
                future.add_done_callback(progress_indicator)

    def merge_panel_data(self, from_db_name, to_db_name, panel_name):
        # 追加模式,会重复
        if panel_name in pd.read_sql(f'SHOW TABLES FROM {to_db_name}', self.ENGINE).iloc[:, 0].to_list():
            return

        # 获取列表
        def get_code_list() -> pd.DataFrame:
            code_list = pd.read_sql(f'SHOW TABLES FROM {from_db_name}', self.ENGINE).iloc[:, 0].to_list()
            self.tasks_total = len(code_list)
            self.pbar = tqdm(range(self.tasks_total))
            self.pbar.update(0)
            self.pbar.refresh()
            return code_list

        # 每只股票的下载程序
        def append_code(code):
            try:
                pd.read_sql_table(code, self.ENGINE, schema=from_db_name).set_index(['ts_code', 'trade_date']).to_sql(
                    panel_name,
                    self.ENGINE,
                    if_exists='append',
                    index=True,
                    schema=to_db_name)
            except Exception as e:
                print(e)

        # 迭代合并
        def merge_multi():

            #
            from concurrent.futures import ThreadPoolExecutor

            # 回调
            def progress_indicator(future):
                with self.lock:  # obtain the lock
                    self.tasks_completed += 1
                    self.pbar.update(1)
                    self.pbar.refresh()

            #
            with ThreadPoolExecutor(max_workers=self.MAX_CORE) as executor:
                futures = [executor.submit(append_code, code) for code in get_code_list()]
                for future in futures:
                    future.add_done_callback(progress_indicator)

        # 联合主键
        def add_pk():
            self.ENGINE.execute(f"alter table {panel_name} add primary key(tscode,trade_date);")

        merge_multi()
        add_pk()

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

    def del_fragment(self):
        """
        删除数据库碎片
        """
        for schema in ['FIN_DAILY_TUSHARE']:  # 'FIN_DAILY_BASIC',
            self.start_multi_task(lambda x: self.ENGINE.execute(f"OPTIMIZE TABLE  {schema}.`{x}` ;"),
                                  pd.read_sql(f'SHOW TABLES FROM {schema}', self.ENGINE).iloc[:, 0].to_list())


if __name__ == '__main__':
    # 加载所有股票K线的面板数据
    loader = DownLoader(MAX_CORE=8)
    import time

    st = time.time()
    df = pd.read_sql_table('ASHARE_BASIC_PANEL', loader.ENGINE, 'FIN_PANEL_DATA',
                           columns=['ts_code', 'trade_date', 'total_mv'])
    print(df)
    print(time.time() - st)
    # loader.load_index()  # loader.load_all_code_daily('daily_basic', 'FIN_DAILY_BASIC')
    # loader.merge_panel_data('FIN_DAILY_BASIC', 'FIN_PANEL_DATA', 'ASHARE_BASIC_PANEL')
    # loader.del_fragment()
