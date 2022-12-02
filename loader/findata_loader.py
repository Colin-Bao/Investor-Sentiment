import time

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
    """
    多进程下载器
    """

    def __init__(self, **kwargs):
        super(DownLoader, self).__init__(**kwargs)
        self.lock = None
        self.tasks_total = 0
        self.tasks_completed = 0
        self.pbar = None
        # 用于下载的线程数量
        self.MAX_CORE = kwargs.get('MAX_CORE', 4)
        # 已经存储的信息
        self.SCHEMA_LIST = self.get_schemas()

    def start_multi_task(self, func, task_list: list):
        """
        多进程处理
        """
        from concurrent.futures import ThreadPoolExecutor
        from threading import Lock
        #
        self.lock = Lock()
        self.tasks_total = len(task_list)
        self.tasks_completed = 0
        self.pbar = tqdm(total=len(task_list))

        # 回调
        def progress_indicator(future_arg):
            with self.lock:  # obtain the lock
                self.tasks_completed += 1
                self.pbar.update(1)

        #
        with ThreadPoolExecutor(max_workers=self.MAX_CORE) as executor:
            futures = [executor.submit(func, task) for task in tqdm(task_list)]
            for future in futures:
                future.add_done_callback(progress_indicator)

    def load_stock_basic(self, db_name='FIN_BASIC'):
        """
        下载基本所有股票信息表
        :return:
        """
        df = self.PRO_API.query('stock_basic', exchange='', list_status='L',
                                fields='ts_code,symbol,name,area,industry,list_date').set_index('ts_code')

        # 先找数据库
        self.create_schema(db_name)

        df.to_sql('stock_basic', self.ENGINE, index=True, if_exists='replace', schema=db_name,
                  dtype={'trade_date': types.NVARCHAR(length=100), 'ts_code': types.NVARCHAR(length=100), 'name': types.NVARCHAR(length=100)})

    def load_daily_data(self, daily_api: str, to_schema: str):
        """
        下载所有的时间序列信息
        """

        # 获取股票列表
        def get_code_list() -> list:
            # 先找数据库
            _ = self.load_stock_basic() if 'FIN_BASIC' not in self.SCHEMA_LIST else None

            # 已经下完的列表
            self.create_schema(to_schema)
            loaded_code = self.get_tables(to_schema)

            def api_code_list():
                return {'pro_bar_i': ['399300.SZ'], 'shibor': ['SHIBOR']
                        }.get(daily_api, pd.read_sql_table('stock_basic', self.ENGINE, schema='FIN_BASIC', columns=['ts_code'])['ts_code'].to_list())

            # 去重
            return [i for i in api_code_list() if i not in loaded_code]

        # 每只股票的下载程序
        def load_code(code):
            def api_code_df() -> pd.DataFrame:
                return {
                    'pro_bar_e': self.TS_API.pro_bar(ts_code=code, adj='qfq', asset='E', ),
                    'pro_bar_i': self.TS_API.pro_bar(ts_code=code, adj='qfq', asset='I', ),
                    'daily_basic': self.PRO_API.daily_basic(ts_code=code),
                    'shibor': (pd.concat([self.PRO_API.shibor(start_date='20140101', end_date='20220101'),
                                          self.PRO_API.shibor(start_date='20220102', end_date='20221231')]).rename(columns={'date': 'trade_date'})),
                }.get(daily_api).set_index('trade_date').sort_index(ascending=False)

            try:
                api_code_df().to_sql(code, self.ENGINE, index=True, schema=to_schema, if_exists='fail',
                                     dtype={'trade_date': types.NVARCHAR(length=100), 'ts_code': types.NVARCHAR(length=100)})

            except Exception as e:
                print(e)

        # 迭代下载
        self.start_multi_task(load_code, get_code_list())

    def merge_panel_data(self, from_schema, to_schema, panel_table):
        """
        合并面板数据
        """
        # 追加模式,会重复
        if panel_table in self.get_tables(to_schema):
            return

        # 主键和索引
        def alter_table():
            sql = f"""
            ALTER table {to_schema}.{panel_table} ADD PRIMARY KEY (tscode,trade_date);
            CREATE INDEX ix_trade_date on {to_schema}.{panel_table} (`trade_date`);
            CREATE INDEX ix_ts_code on {to_schema}.{panel_table} (`ts_code`);
            """
            self.ENGINE.execute(sql)

        # 把时间序列数据添加到面板数据
        def append_time_series(code):
            try:
                # 合并
                (pd.read_sql_table(code, self.ENGINE, schema=from_schema)
                 .to_sql(panel_table, self.ENGINE, if_exists='append', index=False, schema=to_schema,
                         dtype={'trade_date': types.NVARCHAR(length=100), 'ts_code': types.NVARCHAR(length=100)}))
            except Exception as e:
                print(e)

        # 建数据库
        self.create_schema(to_schema)

        # 迭代合并
        self.start_multi_task(append_time_series, self.get_tables(from_schema))

        # 加主键
        alter_table()

    def transform_parquet(self, from_db_name, table_name, columns: list):
        """
        转换数据库为parquet文件方便计算
        """
        # .astype(dtype={'ts_code': 'category', 'trade_date': 'category'})
        st = time.time()
        df = (pd.read_sql_table(table_name, self.ENGINE, schema=from_db_name, columns=columns)
              )
        print(df)
        print(time.time() - st)
        # df.to_feather(f'/home/colin/Investor-Sentiment/DataSets/{table_name}.feather')
        # print(time.time() - st)
        # pd.read_sql_table(table_name, self.ENGINE, schema=from_db_name, columns=columns).to_parquet(
        #     f'{table_name}.parquet')

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

        # def load():
        #     for idx in self.SHAREINDEX_LIST:
        #         if idx + '_weight' in self.TABLE_LIST:
        #             continue
        #         load_index_daily(idx)
        #         load_index_weight(idx)
        #
        # load()

    def del_fragment(self):
        """
        删除数据库碎片
        """
        for schema in ['FIN_DAILY_TUSHARE']:  # 'FIN_DAILY_BASIC',
            self.start_multi_task(lambda x: self.ENGINE.execute(f"OPTIMIZE TABLE  {schema}.`{x}` ;"),
                                  pd.read_sql(f'SHOW TABLES FROM {schema}', self.ENGINE).iloc[:, 0].to_list())


if __name__ == '__main__':
    # 加载所有股票K线的面板数据
    loader = DownLoader(MAX_CORE=10)
    # loader.load_stock_basic()
    # loader.transform_parquet('COLIN_PANEL', 'TEMP_PANEL_FINAL', ['ts_code', 'trade_date', 'total_mv', ])
    loader.load_daily_data('pro_bar_e', 'FIN_DAILY_BAR')
    loader.load_daily_data('pro_bar_i', 'FIN_DAILY_INDEX')
    loader.load_daily_data('shibor', 'FIN_DAILY_INDEX')

# from loader import findata_loader
# loader.load_index()  # loader.load_all_code_daily('daily_basic', 'FIN_DAILY_BASIC')
# loader.merge_panel_data('FIN_DAILY_BASIC', 'FIN_PANEL_DATA', 'ASHARE_BASIC_PANEL')
# loader.del_fragment()
