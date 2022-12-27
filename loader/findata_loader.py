from utils.sql import DB
import pandas as pd
import numpy as np
import cudf
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
        self.MAX_CORE = kwargs.get('MAX_CORE', 10)
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
                  dtype={'trade_date': types.NVARCHAR(length=100), 'ts_code': types.NVARCHAR(length=100),
                         'name'      : types.NVARCHAR(length=100)})

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

            # 返回不同API的待查询表名
            def api_code_list():
                return {
                        'shibor'          : ['SHIBOR'],
                        'pro_bar_i'       : ['000001.SH', '000010.SH', '000015.SH', '000016.SH', '000300.SH', '000903.SH',
                                             '000905.SH', '000906.SH', '000985.SH', '399001.SZ', '399005.SZ', '399006.SZ',
                                             '399300.SZ', '399310.SZ', ],
                        'pro_bar_e'       : (
                                pd.read_sql_table('stock_basic', self.ENGINE, schema='FIN_BASIC', columns=['ts_code'])['ts_code']
                                .to_list()
                        ),
                        'daily_basic'     : (
                                pd.read_sql_table('stock_basic', self.ENGINE, schema='FIN_BASIC', columns=['ts_code'])['ts_code']
                                .to_list()
                        ),
                        'index_dailybasic': ['000001.SH', '000010.SH', '000015.SH', '000016.SH', '000300.SH', '000903.SH',
                                             '000905.SH', '000906.SH', '000985.SH', '399001.SZ', '399005.SZ', '399006.SZ',
                                             '399300.SZ', '399310.SZ', ]
                }.get(daily_api)

            # 去重
            return [i for i in api_code_list() if i not in loaded_code]

        # 每只股票的下载程序
        def load_code(code):
            def api_code_df() -> pd.DataFrame:
                return {
                        'pro_bar_e'       : self.TS_API.pro_bar(ts_code=code, adj='qfq', asset='E', ),
                        'pro_bar_i'       : self.TS_API.pro_bar(ts_code=code, adj='qfq', asset='I', ),
                        'daily_basic'     : self.PRO_API.daily_basic(ts_code=code),
                        'index_dailybasic': self.PRO_API.index_dailybasic(ts_code=code),
                        'shibor'          : (pd.concat([self.PRO_API.shibor(start_date='20140101', end_date='20220101'),
                                                        self.PRO_API.shibor(start_date='20220102', end_date='20221231')])
                                             .rename(columns={'date': 'trade_date'})),
                }.get(daily_api).set_index('trade_date').sort_index(ascending=False)

            try:
                # noinspection all
                api_code_df().to_sql(code, self.ENGINE, index=True, schema=to_schema, if_exists='fail',
                                     dtype={'trade_date': types.NVARCHAR(length=50), 'ts_code': types.NVARCHAR(length=50)})

            except Exception as e:
                print(e)

        # 迭代下载
        self.start_multi_task(load_code, get_code_list())

    def merge_panel_data(self, from_schema, to_schema, panel_table):
        """
        合并面板数据
        """
        # 建数据库
        self.create_schema(to_schema)

        # 追加模式,会重复
        if panel_table in self.get_tables(to_schema):
            return

        # 主键和索引
        def alter_table():
            # noinspection all
            sql = f"""
            ALTER table {to_schema}.{panel_table} ADD PRIMARY KEY (ts_code,trade_date);
            CREATE INDEX ix_trade_date on {to_schema}.{panel_table} (`trade_date`);
            CREATE INDEX ix_ts_code on {to_schema}.{panel_table} (`ts_code`);
            """
            self.ENGINE.execute(sql)

        # 把时间序列数据添加到面板数据
        def append_time_series(code):
            try:
                # 合并
                # noinspection all
                (pd.read_sql_table(code, self.ENGINE, schema=from_schema)
                 .to_sql(panel_table, self.ENGINE, if_exists='append', index=False, schema=to_schema,
                         dtype={'trade_date': types.NVARCHAR(length=100), 'ts_code': types.NVARCHAR(length=100)}
                         ))
            except Exception as e:
                print(e)

        # 迭代合并
        self.start_multi_task(append_time_series, self.get_tables(from_schema))

        # 加主键
        alter_table()

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

    def del_fragment(self):
        """
        删除数据库碎片
        """
        for schema in ['FIN_DAILY_TUSHARE']:  # 'FIN_DAILY_BASIC',
            self.start_multi_task(lambda x: self.ENGINE.execute(f"OPTIMIZE TABLE  {schema}.`{x}` ;"),
                                  pd.read_sql(f'SHOW TABLES FROM {schema}', self.ENGINE).iloc[:, 0].to_list())

    def load_data(self):
        """
        加载时间序列数据和合并为面板数据
        """

        # 下载时间序列数据
        def load_daily_data():
            # A股K线数据
            self.load_daily_data('pro_bar_e', 'FIN_DAILY_BAR')
            # A股基本面数据
            self.load_daily_data('pro_bar_i', 'FIN_DAILY_INDEX')
            # 指数K线数据
            self.load_daily_data('daily_basic', 'FIN_DAILY_BASIC')
            # 指数基本面数据
            self.load_daily_data('index_dailybasic', 'FIN_DAILY_INDEX_BASIC')
            # 宏观数据
            self.load_daily_data('shibor', 'FIN_DAILY_MACRO')

        # 转换面板数据
        def merge_panel_data():
            # A股K线数据
            self.merge_panel_data('FIN_DAILY_BAR', 'FIN_PANEL_DATA', 'ASHARE_BAR_PANEL')
            # A股基本面数据
            self.merge_panel_data('FIN_DAILY_BASIC', 'FIN_PANEL_DATA', 'ASHARE_BASIC_PANEL')
            # 指数K线数据
            self.merge_panel_data('FIN_DAILY_INDEX', 'FIN_PANEL_DATA', 'IDX_BAR_PANEL')
            # 指数基本面数据
            self.merge_panel_data('FIN_DAILY_INDEX_BASIC', 'FIN_PANEL_DATA', 'IDX_BASIC_PANEL')

        # 转为本地文件
        def transform_parquet():
            """
            把文件压缩好 ['ASHARE_BAR_PANEL', 'ASHARE_BASIC_PANEL']
            :return:
            """
            import os
            panel_list = ['ASHARE_BAR_PANEL', 'ASHARE_BASIC_PANEL', 'IDX_BAR_PANEL', 'IDX_BASIC_PANEL']
            for panel_table in panel_list:
                if not os.path.exists(f'/data/DataSets/investor_sentiment/{panel_table}.parquet'):
                    (pd.read_sql_table(panel_table, self.ENGINE, 'FIN_PANEL_DATA')
                     .astype(dtype={'ts_code': 'category', 'trade_date': 'uint32', })
                     .set_index(['trade_date', 'ts_code'])
                     .to_parquet(f'/data/DataSets/investor_sentiment/{panel_table}.parquet', engine='pyarrow', index=True))

        load_daily_data()
        merge_panel_data()
        transform_parquet()


class Loader(TuShare):
    """
    加载数据
    """

    def __init__(self):
        super().__init__()
        self.DATASETS_PATH = '/data/DataSets/investor_sentiment/'

    def get_conidx_panel(self) -> cudf.DataFrame:
        """
        返回指数一致预期面板数据
        :return: 面板数据
        """
        df = cudf.read_parquet(self.DATASETS_PATH + 'CON_FORECAST_IDX.parquet')
        # 筛选研究样本
        df['CON_DATE'] = df['CON_DATE'].dt.strftime('%Y%m%d')

        # 格式处理
        df = df.astype(dtype={'CON_DATE': 'uint32', 'CON_YEAR': 'uint32'})

        # 筛选样本期 短期预期 选择指数样本
        df = df[(df['CON_DATE'] >= 20131231) & ((df['CON_DATE'] // 10000) == df['CON_YEAR'])
                & ((df['INDEX_CODE'].str[:1] == '0') | (df['INDEX_CODE'].str[:1] == '3'))]

        # 改列名
        df['INDEX_CODE'] = (np.where(df['INDEX_CODE'].to_pandas().str[:1] == '0',
                                     df['INDEX_CODE'].to_pandas() + '.SH',
                                     df['INDEX_CODE'].to_pandas() + '.SZ'))

        # 方便合并
        df = df.rename(columns={'CON_DATE': 'trade_date', 'INDEX_CODE': 'ts_code'}).set_index(['trade_date', 'ts_code'])

        # 删掉不要的
        return df.drop(columns=['index', 'ID', 'ENTRYTIME', 'ENTRYTIME', 'UPDATETIME', 'TMSTAMP', 'INDEX_NAME'])

    def get_ashare_panel(self) -> cudf.DataFrame:
        """
        返回A股面板数据 需要基本面的其他指标增加columns即可
        :return:
        """
        df = (
                cudf.concat(
                        [
                                # A股K线数据
                                cudf.read_parquet(f'{self.DATASETS_PATH}ASHARE_BAR_PANEL.parquet', )
                                .rename(columns={'pct_chg': 'share_return'}),
                                # A股基本面数据
                                cudf.read_parquet(f'{self.DATASETS_PATH}ASHARE_BASIC_PANEL.parquet', ).drop(columns=['close'])
                        ],
                        join="outer", axis=1, sort=True
                ).query('trade_date>=20140101')

        )
        return df

    def get_index_panel(self) -> cudf.DataFrame:
        """
        返回指数面板数据 需要基本面的其他指标增加columns即可
        :return:
        """
        df = (
                cudf.concat(
                        [
                                # 指数K线数据
                                cudf.read_parquet(f'{self.DATASETS_PATH}IDX_BAR_PANEL.parquet', )
                                .rename(columns={'pct_chg': 'shareindex_return'}),
                                # 指数基本面数据
                                cudf.read_parquet(f'{self.DATASETS_PATH}IDX_BASIC_PANEL.parquet', ),
                        ],
                        join="outer", axis=1, sort=True
                ).query('trade_date>=20140101')

        )
        return df

    def get_time_series(self) -> cudf.DataFrame:
        """
        返回情绪+无风险收益
        :return:时间序列数据
        """
        df = (cudf.concat(
                [
                        # 情绪数据
                        cudf.from_pandas(
                                pd.concat(
                                        [pd.read_sql_table('IMG_SENT', self.ENGINE, 'SENT_DATA').astype(dtype={'trade_date': 'uint32'})
                                         .set_index('trade_date').rename(columns={'neg_index': 'img_neg'}),

                                         pd.read_sql_table('TEX_SENT', self.ENGINE, 'SENT_DATA').astype(dtype={'trade_date': 'uint32'})
                                         .set_index('trade_date').rename(columns={'neg_index': 'tex_neg'})
                                         ], axis=1
                                )),
                        # SHIBOR数据
                        cudf.from_pandas((pd.read_sql_table('SHIBOR', self.ENGINE, 'FIN_DAILY_MACRO', columns=['trade_date', '3m'])
                                          .astype(dtype={'trade_date': 'uint32'}).set_index('trade_date')
                                          .rename(columns={'3m': 'riskfree_return'}) / 360))
                ],
                axis=1, sort=True))

        return df

    def get_cross_panel_reg(self):
        """
        截面效应分析,返回用于计算异质波动率的数据
        :return:
        """
        df_panel = (
                cudf.merge(
                        # 增加用于回归的无风险利率
                        cudf.merge(
                                self.get_ashare_panel(), self.get_time_series()[['riskfree_return']],
                                on='trade_date', how='left', sort=True
                        ),
                        # 增加用于回归的市场指数
                        cudf.from_pandas(
                                self.get_index_panel()[['shareindex_return']].to_pandas().query("ts_code == '000300.SH'")
                                .reset_index('ts_code', drop=True)
                        ),
                        on='trade_date', how='left', sort=True).to_pandas()
        )
        return df_panel

    def transform_forum_data(self):  # noqa
        """
        处理从经管之家下载的excel数据为一个大的Parquet
        :return:
        """

        import os
        file_path = '/home/ubuntu/notebook/DataSets/FORUM_SENT/'

        def excel_transform():
            """
            转换excel
            :return:
            """

            import multiprocessing as mp

            pool = mp.Pool(10)

            # 任务函数
            def myfunc(name):
                df = pd.read_excel(file_path + name, header=0, engine='openpyxl', skiprows=[1, 2],
                                   dtype={'Stockcode': 'str', 'PostDate': 'str'})
                out_name = file_path + name[:-5] + '.parquet'
                df.to_parquet(out_name)
                return {name: out_name}

            results = [pool.apply_async(myfunc, args=(name,)) for name in os.listdir(file_path) if '.xlsx' in name]
            results = [p.get() for p in results]
            return results

        def merge_parquet():
            df_merge = pd.DataFrame()
            for i in os.listdir(file_path):
                if '.parquet' in i:
                    df_merge = pd.concat([df_merge, pd.read_parquet(file_path + i)])
            df_merge.to_parquet('/data/DataSets/investor_sentiment/FORUM_SENT.parquet')

        def trans_code(x):
            if x[0] == '6' or x[0] == '9': return x + '.SH'
            elif x[0] == '0' or x[0] == '2' or x[0] == '3': return x + '.SZ'
            elif x[0] == '4' or x[0] == '8': return x + '.BJ'
            else: return x

        merge_parquet()

    def extract_pdate_mediasent(self) -> cudf.DataFrame:
        """
        提取推文中的情绪,自然日期
        :return: 按照自然日期返回 cudf 'p_date', 'img_neg', 'tex_neg'
        """
        # 提取
        pd.options.mode.chained_assignment = None  # 忽略警告
        df_select = pd.read_sql(
                "SELECT id,p_date, title_neg, cover_neg, biz FROM WECAHT_DATA.articles_tag "
                "WHERE mov=10 AND biz in ('MjM5MzMwNjM0MA==','MjM5NzQ5MTkyMA==','MjY2NzgwMjU0MA==','MjY2NzgwMjU0MA==')",
                con=self.ENGINE,
                parse_dates=["p_date"])

        # 筛选
        df_select['p_date'] = df_select['p_date'].dt.strftime('%Y%m%d').astype('uint32')
        df_select = df_select.query("p_date>=20140101")

        # 阈值处理
        df_select['title_flag'] = df_select['title_neg'].apply(lambda x: 1 if x > 0.5 else 0)
        df_select['cover_flag'] = df_select['cover_neg'].apply(lambda x: 1 if x > 0.5 else 0)

        # 每日情绪
        df_select['day_article'] = df_select.groupby('p_date')['id'].transform('count')

        df_select['tex_neg'] = df_select['title_flag'] / df_select['day_article']
        df_select['img_neg'] = df_select['cover_flag'] / df_select['day_article']

        # 时间序列
        df_select = df_select.groupby(['p_date', ], as_index=False).first()
        # 筛选
        return cudf.from_pandas(df_select[['p_date', 'img_neg', 'tex_neg']]).set_index('p_date').sort_index().reset_index()

    def extract_tdate_mediasent(self) -> cudf.DataFrame:
        """
        按照交易日返回媒体情绪
        :return:按照交易日期返回 cudf 'p_date', 'img_neg', 'tex_neg'
        """
        return self.get_time_series()[['img_neg', 'tex_neg', ]].sort_index().reset_index().dropna(axis=0)

