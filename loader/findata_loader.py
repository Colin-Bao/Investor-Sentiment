from utils.sql import Base
import pandas as pd
from tqdm import tqdm


class TuShare(Base):
    def __init__(self):
        super(TuShare, self).__init__()
        self.__TOKEN = 'b078ff41ce1988e87fb5b2e2c13c6a8a8b89b85289f4cc842b269f8d'
        import tushare as ts
        ts.set_token(self.__TOKEN)
        self.TS_API = ts
        self.PRO_API = ts.pro_api()


class DownLoader(TuShare):
    def __init__(self, SHAREINDEX_LIST):
        super(DownLoader, self).__init__()
        self.SHAREINDEX_LIST = SHAREINDEX_LIST
        from threading import Lock
        self.lock = Lock()
        self.tasks_total = len(self.get_index_members())
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
                # ma = [5, 10, 30],
                # factors = ['tor', 'vr']
                self.save_sql(df_code, code)
                # self.ENGINE.execute(f"CREATE INDEX ix_{code.replace('.', '')}_trade_date ON '{code}' (trade_date)")
            except Exception as e:
                print(e)

    # def load_code_

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

    def load_shibor(self):
        if 'shibor' in self.TABLE_LIST:
            return
        df_1 = self.PRO_API.shibor(start_date=self.START_DATE, end_date=str(int(self.START_DATE) + 30000))
        df_2 = self.PRO_API.shibor(start_date=str(int(self.START_DATE) + 30000), end_date=self.END_DATE)
        df = pd.concat([df_1, df_2], axis=0).rename(columns={'date': 'trade_date'}).sort_values('trade_date',
                                                                                                ascending=False)
        # print(df)
        self.save_sql(df, 'shibor')


class FinDerCalulator(Base):
    """
    衍生数据计算
    """

    def __init__(self, OLS_WINDOW: int, RS_WINDOW, QUANTILE=0.4, REFER_INDEX='399300.SZ'):
        super(FinDerCalulator, self).__init__()
        self.OLS_WINDOW = OLS_WINDOW
        self.RS_WINDOW = RS_WINDOW
        self.REFER_INDEX = REFER_INDEX
        self.QUANTILE = QUANTILE

    def cal_sentiment_r(self) -> pd.DataFrame:
        """
        计算情绪残差
        """

        def cal_by_code(code, df_sent_daily, df_shibor_daily):
            """
            分组计算异质波动率
            """

            def extract_code():
                # 拼接市场收益率
                df_extract = self.get_code_daily(code).sort_values('trade_date', ascending=True).set_index(
                    'trade_date').join(df_sent_daily, how='left').join(df_shibor_daily, how='left')
                return df_extract

            def roll_regression(df_code):
                # 计算回归系数
                from statsmodels.regression.rolling import RollingOLS
                df_ols = pd.DataFrame()

                # 估计img
                df_ols['Y'], df_ols['const'], df_ols['X'] = df_code['pct_chg'], 1, df_code['img_sent']
                df_ols = df_ols.replace(0, 0.000001)  # 除0 错误
                model = RollingOLS(endog=df_ols['Y'].values, exog=df_ols[['const', 'X']], window=5)
                df_para = model.fit().params
                df_residual_img = (df_para['const'] + df_ols['X'] * df_para['X']) - df_ols['Y']

                # 估计text
                df_ols['X'] = df_code['text_sent']
                model = RollingOLS(endog=df_ols['Y'].values, exog=df_ols[['const', 'X']], window=5)
                df_para = model.fit().params
                df_residual_text = (df_para['const'] + df_ols['X'] * df_para['X']) - df_ols['Y']
                return pd.concat([df_code, df_residual_img, df_residual_text], axis=1).rename(
                    columns={0: 'r_img', 1: 'r_text'})

            return roll_regression(extract_code().fillna(0))

        def concat_panel(save_name):
            df_sent = self.get_sent_index().set_index('trade_date')
            df_code_panel = pd.DataFrame()
            for code in tqdm([i for i in self.get_index_members() if i in self.TABLE_LIST]):
                try:
                    df_code_panel = cal_by_code(code, df_sent).reset_index().sort_values(
                        by=['trade_date'], ascending=True)
                    self.save_sql(df_code_panel, save_name, if_exists='append')

                except Exception as e:
                    print(code, e)
                    continue

        def cal_by_index():
            return cal_by_code('399300.SZ', self.get_sent_index().set_index('trade_date'),
                               self.get_shibor().set_index('trade_date')[['1m']])

        return cal_by_index()
        # table_idvol = f'ASHARE_panel_O{self.OLS_WINDOW}_R{self.RS_WINDOW}_r'
        # if table_idvol not in self.TABLE_LIST:
        #     concat_panel(table_idvol)

    def cal_idvol(self, method: str = 'CAPM'):
        """
        计算异质波动率
        """

        def cal_by_code(code, df_index_daily, df_shibor_daily):
            """
            分组计算异质波动率
            """

            def extract_code():
                # 拼接市场收益率
                df_extract = self.get_code_daily(code).sort_values('trade_date', ascending=True).set_index(
                    'trade_date').join(
                    df_index_daily[['pct_chg']].rename(columns={'pct_chg': 'index_pct_chg'}), how='left')
                df_extract = df_extract.join(df_shibor_daily[['on']].rename(columns={'on': 'shibor_rf'}),
                                             how='left')
                return df_extract.fillna(0)

            def roll_regression(df_code):
                # 计算回归系数
                from statsmodels.regression.rolling import RollingOLS
                df_ols = pd.DataFrame()
                df_ols['Y'] = df_code['pct_chg'] - df_code['shibor_rf']
                df_ols['const'] = 1
                df_ols['X'] = df_code['index_pct_chg'] - df_code['shibor_rf']
                model = RollingOLS(endog=df_ols['Y'].values, exog=df_ols[['const', 'X']], window=5)
                df_para = model.fit().params
                df_para['Y_HAT'] = df_para['const'] + df_ols['X'] * df_para['X']
                df_para['residual'] = df_para['Y_HAT'] - df_ols['Y']
                return pd.concat([df_code, df_para['residual']], axis=1)

            def cal_residual_square(df_res):
                import numpy as np
                df_res['residual_var'] = df_res[['residual']].rolling(self.RS_WINDOW).apply(lambda x: np.var(x, ddof=1))
                return df_res

            return cal_residual_square(roll_regression(extract_code()))

        def concat_panel(save_name):
            df_code_panel = pd.DataFrame()
            df_index_daily = self.get_code_daily(self.REFER_INDEX).set_index('trade_date')
            df_shibor_daily = self.get_shibor().set_index('trade_date')
            for code in tqdm([i for i in self.get_index_members() if i in self.TABLE_LIST]):
                try:
                    df_code_panel = cal_by_code(code, df_index_daily, df_shibor_daily).reset_index().sort_values(
                        by=['trade_date'], ascending=True)
                    self.save_sql(df_code_panel, save_name, if_exists='append')

                except Exception as e:
                    print(code, e)
                    continue

        table_idvol = f'csi300_panel_O{self.OLS_WINDOW}_R{self.RS_WINDOW}'
        if table_idvol not in self.TABLE_LIST:
            concat_panel(table_idvol)

    def cal_high_low(self):

        def extract_data():
            def extract_mv_panel():
                df_select = pd.read_sql(
                    f"SELECT ann_date,stockcode,s_val_mv FROM ASHARE_MV WHERE ann_date BETWEEN :sd AND :ed ",
                    con=self.ENGINE, params={'sd': int(self.START_DATE), 'ed': int(self.END_DATE), })
                return df_select.rename(columns={'ann_date': 'trade_date', 'stockcode': 'ts_code'})

            def extract_code_panel():
                df_select = pd.read_sql('SELECT * FROM csi300_panel_O5_R30 ', self.ENGINE)
                return df_select

            def extract():
                if 'temp_panel_merge' not in self.TABLE_LIST:
                    df_mer = pd.merge(extract_code_panel(), extract_mv_panel(), how='left',
                                      on=['trade_date', 'ts_code'])
                    self.save_sql(df_mer, 'temp_panel_merge')
                return pd.read_sql('SELECT * FROM temp_panel_merge ', self.ENGINE).sort_values(
                    by=['trade_date', 'ts_code'],
                    ascending=True)

            return extract()

        def cal_by_group(df_extract):
            import numpy as np
            df_e = df_extract[['trade_date', 'ts_code', 'pct_chg', 's_val_mv', 'residual_var']].set_index(
                'trade_date')

            # ------------------------按照residual_var分组------------------------#
            df_e['rv_q_top'] = df_e['residual_var'].groupby(df_e.index).transform(
                lambda x: x.quantile(self.QUANTILE))
            df_e['rv_q_bom'] = df_e['residual_var'].groupby(df_e.index).transform(
                lambda x: x.quantile(1 - self.QUANTILE))
            df_e['rv_group'] = np.where(df_e['residual_var'] >= df_e['rv_q_top'], 'high', "mid")
            df_e['rv_group'] = np.where(df_e['residual_var'] <= df_e['rv_q_bom'], 'low', df_e['rv_group'])

            # 求组中市值加权系数,并求回报
            df_e['rv_group_ratio'] = df_e['s_val_mv'] / df_e.groupby([df_e.index, 'rv_group'])['s_val_mv'].transform(
                lambda x: sum(x))
            df_e['rv_group_return'] = df_e['rv_group_ratio'] * df_e['pct_chg']
            df_e['rv_vw_return'] = df_e.groupby([df_e.index, 'rv_group'])['rv_group_return'].transform(lambda x: sum(x))

            # ------------------------按照市值分组------------------------#
            df_e['mv_q_top'] = df_e['s_val_mv'].groupby(df_e.index).transform(
                lambda x: x.quantile(self.QUANTILE))
            df_e['mv_q_bom'] = df_e['s_val_mv'].groupby(df_e.index).transform(
                lambda x: x.quantile(1 - self.QUANTILE))
            df_e['mv_group'] = np.where(df_e['s_val_mv'] >= df_e['mv_q_top'], 'high', "mid")
            df_e['mv_group'] = np.where(df_e['s_val_mv'] <= df_e['mv_q_bom'], 'low', df_e['mv_group'])

            # 求组中市值加权系数,并求回报
            df_e['mv_group_ratio'] = df_e['s_val_mv'] / df_e.groupby([df_e.index, 'mv_group'])['s_val_mv'].transform(
                lambda x: sum(x))
            df_e['mv_group_return'] = df_e['mv_group_ratio'] * df_e['pct_chg']
            df_e['mv_vw_return'] = df_e.groupby([df_e.index, 'mv_group'])['mv_group_return'].transform(lambda x: sum(x))

            # ------------------------转为时间序列数据------------------------#
            df_rv = df_e[['rv_group', 'rv_vw_return']].reset_index().set_index(['trade_date', 'rv_group'])
            df_rv = df_rv[~df_rv.index.duplicated(keep='last')].reset_index()
            df_rv = df_rv.pivot(index='trade_date', columns='rv_group', values='rv_vw_return')
            df_rv['high_low'] = df_rv['high'] - df_rv['low']  # 求高-低
            self.save_sql(df_rv.fillna(0).reset_index(), 'rv_vw_return')

            df_mv = df_e[['mv_group', 'mv_vw_return']].reset_index().set_index(['trade_date', 'mv_group'])
            df_mv = df_mv[~df_mv.index.duplicated(keep='last')].reset_index()
            df_mv = df_mv.pivot(index='trade_date', columns='mv_group', values='mv_vw_return')
            df_mv['high_low'] = df_mv['high'] - df_mv['low']  # 求高-低
            self.save_sql(df_mv.reset_index(), 'mv_vw_return')

        cal_by_group(extract_data())


# with DownLoader(['000001.SH', '399001.SZ', '000011.SH', '399300.SZ']) as DownLoader:
#     DownLoader.load_index()
#     DownLoader.load_index_members('399300.SZ')
#     DownLoader.load_shibor()

with FinDerCalulator(5, 30, ) as Calulator:
    # 计算滑动值
    # 投资
    import numpy as np


    def cal_return(df, MA):
        df.dropna(axis=0, inplace=True)
        df[f'ma{MA}_t'] = (df['text_sent'].rolling(MA).mean())
        df[f'ma{MA}_i'] = (df['img_sent'].rolling(MA).mean())

        # 历史均值
        df['is_ma_img'] = (df['img_sent'] >= df[f'ma{MA}_i'])
        df['is_ma_text'] = (df['text_sent'] >= df[f'ma{MA}_t'])
        df['is_ma_img'] = df['is_ma_img'].shift(1)
        df['is_ma_text'] = df['is_ma_text'].shift(4)

        df['return_img'] = np.where(df['is_ma_img'], -1 * (df['is_ma_img'] * df['pct_chg']), df['pct_chg'])
        df['return_text'] = np.where(df['is_ma_text'], -1 * (df['is_ma_text'] * df['pct_chg']), df['pct_chg'])

        # 换算
        df.dropna(axis=0, inplace=True)

        df['mv_csi300'] = (df['pct_chg'] + 100) / 100
        df['mv_text'] = (df['return_text'] + 100) / 100
        df['mv_img'] = (df['return_img'] + 100) / 100

        df['mv_csi300'] = df['mv_csi300'].cumprod(axis=0)
        df['mv_text'] = df['mv_text'].cumprod(axis=0)
        df['mv_img'] = df['mv_img'].cumprod(axis=0)
        df = df.rename(columns={'mv_img': f'mv_img_{MA}', 'mv_text': f'mv_text_{MA}'})

        return df


    df_in = Calulator.cal_sentiment_r()
    for i in [5, 10, 15, 20, 25, 30]:
        df_in = cal_return(df_in, i)

    df_in = df_in[[i for i in df_in.columns if 'mv_' in i]]
    df_in.to_csv('invest.csv')
