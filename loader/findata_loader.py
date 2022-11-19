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
            df_code = self.TS_API.pro_bar(ts_code=code, adj='qfq', asset='E',
                                          start_date=self.START_DATE, end_date=self.END_DATE, ma=[5, 10, 30],
                                          factors=['tor', 'vr'])
            self.save_sql(df_code, code)
            self.ENGINE.execute(f"CREATE INDEX ix_{code.replace('.', '')}_trade_date ON '{code}' (trade_date)")

    # def load_code_

    def load_index_members(self, index):
        for code in tqdm(self.get_index_members(index)):
            try:
                self.load_code(code)
            except Exception as e:
                print(e, '\n')
                continue

    def load_shibor(self):
        if 'shibor' in self.TABLE_LIST:
            return
        df_1 = self.PRO_API.shibor(start_date=self.START_DATE, end_date=str(int(self.START_DATE) + 30000))
        df_2 = self.PRO_API.shibor(start_date=str(int(self.START_DATE) + 30000), end_date=self.END_DATE)
        df = pd.concat([df_1, df_2], axis=0).rename(columns={'date': 'trade_date'}).sort_values('trade_date',
                                                                                                ascending=False)
        self.save_sql(df, 'shibor')


class FinDerCalulator(Base):
    """
    衍生数据计算
    """

    def __init__(self, OLS_WINDOW: int, RS_WINDOW, REFER_INDEX):
        super(FinDerCalulator, self).__init__()
        self.OLS_WINDOW = OLS_WINDOW
        self.RS_WINDOW = RS_WINDOW
        self.REFER_INDEX = REFER_INDEX

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

        def concat_panel():
            df_code_panel = pd.DataFrame()
            df_index_daily = self.get_code_daily(self.REFER_INDEX).set_index('trade_date')
            df_shibor_daily = self.get_shibor().set_index('trade_date')
            for code in tqdm([i for i in self.get_index_members(self.REFER_INDEX) if i in self.TABLE_LIST]):
                df_code_panel = pd.concat([df_code_panel, cal_by_code(code, df_index_daily, df_shibor_daily)], axis=0)
            return df_code_panel.reset_index().sort_values(by=['trade_date', 'ts_code'], ascending=False)

        table_idvol = f'csi300_panel_O{self.OLS_WINDOW}_R{self.RS_WINDOW}'
        if table_idvol not in self.TABLE_LIST:
            self.save_sql(concat_panel(), table_idvol)

    def cal_high_low(self):

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
                df_mer = pd.merge(extract_code_panel(), extract_mv_panel(), how='left', on=['trade_date', 'ts_code'])
                self.save_sql(df_mer, 'temp_panel_merge')
            return pd.read_sql('SELECT * FROM temp_panel_merge', self.ENGINE)

        df = extract()
        print(df)


# with DownLoader(['000001.SH', '399001.SZ', '000011.SH', '399300.SZ']) as DownLoader:
#     DownLoader.load_index()
#     DownLoader.load_index_members('399300.SZ')
#     DownLoader.load_shibor()

with FinDerCalulator(5, 30, '399300.SZ') as Calulator:
    Calulator.cal_idvol('CAPM')
    Calulator.cal_high_low()
