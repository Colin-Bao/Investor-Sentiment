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
                load_index_daily(idx)
                load_index_weight(idx)
                # if idx not in self.SHAREINDEX_TABLES:

        load()

    def load_code(self, code):
        return self.TS_API.pro_bar(ts_code=code, adj='qfq', asset='E',
                                   start_date=self.START_DATE, end_date=self.END_DATE, ma=[5, 10, 30],
                                   factors=['tor', 'vr'])

    def load_index_members(self, index):
        for code in tqdm(self.get_index_members(index)):
            if code in self.TABLE_LIST:
                continue
            try:
                self.save_sql(self.load_code(code), code)
                self.ENGINE.execute(f"CREATE INDEX ix_{code.replace('.', '')}_trade_date ON '{code}' (trade_date)")
            except Exception as e:
                print(e, '\n')
                continue


class FinDerCalulator(Base):
    """
    衍生数据计算
    """

    def __init__(self, VAR_PERIOD: int, REFER_INDEX):
        super(FinDerCalulator, self).__init__()
        self.OBSERVE_PERIOD = VAR_PERIOD
        self.REFER_INDEX = REFER_INDEX

    def cal_idvol(self, method: str = 'CAPM'):
        """
        计算异质波动率
        """

        def cal_by_code(code, df_index_daily):
            """
            分组计算异质波动率
            """

            def extract_code():
                # 拼接市场收益率
                return self.get_code_daily(code).sort_values('trade_date', ascending=True).set_index(
                    'trade_date').join(
                    df_index_daily[['pct_chg']].rename(columns={'pct_chg': 'index_pct_chg'}), how='left').fillna(0)

            def roll_regression(df_code):
                from sklearn.linear_model import LinearRegression

                # 计算回归系数
                def reg():
                    linreg = LinearRegression()
                    Y, X = df_code['pct_chg'].to_numpy().reshape(-1, 1), df_code['index_pct_chg'].to_numpy().reshape(-1,
                                                                                                                     1)
                    linreg.fit(X, Y)
                    Y_PRED = linreg.predict(X)
                    Y_Residual = Y_PRED - Y
                    return Y_Residual

                reg()

            return roll_regression(extract_code())

        def cal():
            df_code_panel = pd.DataFrame()
            df_index_daily = self.get_code_daily(self.REFER_INDEX)
            for code in [i for i in self.get_index_members(self.REFER_INDEX) if i in self.TABLE_LIST]:
                df_code_panel = pd.concat([df_code_panel, cal_by_code(code, df_index_daily)], axis=0)
                break
            return df_code_panel

        df = cal()
        print(df)

        # df_index = pd.merge(df_index, cal_by_code(c), how='left', left_on=['trade_date', 'con_code'],
        #                     right_on=['trade_date', 'ts_code'])
        # extract_index()
        # self.ENGINE.execute("ALTER TABLE '000001.SZ' ADD test FLOAT")

    # def cal_


# with DownLoader(['399300.SZ', ]) as DownLoader:
#     DownLoader.load_index()
#     DownLoader.load_index_members('399300.SZ')

with FinDerCalulator(30, '399300.SZ') as Calulator:
    # DerCalulator
    Calulator.cal_idvol('CAPM')
