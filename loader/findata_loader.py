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
            self.save_sql(df_weight_con, index + '_weight')

        def load():
            for idx in self.SHAREINDEX_LIST:
                if idx not in self.SHAREINDEX_TABLES:
                    load_index_daily(idx)
                    load_index_weight(idx)

        load()

        # _ = [self.save_sql(extract_daily(index), index) for index in self.SHAREINDEX_LIST if
        #      index not in self.SHAREINDEX_TABLES]
        # print(self.SHAREINDEX_TABLES)

    def load_index_members(self, index):
        pass


class DerCalulator(TuShare):
    """
    衍生数据计算
    """

    def __init__(self):
        super(DerCalulator, self).__init__()


with DownLoader(['399300.SZ', ]) as DownLoader:
    DownLoader.load_index()
