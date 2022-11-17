from utils.sql import DB


class TuShare(DB):
    def __init__(self):
        super(TuShare, self).__init__()
        self.__TOKEN = 'b078ff41ce1988e87fb5b2e2c13c6a8a8b89b85289f4cc842b269f8d'
        import tushare as ts
        ts.set_token(self.__TOKEN)
        self.TS_API = ts
        # self.PRO_API = ts.pro_api()


class DownLoader(TuShare):
    def __init__(self, INDEX_LIST):
        super(DownLoader, self).__init__()
        self.INDEX_LIST = INDEX_LIST

    def load_index(self):
        def extract(code): return self.TS_API.pro_bar(ts_code=code, adj='qfq', asset='I',
                                                      start_date=self.START_DATE, end_date=self.END_DATE)

        _ = [self.save_sql(extract(index), index) for index in self.INDEX_LIST]
