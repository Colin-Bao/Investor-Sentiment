import pandas as pd
from configer.base_config import Base


class Analyzer(Base):

    def __init__(self):
        super(Analyzer, self).__init__()
        self.TRADE_TABLE = '399300.SZ'
        self.MAP_TABLE = 'map_date'
        self.UPDATE_LIMIT = 2000

    def map_trade_date(self):
        def extract_trade_date():
            return pd.read_sql(
                f"SELECT trade_date FROM '{self.TRADE_TABLE}' ", con=self.ENGINE, parse_dates=["trade_date"])

        def gen_map_table():
            # 生成
            df_date = pd.DataFrame(pd.date_range(self.START_DATE, self.END_DATE)).rename(
                columns={0: 'nature_date'})
            # 匹配
            df_date = pd.merge(df_date, extract_trade_date(), left_on='nature_date', right_on='trade_date', how='left')
            df_date['nature_date'] = pd.to_datetime(df_date['nature_date'].dt.strftime("%Y-%m-%d 15:00:00"))

            # 填充
            df_date['map_l0'] = df_date['trade_date'].fillna(method='bfill')
            df_date['map_l1'] = df_date['map_l0'].shift(-1)

            # 保存
            df_date.to_sql(self.MAP_TABLE, self.ENGINE, index=False, if_exists='replace')

        def extract_map_table():
            df_select = pd.read_sql(
                f"SELECT nature_date,trade_date,map_l0,map_l1 FROM '{self.MAP_TABLE}' ",
                con=self.ENGINE, parse_dates=['nature_date', 'trade_date', 'map_l0', 'map_l1'], )
            df_select['map_date'] = pd.to_datetime(df_select['nature_date'].dt.strftime("%Y-%m-%d 00:00:00"))
            return df_select

        def extract_publish_date():
            df_select = pd.read_sql(
                f"SELECT id,p_date FROM '{self.ARTICLE_TABLE}' WHERE mov=:mov AND t_date IS NULL LIMIT {self.UPDATE_LIMIT}",
                con=self.ENGINE, params={'mov': 10}, parse_dates=["p_date"])
            df_select['map_date'] = pd.to_datetime(df_select['p_date'].dt.strftime("%Y-%m-%d 00:00:00"))
            return df_select

        def update_by_limit():
            count = 0
            while True:
                print(count)
                count += 1
                df_publish_date = extract_publish_date()
                if df_publish_date.empty:
                    break
                else:
                    # 匹配
                    df_con = pd.merge(df_publish_date, extract_map_table(), on='map_date', how='left')
                    df_con['t_date'] = (df_con['map_l0']).where(df_con['p_date'] <= df_con['nature_date'],
                                                                df_con['map_l1'])
                    df_con = df_con[['id', 't_date']]
                    # 更新
                    self.update_by_temp(df_con, self.ARTICLE_TABLE, 't_date', 'id')

        # 生成表
        if self.MAP_TABLE not in self.TABLE_LIST:
            gen_map_table()
        # 分片更新
        update_by_limit()

    def cal_img_sentiment(self):
        pass


if __name__ == '__main__':
    with Analyzer() as Analyzer:
        Analyzer.map_trade_date()
