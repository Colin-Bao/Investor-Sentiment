import pandas as pd
from configer.base_config import Base


class SentCalculator(Base):

    def __init__(self):
        super(SentCalculator, self).__init__()
        self.TRADE_TABLE = '399300.SZ'  # 用作指数计算
        self.MAP_TABLE = 'map_date'
        self.UPDATE_LIMIT = 2000  # 分片更新
        self.NEG_VALUE = 0.55  # 临界值
        self.GZH_LIST = ['中国证券报', '财新网', '央视财经', '界面新闻']
        self.NEG_COLUMN = 'cover_neg'
        self.SAVE_NAME = f'img_sent_{len(self.GZH_LIST)}_{int(self.NEG_VALUE * 100)}'

    def map_trade_date(self):
        """
        映射交易日期\n
        :return:
        """

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
                f"SELECT id,p_date FROM '{self.ARTICLE_TABLE}' WHERE mov=:mov AND p_date BETWEEN :sd AND :ed "
                f"AND t_date IS NULL LIMIT {self.UPDATE_LIMIT}",
                con=self.ENGINE,
                params={'mov': 10,
                        'sd': int(pd.to_datetime(self.START_DATE).timestamp()),
                        'ed': int(pd.to_datetime(self.END_DATE).timestamp()), },
                parse_dates=["p_date"])
            df_select['map_date'] = pd.to_datetime(df_select['p_date'].dt.strftime("%Y-%m-%d 00:00:00"))
            return df_select

        def update_by_limit():
            count = 0
            while True:
                count += 1
                df_publish_date = extract_publish_date()
                if df_publish_date.empty:
                    break
                else:
                    print(count)
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

    def extract_panel_data(self):
        """
        转为标准的面板数据用于计算/n
        :return:
        """

        def extract():
            df_select = pd.read_sql(
                f"SELECT a.t_date,gzhs.nickname,a.title,a.cover_local,a.cover_neg "
                f"FROM gzhs LEFT JOIN {self.ARTICLE_TABLE} AS a ON gzhs.biz = a.biz "
                "WHERE a.mov=:mov AND a.p_date BETWEEN :sd AND :ed "
                "AND a.cover_neg IS NOT NULL AND a.t_date IS NOT NULL ",
                con=self.ENGINE,
                params={'mov': 10,
                        'sd': int(pd.to_datetime(self.START_DATE).timestamp()),
                        'ed': int(pd.to_datetime(self.END_DATE).timestamp())},
                parse_dates=["t_date"])

            # 转换
            df_select['t_date'] = df_select['t_date'].dt.strftime("%Y%m%d")
            return df_select[df_select['nickname'].isin(self.GZH_LIST)]

        return extract()

    def cal_sentiment_index(self):
        import numpy as np
        # 提取
        df_select = self.extract_panel_data()
        # 设定阈值
        df_select['is_neg'] = np.where(df_select[self.NEG_COLUMN] >= self.NEG_VALUE, 1, 0)
        # 聚合
        df_group = (df_select.groupby('t_date')
                    .agg({'is_neg': 'sum', 't_date': 'count'})
                    .rename(columns={'is_neg': 'neg_count', 't_date': 'all_count'}).reset_index())
        # 图像情绪指数
        df_group['img_neg'] = df_group['neg_count'] / df_group['all_count']
        # 储存
        df_group[['t_date', 'img_neg']].to_sql(self.SAVE_NAME, self.ENGINE, index=False, if_exists='replace')


class RegCalculator(Base):
    def __init__(self):
        super(RegCalculator, self).__init__()
        from pystata import config
        config.init('mp')
        from pystata import stata
        self.STATA_API = stata
        self.df_stata = pd.DataFrame()

    def set_df_to_stata(self, df: pd.DataFrame):
        self.STATA_API.pdataframe_to_data(df, force=True)
        self.df_stata = df

    def run_stata_do(self, stata_str):
        self.STATA_API.run(stata_str)


with RegCalculator() as RegCalculator:
    RegCalculator.run_stata_do('li')
