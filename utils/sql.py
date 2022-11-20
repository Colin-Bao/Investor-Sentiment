import pandas as pd


class DB:

    def __init__(self):
        # -------------------------------数据库后端---------------------------------#
        from sqlalchemy import create_engine
        import sqlalchemy
        self.SQL_API = sqlalchemy
        self.ARTICLE_TABLE = 'articles_copy1'  # 引用的数据库
        self.ENGINE = create_engine('sqlite:////Users/mac/PycharmProjects/wcplusPro7.31/db_folder/data-dev.db',
                                    echo=False, connect_args={'check_same_thread': False})

        # -------------------------------使用配置---------------------------------#
        self.START_DATE = '20140101'
        self.END_DATE = '20221231'

    def save_sql(self, df_save: pd.DataFrame, name: str, if_exists='replace', schema=None) -> None: df_save.to_sql(name,
                                                                                                                   self.ENGINE,
                                                                                                                   index=False,
                                                                                                                   schema=schema,
                                                                                                                   if_exists=if_exists)

    def __enter__(self): return self

    def __exit__(self, exc_type, exc_val, exc_tb): self.ENGINE.dispose()


class Base(DB):
    """
    基础类\n
    """

    def __init__(self):
        super(Base, self).__init__()
        # -------------------------------使用配置---------------------------------#
        self.TABLE_LIST = self.__get_tables()['name'].to_list()  # 所有的表
        self.NICKNAME_LIST = self.__get_gzhs()['nickname'].to_list()  # 所有的绰号列表
        self.GZH_LIST = self.__get_gzhs()['biz'].to_list()  # 所有的公众号列表
        self.MAP_NICK = dict(zip(self.NICKNAME_LIST, self.GZH_LIST))  # 映射绰号到公众号
        self.SHAREINDEX_TABLES = ['000001.SH', '399001.SZ', '000011.SH', '399300.SZ']  # 所有的股票指数

    def __get_gzhs(self) -> pd.DataFrame: return pd.read_sql("SELECT biz,nickname FROM gzhs", self.ENGINE)

    def __get_tables(self) -> pd.DataFrame: return pd.read_sql("select name from sqlite_master WHERE type ='table'",
                                                               self.ENGINE)

    def get_index_members(self, ) -> list:
        return pd.read_sql("SELECT DISTINCT stockcode FROM ASHARE_MV ", self.ENGINE)['stockcode'].to_list()

    def get_index_weight(self, index): return pd.read_sql(f"SELECT trade_date,con_code,weight FROM '{index}_weight' ",
                                                          self.ENGINE)

    def get_code_daily(self, code) -> pd.DataFrame: return pd.read_sql(
        f"SELECT ts_code,trade_date,pct_chg,vol FROM '{code}' ",
        self.ENGINE)

    def get_shibor(self) -> pd.DataFrame: return pd.read_sql('SELECT * FROM shibor', self.ENGINE)

    def update_by_temp(self, df_temp: pd.DataFrame, update_table, update_column, update_pk='id'):
        """
        生成中间表来更新\n
        :param df_temp:
        :param update_table:
        :param update_column:
        :param update_pk:
        :return:
        """
        update_table_temp = update_table + '_temp'
        df_temp.to_sql(update_table_temp, self.ENGINE, index=False, if_exists='replace')
        sql = f"""
                  UPDATE {update_table} AS tar 
                  SET {update_column} = (SELECT temp.{update_column} FROM {update_table_temp} AS temp WHERE temp.{update_pk} = tar.{update_pk})
                  WHERE EXISTS(SELECT {update_pk},{update_column} FROM {update_table_temp} AS temp WHERE temp.{update_pk} = tar.{update_pk})
                  """
        self.ENGINE.execute(sql)
