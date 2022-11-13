import pandas as pd


class Base:
    """
    基础类\n
    """

    def __init__(self):
        from sqlalchemy import create_engine
        self.DOWNLOAD_PATH_ROOT = f'/Users/mac/Downloads/load_img/'
        self.ARTICLE_TABLE = 'articles_copy1'
        self.ENGINE = create_engine('sqlite:////Users/mac/PycharmProjects/wcplusPro7.31/db_folder/data-dev.db',
                                    echo=False, connect_args={'check_same_thread': False})

    def get_gzhs(self) -> pd.DataFrame:
        return pd.read_sql("SELECT biz,nickname FROM gzhs ", con=self.ENGINE)

    def update_by_temp(self, df_temp: pd.DataFrame, update_table, update_column, update_pk):
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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.ENGINE.dispose()
