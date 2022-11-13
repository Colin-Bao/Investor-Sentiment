import pandas as pd


class BaseInfo:
    """
    基础类\n
    """

    def __init__(self):
        from sqlalchemy import create_engine
        self.DOWNLOAD_PATH_ROOT = f'/Users/mac/Downloads/load_img/'
        self.article_table = 'articles_copy1'
        self.engine = create_engine('sqlite:////Users/mac/PycharmProjects/wcplusPro7.31/db_folder/data-dev.db',
                                    echo=False, connect_args={'check_same_thread': False})

    def get_gzhs(self) -> pd.DataFrame:
        return pd.read_sql("SELECT biz,nickname FROM gzhs ", con=self.engine)


class DownLoader(BaseInfo):
    """
    下载器
    """

    def __init__(self):
        super(DownLoader, self).__init__()

    def down_cover_by_gzh(self, biz: str):
        """
        按照公众号名称,分片下载\n
        :param biz:
        :return:
        """

        def extract() -> pd.DataFrame:
            """
            提取/n
            :return:
            """
            df_select = pd.read_sql(
                f"SELECT id,cover,cover_local FROM {self.article_table} "
                "WHERE biz=:biz AND mov=:mov AND p_date BETWEEN :sd AND :ed AND cover_local IS NULL",
                con=self.engine, params={'biz': biz,
                                         'sd': int(pd.to_datetime('20150101').timestamp()),
                                         'ed': int(pd.to_datetime('20210101').timestamp()),
                                         'mov': 10},
                parse_dates=["p_date"], )
            return df_select

        def down(df_extract) -> pd.DataFrame:
            """
            下载图片\n
            :param df_extract:
            :return:
            """
            import requests
            import os

            load_path = self.DOWNLOAD_PATH_ROOT + f'{biz}/'
            os.makedirs(load_path, exist_ok=True)

            # 下载图片
            def down_url(x):
                with open(load_path + f"{x['id']}.jpeg", 'wb') as f:
                    if f.write(requests.get(x['cover'], stream=True).content):
                        return load_path + f"{x['id']}.jpeg"
                return None

            import dask.dataframe as dd
            from dask.diagnostics import ProgressBar
            with ProgressBar():
                df_extract['cover_local'] = (dd.from_pandas(df_extract, npartitions=5)
                                             .map_partitions(lambda df: df.apply(lambda row: down_url(row), axis=1),
                                                             meta=df_extract.dtypes).compute())
            del df_extract['cover']
            return df_extract

        # 更新下载完的图片
        self.update_by_temp(down(extract()), self.article_table, 'cover_local', 'id')

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
        df_temp.to_sql(update_table_temp, self.engine, index=False, if_exists='replace')
        sql = f"""
                UPDATE {update_table} AS tar 
                SET {update_column} = (SELECT temp.{update_column} FROM {update_table_temp} AS temp WHERE temp.{update_pk} = tar.{update_pk})
                WHERE EXISTS(SELECT {update_pk},{update_column} FROM {update_table_temp} AS temp WHERE temp.{update_pk} = tar.{update_pk})
                """
        self.engine.execute(sql)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.engine.dispose()


if __name__ == '__main__':
    with DownLoader() as DownLoader:
        for gzh in DownLoader.get_gzhs()['biz'].to_list():
            DownLoader.down_cover_by_gzh(gzh)  # 中国证券报 财新  央视财经
