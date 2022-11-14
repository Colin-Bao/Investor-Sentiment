import pandas as pd
from configer.base_config import Base


class DownLoader(Base):
    """
    下载器
    """

    def __init__(self):
        super(DownLoader, self).__init__()
        # -------------------------------文件存储---------------------------------#
        self.IMG_PATH_ROOT = f'/Users/mac/Downloads/load_img/'  # 文件存储的路径

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
                f"SELECT id,cover,cover_local FROM {self.ARTICLE_TABLE} "
                "WHERE biz=:biz AND mov=:mov AND p_date BETWEEN :sd AND :ed AND cover_local IS NULL",
                con=self.ENGINE, params={'biz': biz,
                                         'sd': self.START_TIME,
                                         'ed': self.END_TIME,
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

            load_path = self.IMG_PATH_ROOT + f'{biz}/'
            os.makedirs(load_path, exist_ok=True)

            # 下载图片
            def down_url(x):
                with open(load_path + f"{x['id']}.jpeg", 'wb') as f:
                    try:
                        if f.write(requests.get(x['cover'], stream=True).content):
                            return load_path + f"{x['id']}.jpeg"
                        else:
                            return None
                    except Exception as e:
                        # print(e.args)
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
        self.update_by_temp(down(extract()), self.ARTICLE_TABLE, 'cover_local', 'id')


if __name__ == '__main__':
    with DownLoader() as DownLoader:
        for gzh in DownLoader.GZH_LIST:
            DownLoader.down_cover_by_gzh(gzh)  # 中国证券报 财新  央视财经
