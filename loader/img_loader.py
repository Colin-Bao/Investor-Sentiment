import pandas as pd
from utils.sql import Base
from utils.tools import MultiExecutor


class DownLoader(Base):
    """
    下载器,下载图片到本地
    """

    def __init__(self):
        super(DownLoader, self).__init__()
        self.IMG_PATH_ROOT = '/data/DataSets/WC_IMGS/'  # 文件存储的路径

    def load_cover_multi(self):
        """
        根据biz下载图片
        :return:
        """
        import requests
        import os

        # 单个任务
        def down_task(i):
            """
            下载图片到本地
            """

            # 创建目录
            os.makedirs(self.IMG_PATH_ROOT + i[1], exist_ok=True)

            # 下载图片
            img_path = self.IMG_PATH_ROOT + f"{i[1]}/{i[2]}.jpeg"
            with open(img_path, 'wb') as f:
                try:
                    # 下载
                    f.write(requests.get(i[0], stream=True).content)
                    # 更新状态
                    self.ENGINE.execute(f"UPDATE NEW_WECHAT_DATA.articles SET cover_local='{img_path}' WHERE id ='{i[2]}' ")
                # 错误
                except Exception as e: return e.args, f'id {i[2]} failed'

        # 遍历所有的数据行
        for chunk in pd.read_sql('SELECT cover,biz,id FROM NEW_WECHAT_DATA.articles WHERE cover_local IS NULL', self.ENGINE,
                                 chunksize=10000):
            # 多线程下载器
            # 开始任务
            MultiExecutor().start_multi_task(down_task, list(zip(chunk['cover'].to_list(), chunk['biz'].to_list(), chunk['id'].to_list())))

    def load_cover_by_gzh(self, biz: str):
        """
        按照公众号名称,分片下载\n
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
                                             'sd' : int(pd.to_datetime(self.START_DATE).timestamp()),
                                             'ed' : int(pd.to_datetime(self.END_DATE).timestamp()),
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


class ImgGenerator(Base):
    """
    图片生成,用于生成数据验证分类器效果
    """

    def __init__(self, NICKNAME_LIST):
        super(ImgGenerator, self).__init__()
        self.NICKNAME_LIST = NICKNAME_LIST

    def get_test_set(self):
        def extract(nickname):
            df_select = pd.read_sql(
                    f"SELECT cover_local FROM {self.ARTICLE_TABLE} "
                    "WHERE biz=:biz AND mov=:mov AND p_date BETWEEN :sd AND :ed AND cover_local IS NOT NULL "
                    "ORDER BY random() LIMIT 100",
                    con=self.ENGINE, params={'biz': self.MAP_NICK[nickname],
                                             'sd' : int(pd.to_datetime(self.START_DATE).timestamp()),
                                             'ed' : int(pd.to_datetime(self.END_DATE).timestamp()),
                                             'mov': 10},
            )
            return df_select

        def gen_test_set() -> None:
            # 随机取样
            df_test_set = pd.DataFrame()
            for i in [extract(i) for i in self.NICKNAME_LIST]:
                df_test_set = pd.concat([df_test_set, i])
            df_test_set = df_test_set.sample(n=100)

            # 下载到本地
            import os
            copy_path = '/Users/mac/Downloads/load_img/testset/'

            def copy(x):
                f_path = copy_path + x['cover_local'].split('/')[-1]
                os.system(f"cp {x['cover_local']} {f_path}")

            df_test_set.apply(lambda x: copy(x), axis=1)

        gen_test_set()


# s = ImgGenerator(['中国证券报', '财新网', '央视财经', '界面新闻'])
DownLoader().load_cover_multi()
