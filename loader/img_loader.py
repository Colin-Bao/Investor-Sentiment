import pandas as pd
from utils.sql import Base


class DownLoader(Base):
    """
    下载器,下载图片到本地
    """

    def __init__(self):
        super(DownLoader, self).__init__()
        self.IMG_PATH_ROOT = '/data/DataSets/WC_IMGS/'  # 文件存储的路径

    def load_cover_multi(self):
        """
        根据biz下载图片,多线程
        """
        import requests
        import os
        from utils.tools import MultiExecutor

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


class ImgGenerator(Base):
    """
    图片生成,用于生成数据验证分类器效果
    """

    def __init__(self):
        super(ImgGenerator, self).__init__()
        self.IMG_PATH_ROOT = '/data/DataSets/label_studio_ds/'  # 图片生成的路径

    def gen_img_dataset(self):
        """
        生成图片数据集用于训练,把图片数据集生成到Label Studio
        """
        import os
        import shutil
        from tqdm import tqdm
        for biz in self.BIZ_LIST:
            # 创建目录
            os.makedirs(self.IMG_PATH_ROOT + biz, exist_ok=True)

            # 复制图片
            for path in tqdm(pd.read_sql('SELECT cover_local FROM NEW_WECHAT_DATA.articles '
                                         'WHERE biz=%(biz)s AND cover_local IS NOT NULL AND mov=10 ORDER BY rand(520) LIMIT 100',
                                         self.ENGINE, params={'biz': biz})['cover_local'].to_list()):
                shutil.copy(path, self.IMG_PATH_ROOT + biz)

# s = ImgGenerator(['中国证券报', '财新网', '央视财经', '界面新闻'])
# DownLoader().load_cover_multi()
# ImgGenerator().gen_img_dataset()
