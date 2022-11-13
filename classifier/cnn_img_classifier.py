import pandas as pd
from configer.base_config import Base

# 胡老师,这周就是读了一些指标体系的文章,数据上做了一些拓展.
# 主要是收集了了能够代表机构投资者的情绪的,分析师一致预期数据.
# 这是一种主观调查的情绪指标,就是分析师对股票指数的价格预期,看看能不能和媒体的情绪结合起来.
# 然后之前收集的非结构化的数据噪音很大,数据处理上也想了一些办法.
# 根据之前田老师的意见,用深度学习的方法建立指标,最重要的是证明提取出来的信息是可靠的,如果没有好的办法的话,最差的可能就需要人工对非结构化的数据打标签.
# 下周应该把实证结合之前的重新完善一下,然后再一起展示情绪指标体系建立的章节..
class ImgClassifier(Base):

    def __init__(self):
        super(ImgClassifier, self).__init__()
        self.MODEL_PATH = '/Users/mac/PycharmProjects/Google-V3/img_predict/twitter_tl_500.h5'

    def get_imgs_by_gzh(self, biz, limit_size: int = 500):
        """
        从数据库获取待预测的图片\n
        :return:
        """
        df_select = pd.read_sql(
            f"SELECT id,cover_local FROM {self.ARTICLE_TABLE} "
            "WHERE biz=:biz AND mov=:mov AND p_date BETWEEN :sd AND :ed "
            "AND cover_local IS NOT NULL AND cover_neg IS NULL LIMIT :limit_size",
            con=self.ENGINE, params={'biz': biz,
                                     'sd': int(pd.to_datetime('20200101').timestamp()),
                                     'ed': int(pd.to_datetime('20210101').timestamp()),
                                     'mov': 10,
                                     'limit_size': limit_size}, )
        return df_select

    def predict_imgs(self, df_query: pd.DataFrame):
        """
        图片路径读取成可以预测的格式,第2列是img路径\n
        :param df_query:
        :return:
        """

        def transform_imgs(df_img):
            """
            把图片路径转为RGB数组\n
            :param df_img:
            :return:
            """
            from keras.applications.inception_v3 import preprocess_input
            from keras.utils import load_img, img_to_array
            import PIL
            import numpy as np

            img_path_list = []
            for i in range(len(df_img)):
                try:
                    images = load_img(df_img[i], target_size=(299, 299))
                    img_arrary = img_to_array(images)
                    img_arrary = np.expand_dims(img_arrary, axis=0)
                    img_arrary = preprocess_input(img_arrary)
                    img_path_list.append(img_arrary)
                except (FileNotFoundError, OSError, PIL.UnidentifiedImageError) as e:
                    continue
            # 把图片数组联合在一起
            return np.concatenate([i for i in img_path_list])

        def classify_img_bymodel(np_img) -> pd.DataFrame:
            """
            分类\n
            :param np_img:
            :return:
            """
            import os
            from keras.models import load_model
            os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
            try:
                model = load_model(self.MODEL_PATH)
                y_pred = pd.DataFrame(model.predict(np_img))
                return y_pred
            except OSError as e:
                return pd.DataFrame()

        # 预测
        df_pred = classify_img_bymodel(transform_imgs(df_query['cover_local']))

        # 预测结果与原表拼在一起
        df_res = pd.concat([df_query[['id']], df_pred[[0]]], axis=1).rename(columns={0: 'cover_neg'})
        self.update_by_temp(df_res, self.ARTICLE_TABLE, 'cover_neg', 'id')


if __name__ == "__main__":
    with ImgClassifier() as ImgClassifier:
        for gzh in ImgClassifier.get_gzhs()['biz'].to_list():
            while True:
                imgs = ImgClassifier.get_imgs_by_gzh(gzh, 512)
                if imgs.empty:
                    break
                ImgClassifier.predict_imgs(imgs)
