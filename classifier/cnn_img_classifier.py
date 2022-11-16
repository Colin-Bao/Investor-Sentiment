import pandas as pd
from configer.base_config import Base


# 熵选法
class ImgClassifier(Base):

    def __init__(self):
        super(ImgClassifier, self).__init__()
        self.MODEL_PATH = '/Users/mac/PycharmProjects/Google-V3/img_predict/twitter_tl_500.h5'
        self.TESTSET_PATH = '/Users/mac/Downloads/load_img/testset/'
        self.TESTSET_TAG_PATH = '/Users/mac/Downloads/load_img/testset_tag.csv'
        self.BATCH_SIZE = 512

    def extract_imgs_by_gzh(self, biz):
        """
        从数据库获取待预测的图片\n
        :return:
        """

        def extract():
            df_select = pd.read_sql(
                f"SELECT id,cover_local FROM {self.ARTICLE_TABLE} "
                "WHERE biz=:biz AND mov=:mov AND p_date BETWEEN :sd AND :ed "
                "AND cover_local IS NOT NULL AND cover_neg IS NULL LIMIT :limit_size",
                con=self.ENGINE, params={'biz': biz,
                                         'sd': int(pd.to_datetime(self.START_DATE).timestamp()),
                                         'ed': int(pd.to_datetime(self.END_DATE).timestamp()),
                                         'mov': 10,
                                         'limit_size': self.BATCH_SIZE}, )
            return df_select

        return extract()

    def extract_imgs_by_testset(self):
        """
        在测试集中生成图片
        :return:
        """
        import os
        df_select = pd.DataFrame(os.listdir(self.TESTSET_PATH)).rename(columns={0: 'cover_local'})
        df_select['id'] = df_select['cover_local'].str[:-5]
        df_select['cover_local'] = self.TESTSET_PATH + df_select['cover_local']
        return df_select[['id', 'cover_local']]

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
                    np_img = img_to_array(images)
                    np_img = np.expand_dims(np_img, axis=0)
                    np_img = preprocess_input(np_img)
                    img_path_list.append(np_img)
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
        return pd.concat([df_query[['id']], df_pred[[0]]], axis=1).rename(columns={0: 'cover_neg'})

    def update_pred(self, df_pred):
        """
        更新
        :param df_pred:
        :return:
        """
        self.update_by_temp(df_pred, self.ARTICLE_TABLE, 'cover_neg', 'id')

    def calculate_metrics(self):
        """
        比较与真实数据的准确性
        :return:
        """
        import numpy as np

        def extract_pred_testset() -> pd.DataFrame:
            if 'testset' not in self.TABLE_LIST:
                self.save_sql(self.predict_imgs(self.extract_imgs_by_testset()), 'testset')
            df_pred = pd.read_sql('SELECT id,cover_neg FROM testset', self.ENGINE).rename(
                columns={'cover_neg': 'pred'})
            df_pred['pred'] = np.where(df_pred['pred'] >= 0.5, 'negative', 'positive')
            return df_pred

        def extract_real_tag() -> pd.DataFrame:
            df_real = pd.read_csv(self.TESTSET_TAG_PATH, usecols=['image', 'choice']).rename(
                columns={'choice': 'real'})
            df_real['real'] = np.where(df_real['real'] == 'negative', 'negative', 'positive')
            # 提取id
            df_id = df_real['image'].str.rsplit("/", expand=True, n=1)[1].str[:-5].rename('id')
            return pd.concat([df_id, df_real['real']], axis=1)

        def metrics(df_compare):
            from sklearn.metrics import confusion_matrix, accuracy_score, recall_score, precision_score, f1_score
            y_true, y_pred = df_compare['real'], df_compare['pred']
            CM, AS, RS, PS, FS = (confusion_matrix(y_true, y_pred, labels=["positive", "negative"]),
                                  accuracy_score(y_true, y_pred),
                                  recall_score(y_true, y_pred, average='macro'),
                                  precision_score(y_true, y_pred, average='macro'),
                                  f1_score(y_true, y_pred, average='macro'))

            return CM, AS, RS, PS, FS

        res = dict(zip(('confusion_matrix', 'accuracy_score', 'recall_score', 'precision_score', 'f1_score'),
                       metrics(pd.merge(extract_real_tag(), extract_pred_testset(), on='id', how='inner'))))
        print(res)

        return res


ImgClassifier().calculate_metrics()
