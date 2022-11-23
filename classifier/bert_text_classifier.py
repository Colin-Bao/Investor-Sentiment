#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :bert_text_classifier.py
# @Time      :2022/11/23 05:58
# @Author    :Colin
# @Note      :None
from typing import Union

from pandas import DataFrame, Series

from classifier.cnn_img_classifier import ImgClassifier
import pandas as pd


class TextClassifier(ImgClassifier):
    def __init__(self):
        super(TextClassifier, self).__init__()
        self.MODEL_PATH = '/Users/mac/PycharmProjects/pythonProject/saved/FinBERT/checkpoint-1000'
        self.HYPER_PARAS = {'MAX_LENGTH': 32, 'BATCH_SIZE': 512}

    def extract_apply_dataset(self):
        """
        取出实际应用场景中的数据集
        """

        def extract():
            df_extract = pd.read_sql(
                f"SELECT id,title FROM {self.ARTICLE_TABLE} "
                "WHERE mov=:mov AND title IS NOT NULL AND title_neg IS NULL LIMIT :limit_size ",
                con=self.ENGINE, params={'mov': 10, 'limit_size': self.HYPER_PARAS['BATCH_SIZE']}, )
            return df_extract

        return extract()

    # 执行预测
    def predict_from_bertmodel(self, input_list: list) -> pd.DataFrame:
        # 获取预训练的模型
        from transformers import BertTokenizer, BertForSequenceClassification
        tokenizer = BertTokenizer.from_pretrained(self.MODEL_PATH)
        model = BertForSequenceClassification.from_pretrained(self.MODEL_PATH, num_labels=3)

        # 首先还是用分词器进行预处理
        encoded = tokenizer(input_list, truncation=True, padding='max_length', max_length=32, return_tensors='pt')
        out = model(**encoded)
        probs = out.logits.softmax(dim=-1)

        # 返回预测 {0: 'positove', 1: 'neutral', 2: 'negative'}
        return pd.DataFrame(probs.detach().numpy()[:, 2]).rename(columns={0: 'title_neg'})

    def predict_texts_batch(self, ):
        """
        预测和更新
        :return:
        """
        from tqdm import tqdm
        null_row = self.get_count_null('title_neg', self.ARTICLE_TABLE)
        if null_row.shape[0] == 1:
            return
        pbar = tqdm(range(null_row['NUM'][1]))
        pbar.update(null_row['NUM'][0])

        while True:
            df_input = self.extract_apply_dataset()
            if df_input.empty:
                break
            df_pred = self.predict_from_bertmodel(df_input['title'].to_list())
            self.update_by_temp(pd.concat([df_input, df_pred], axis=1), self.ARTICLE_TABLE, 'title_neg', 'id')

            pbar.update(self.HYPER_PARAS['BATCH_SIZE'])
            pbar.refresh()


if __name__ == "__main__":
    with TextClassifier() as TextClassifier:
        TextClassifier.predict_texts_batch()
        # print(df.shape)
# res = TextClassifier.predict_from_model(['不好的重大交通事故', '迎来了重大利好,股市一片欣欣向荣'])
#         print(res)
