#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :bert_text_classifier.py
# @Time      :2022/11/23 05:58
# @Author    :Colin
# @Note      :None
from classifier.cnn_img_classifier import ImgClassifier


class TextClassifier(ImgClassifier):
    def __init__(self):
        super(TextClassifier, self).__init__()


if __name__ == "__main__":
    with TextClassifier() as TextClassifier:
        pass
        # TextClassifier.save_sql()
