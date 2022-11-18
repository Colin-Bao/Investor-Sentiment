def findata_loader():
    from loader.findata_loader import DownLoader
    with DownLoader(['000001.SH', '399001.SZ', '000011.SH', '399300.SZ']) as DownLoader:
        DownLoader.load_index()


def img_loader():
    from loader.img_loader import DownLoader
    with DownLoader() as DownLoader:
        for gzh in DownLoader.GZH_LIST:
            DownLoader.load_cover_by_gzh(gzh)


def sent_analyzer():
    from analyzer.sent_analyzer import SentCalculator, RegCalculator
    with SentCalculator('img', 0.55, ['中国证券报', '财新网', '央视财经', '界面新闻']) as Calculator:
        Calculator.map_trade_date()
        Calculator.cal_sentiment_index()
    with RegCalculator([0.01, 0.01]) as RegCalculator:
        RegCalculator.regression('VAR', 10)
        # RegCalculator.regression('LIN', 2)


def img_classifier():
    from classifier.cnn_img_classifier import ImgClassifier
    with ImgClassifier() as ImgClassifier:
        for gzh in ImgClassifier.GZH_LIST:
            while True:
                imgs = ImgClassifier.extract_imgs_by_gzh(gzh)
                if imgs.empty:
                    break
                ImgClassifier.update_pred(ImgClassifier.predict_imgs(imgs))
        # 模型评估
        ImgClassifier.calculate_metrics()


if __name__ == '__main__':
    # findata_loader()
    # img_loader()
    # img_classifier()
    sent_analyzer()
