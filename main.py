def findata_loader():
    from loader.findata_loader import DownLoader, FinDerCalulator
    with DownLoader(['000001.SH', '399001.SZ', '000011.SH', '399300.SZ']) as DownLoader:
        # DownLoader.load_index()
        # DownLoader.load_index_members()
        DownLoader.load_shibor()
    with FinDerCalulator(5, 30, 0.6, '399300.SZ') as Calulator:
        # Calulator.cal_idvol('CAPM')
        Calulator.cal_high_low()


def img_loader():
    from loader.img_loader import DownLoader
    with DownLoader() as DownLoader:
        for gzh in DownLoader.GZH_LIST:
            DownLoader.load_cover_by_gzh(gzh)


def sent_analyzer():
    from analyzer.sent_analyzer import SentCalculator, RegCalculator
    # with SentCalculator('img', 0.55, ['中国证券报', '财新网', '央视财经', '界面新闻']) as Calculator:
    #     # Calculator.map_trade_date()
    #     Calculator.cal_sentiment_index()
    with SentCalculator('img', 0.55, ['中国证券报', '财新网', '央视财经', '界面新闻']) as imgCalculator:
        imgCalculator.cal_sentiment_index()
    with RegCalculator([0.01, 0.01], 'img') as imgRegCalculator:
        imgRegCalculator.regression('VAR', 5)
    #
    # with SentCalculator('text', 0.55, ['中国证券报', '财新网', '央视财经', '界面新闻']) as Calculator:
    #     Calculator.cal_sentiment_index()
    # with RegCalculator([0.01, 0.01], 'text') as RegCalculator:
    #     RegCalculator.regression('VAR', 5)


def img_classifier():
    from classifier.cnn_img_classifier import ImgClassifier
    with ImgClassifier() as ImgClassifier:
        # for gzh in ImgClassifier.GZH_LIST:
        #     while True:
        #         imgs = ImgClassifier.extract_imgs_by_gzh(gzh)
        #         if imgs.empty:
        #             break
        #         ImgClassifier.update_pred(ImgClassifier.predict_imgs(imgs))
        # 模型评估
        ImgClassifier.calculate_metrics()


def test():
    import pandas as pd
    from loader.img_loader import DownLoader
    df = pd.read_parquet('/Users/mac/Downloads/suntime_dataset/con_forecast_idx.parquet')
    with DownLoader() as DownLoader:
        DownLoader.save_sql(df, 'con_forecast_idx')


if __name__ == '__main__':
    # findata_loader()
    # img_loader()
    # img_classifier()
    sent_analyzer()
    # test()
