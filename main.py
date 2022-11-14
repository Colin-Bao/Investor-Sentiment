def findata_loader():
    from loader.findata_loader import DownLoader
    with DownLoader() as DownLoader:
        DownLoader.load_index()


def img_loader():
    from loader.img_loader import DownLoader
    with DownLoader() as DownLoader:
        for gzh in DownLoader.GZH_LIST:
            DownLoader.load_cover_by_gzh(gzh)


def imgsent_analyzer():
    from analyzer.img_analyzer import SentCalculator
    with SentCalculator() as Calculator:
        Calculator.map_trade_date()
        Calculator.cal_sentiment_index()


def img_classifier():
    from classifier.cnn_img_classifier import ImgClassifier
    with ImgClassifier() as ImgClassifier:
        for gzh in ImgClassifier.GZH_LIST:
            while True:
                imgs = ImgClassifier.get_imgs_by_gzh(gzh)
                if imgs.empty:
                    break
                ImgClassifier.predict_imgs(imgs)


if __name__ == '__main__':
    findata_loader()
    img_loader()
    img_classifier()
    imgsent_analyzer()
