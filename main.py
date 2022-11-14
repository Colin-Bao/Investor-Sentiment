def img_loader():
    from loader.img_loader import DownLoader
    with DownLoader() as DownLoader:
        for gzh in DownLoader.GZH_LIST:
            DownLoader.down_cover_by_gzh(gzh)  # 中国证券报 财新  央视财经


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
    img_loader()
    img_classifier()
