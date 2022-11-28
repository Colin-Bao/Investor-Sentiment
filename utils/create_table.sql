-- 1.create gzh table
CREATE TABLE IF NOT EXISTS WECHAT_GZH.gzhs
(
    biz                      VARCHAR(20) NOT NULL,
    nickname                 VARCHAR(20) NOT NULL,
    created_time             INTEGER     NOT NULL,
    updated_time             INTEGER     NOT NULL,
    total_article_num        INTEGER,
    main_article_num         INTEGER,
    reading_data_article_num INTEGER,
    comment_article_num      INTEGER,
    saved_article_num        INTEGER,
    PRIMARY KEY (biz)
);

CREATE INDEX ix_gzhs_biz ON WECHAT_GZH.gzhs (biz);

-- 2.create articles table
CREATE TABLE IF NOT EXISTS WECHAT_GZH.articles
(
    id              VARCHAR(32) NOT NULL,
    content_url     TEXT        NOT NULL,
    title           VARCHAR(300),
    source_url      TEXT,
    digest          TEXT,
    cover           TEXT,
    cover_local     TEXT,
    cover_neg       REAL,
    title_neg       REAL,
    p_date          INTEGER,
    t_date          DATETIME,
    read_num        INTEGER,
    like_num        INTEGER,
    old_like_num    INTEGER,
    comment_id      VARCHAR(30),
    post_location   VARCHAR(20),
    comment_num     INTEGER,
    reward_num      INTEGER,
    author          TEXT,
    copyright_stat  INTEGER,
    mov             INTEGER,
    local_article   BOOLEAN,
    cat_name        VARCHAR(30),
    add_to_cat_date INTEGER,
    created_time    INTEGER     NOT NULL,
    updated_time    INTEGER     NOT NULL,
    biz             VARCHAR(50) NOT NULL,
    PRIMARY KEY (id)
);


CREATE INDEX ix_articles_biz
    ON WECHAT_GZH.articles (
                            biz ASC
        );



CREATE INDEX ix_articles_id
    ON WECHAT_GZH.articles (
                            id ASC
        );

CREATE INDEX ix_articles_title
    ON WECHAT_GZH.articles (
                            title ASC
        );

CREATE INDEX ix_p_date
    ON WECHAT_GZH.articles (
                            p_date ASC
        );

CREATE INDEX ix_t_date
    ON WECHAT_GZH.articles (
                            t_date ASC
        );
