# 合并面板数据
CREATE TABLE IF NOT EXISTS TEMP_MERGE_PANEL AS
    (SELECT ASHARE_DAILY_PANEL.ts_code,
            ASHARE_DAILY_PANEL.trade_date,
            ASHARE_DAILY_PANEL.pct_chg,
            ASHARE_BASIC_PANEL.total_mv

     FROM FIN_PANEL_DATA.ASHARE_DAILY_PANEL
              LEFT JOIN
          FIN_PANEL_DATA.ASHARE_BASIC_PANEL
          ON (ASHARE_DAILY_PANEL.ts_code = ASHARE_BASIC_PANEL.ts_code AND
              ASHARE_DAILY_PANEL.trade_date = ASHARE_BASIC_PANEL.trade_date)

     WHERE ASHARE_DAILY_PANEL.trade_date >= 20140101);

# 合并时间序列数据
CREATE TABLE IF NOT EXISTS TEMP_MERGE_INDEX AS
    (SELECT `399300.SZ`.trade_date,
            `399300.SZ`.pct_chg AS index_return,
            IMG_SENT.neg_index  AS img_neg,
            TEX_SENT.neg_index  AS tex_neg,
            SHIBOR.1m           AS riskfree_return
     FROM FIN_DAILY_INDEX.`399300.SZ`
              LEFT JOIN
          FIN_DAILY_INDEX.IMG_SENT
          ON
              `399300.SZ`.trade_date = IMG_SENT.trade_date
              LEFT JOIN
          FIN_DAILY_INDEX.SHIBOR
          ON
              `399300.SZ`.trade_date = SHIBOR.trade_date
              LEFT JOIN
          FIN_DAILY_INDEX.TEX_SENT
          ON
              `399300.SZ`.trade_date = TEX_SENT.trade_date
     WHERE `399300.SZ`.trade_date >= 20140101);

# 合并最终的数据
CREATE TABLE IF NOT EXISTS COLIN_PANEL.TEMP_PANEL_FINAL AS
    (SELECT FIN_PANEL_DATA.TEMP_MERGE_PANEL.ts_code,
            FIN_PANEL_DATA.TEMP_MERGE_PANEL.trade_date,
            FIN_PANEL_DATA.TEMP_MERGE_PANEL.pct_chg,
            FIN_PANEL_DATA.TEMP_MERGE_PANEL.total_mv,
            FIN_DAILY_INDEX.TEMP_MERGE_INDEX.riskfree_return,
            FIN_DAILY_INDEX.TEMP_MERGE_INDEX.index_return,
            FIN_DAILY_INDEX.TEMP_MERGE_INDEX.img_neg,
            FIN_DAILY_INDEX.TEMP_MERGE_INDEX.tex_neg
     FROM FIN_PANEL_DATA.TEMP_MERGE_PANEL
              LEFT JOIN
          FIN_DAILY_INDEX.TEMP_MERGE_INDEX
          ON
                  FIN_PANEL_DATA.TEMP_MERGE_PANEL.trade_date =
                  FIN_DAILY_INDEX.TEMP_MERGE_INDEX.trade_date);