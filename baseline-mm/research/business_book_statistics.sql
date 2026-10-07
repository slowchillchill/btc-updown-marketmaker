-- 仅完整 book 事件的分布；不是时间加权盘口，也不是本账户成交概率。
-- unit_books 已复用公共 admission 去重；clock 包含同单元全部 PM 行的可见前沿。
CREATE TEMP TABLE book_statistics AS
WITH mapped AS (
    SELECT b.*,m.start_at,m.end_at,m.assets,
        list_transform(b.bids,x->struct_pack(price:=CAST(x.price AS DOUBLE),size:=CAST(x.size AS DOUBLE))) AS bid_levels,
        list_transform(b.asks,x->struct_pack(price:=CAST(x.price AS DOUBLE),size:=CAST(x.size AS DOUBLE))) AS ask_levels
    FROM unit_books b LEFT JOIN business_markets m ON b.market=m.condition_id
    WHERE clock >= $start AND clock < $end
), top AS (
    SELECT *,
        list_max(list_transform(list_filter(bid_levels,x->x.size>0),x->x.price)) AS best_bid,
        list_min(list_transform(list_filter(ask_levels,x->x.size>0),x->x.price)) AS best_ask,
        CASE WHEN start_at IS NULL THEN 'unmapped_market'
             WHEN NOT list_contains(assets,asset_id) THEN 'unmapped_asset'
             WHEN clock < start_at OR clock >= end_at THEN 'outside_market_window'
             ELSE 'in_window' END AS scope
    FROM mapped
)
SELECT clock // 86400000 AS day,scope,
    CASE WHEN scope='in_window' AND best_bid IS NOT NULL AND best_ask IS NOT NULL
         AND best_ask>=best_bid THEN best_ask-best_bid END AS spread,
    CASE WHEN scope='in_window' THEN
        list_sum(list_transform(list_filter(ask_levels,x->x.price=best_ask AND x.size>0),x->x.size))
    END AS best_ask_shares,
    scope='in_window' AND best_bid IS NULL AS missing_bid,
    scope='in_window' AND best_ask IS NULL AS missing_ask,
    scope='in_window' AND best_bid>best_ask AS crossed
FROM top;
