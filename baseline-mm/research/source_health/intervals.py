"""Exact continuous-time intervals. Policy belongs to baseline-mm."""
from dpt_health.projection import literal

DAY = 86_400_000


def make_selection(db, start, end, trade_enabled, depth_enabled):
    # Only the small market/rule boundary set is materialized here, not raw rows.
    # A valid spot remains eligible through silence and classified RTDS anomalies.
    db.execute(f"""CREATE TABLE market_boundaries AS
        SELECT condition_id,greatest(window_start,known_at,{start}) AS visible_at FROM markets WHERE window_start<{end} AND window_end>{start}
        UNION SELECT condition_id,window_end FROM markets WHERE window_end>{start} AND window_end<{end}
        UNION SELECT r.condition_id,r.visible_at FROM rules r JOIN markets m USING(condition_id)
              WHERE r.visible_at>greatest(m.window_start,m.known_at,{start}) AND r.visible_at<m.window_end AND r.visible_at<{end}
        UNION SELECT m.condition_id,p.visible_at FROM markets m CROSS JOIN
          (SELECT min(visible_at) AS visible_at FROM changes WHERE source='chainlink_spot' AND received IS NOT NULL AND available) p
          WHERE p.visible_at>greatest(m.window_start,m.known_at,{start}) AND p.visible_at<least(m.window_end,{end})""")
    db.execute(f"""CREATE TABLE market_selection AS WITH effective AS (
        SELECT m.condition_id,b.visible_at AS a,least(coalesce(lead(b.visible_at) OVER(PARTITION BY b.condition_id ORDER BY b.visible_at),m.window_end),m.window_end,{end}) AS b,
               r.reference,
               b.visible_at >= (SELECT min(visible_at) FROM changes WHERE source='chainlink_spot' AND received IS NOT NULL AND available) AS spot_present
        FROM market_boundaries b JOIN markets m USING(condition_id)
        ASOF LEFT JOIN (SELECT condition_id,visible_at,reference FROM rules QUALIFY row_number() OVER(PARTITION BY condition_id,visible_at ORDER BY sequence DESC)=1) r
        ON b.condition_id=r.condition_id AND b.visible_at>=r.visible_at
        WHERE b.visible_at>=greatest(m.window_start,m.known_at,{start}) AND b.visible_at<least(m.window_end,{end})
    ) SELECT condition_id,a,b,CASE WHEN spot_present THEN 'chainlink_spot'
           WHEN reference=8 THEN 'chainlink_twap60' WHEN reference=0 THEN NULL ELSE 'chainlink_spot' END AS reference FROM effective WHERE b>a""")
    # Sweep +1/-1 edges and union coverage. Repeated market demand never weights
    # shared Chainlink or Binance sources multiple times.
    db.execute("""CREATE TABLE demand AS SELECT reference AS source,a,b FROM market_selection WHERE reference IS NOT NULL""")
    for source, enabled in (("binance_trade",trade_enabled),("binance_depth",depth_enabled)):
        if enabled:
            db.execute(f"INSERT INTO demand SELECT {literal(source)},a,b FROM market_selection")
    db.execute("""CREATE TABLE selections AS WITH edges AS (
        SELECT source,a AS visible_at,1 AS delta FROM demand UNION ALL SELECT source,b,-1 FROM demand
    ), grouped AS (SELECT source,visible_at,sum(delta) AS delta FROM edges GROUP BY source,visible_at),
    coverage AS (SELECT source,visible_at AS a,lead(visible_at) OVER w AS b,sum(delta) OVER( PARTITION BY source ORDER BY visible_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS active FROM grouped WINDOW w AS (PARTITION BY source ORDER BY visible_at))
    SELECT source,a,b FROM coverage WHERE active>0 AND b>a""")


def daily_intervals(db, table, target):
    db.execute(f"""CREATE TEMP TABLE {target} AS SELECT i.*,d.day_start,
        greatest(a,d.day_start) AS da,least(b,d.day_start+{DAY}) AS db
        FROM {table} i, LATERAL range((a//{DAY})*{DAY},b,{DAY}) d(day_start) WHERE b>a""")


def candidate_intervals(db, candidate, table, target):
    # Policy thresholds are complete group configurations. Normal and max may be
    # equal or zero. All boundaries are exact integers and strictly inside spans.
    db.execute("CREATE TEMP TABLE limits(source VARCHAR,normal_s BIGINT,normal_a BIGINT,max_s BIGINT,max_a BIGINT)")
    limits = []
    for source in ("polymarket","chainlink_spot","chainlink_twap60","binance_trade","binance_depth"):
        group="chainlink" if source.startswith("chainlink_") else source
        p=candidate[group]
        limits.append((source,p["normal_s"],p["normal_a"],p["max_s"],p["max_a"]))
    db.executemany("INSERT INTO limits VALUES (?,?,?,?,?)",limits)
    db.execute(f"""CREATE TEMP TABLE {target} AS WITH joined AS (
        SELECT i.*,l.* EXCLUDE(source) FROM {table} i JOIN limits l USING(source)
    ), split AS (
        SELECT j.*,u.visible_at AS x FROM joined j,
          LATERAL unnest(list_distinct([a,b,
            greatest(a,least(b,received+normal_s)),greatest(a,least(b,event+normal_a)),
            greatest(a,least(b,received+max_s)),greatest(a,least(b,event+max_a))])) u(visible_at)
    ), labelled AS (
        SELECT source,condition_id,asset,x AS a,lead(x) OVER(PARTITION BY source,condition_id,asset,a ORDER BY x) AS b,
            received,event,original_at,sync_unknown,generation_unknown,
            CASE WHEN exclusion IS NOT NULL THEN exclusion
                 WHEN x-received>=max_s OR x-event>=max_a THEN 'exceeded'
                 WHEN x-received>=normal_s OR x-event>=normal_a THEN 'elevated' ELSE 'normal' END AS state
        FROM split
    ) SELECT * FROM labelled WHERE b>a""")
    db.execute("DROP TABLE limits")


def episodes(db, table, target, selection_scopes=False):
    # A one-ms causal probe must not bridge a real one-ms deselection gap.
    partition=',evaluation_start' if selection_scopes else ''
    # Positive-time continuity: same-ms temporary selections never split runs.
    db.execute(f"""CREATE TEMP TABLE {target} AS WITH prior AS (
        SELECT *,lag(state) OVER w AS previous_state,lag(b) OVER w AS previous_end
        FROM {table} WINDOW w AS (PARTITION BY source,condition_id,asset{partition} ORDER BY a)
    ), numbered AS (
        SELECT *,sum(CASE WHEN state IS DISTINCT FROM previous_state OR a!=previous_end THEN 1 ELSE 0 END)
              OVER(PARTITION BY source,condition_id,asset{partition} ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS episode
        FROM prior
    ) SELECT source,condition_id,asset{partition},state,episode,min(a) AS a,max(b) AS b,
        min_by(received,a) AS received,min_by(event,a) AS event,
        min_by(previous_state,a) AS previous_state,min_by(previous_end,a) AS previous_end,
        min_by(original_at,a) AS original_at FROM numbered GROUP BY source,condition_id,asset{partition},state,episode""")
