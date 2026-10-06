"""Duration-weighted distributions and forward-only candidate evaluation."""
from collections import defaultdict
from .intervals import DAY, candidate_intervals, daily_intervals, episodes

LAST_LINEAR = 600 + (DAY - 60_000) // 1000
GROUP = "CASE WHEN source LIKE 'chainlink_%' THEN 'chainlink' ELSE source END"


def histogram_macros(db):
    db.execute(f"""CREATE MACRO age_bin(x) AS CASE WHEN x<60000 THEN x//100
        WHEN x<{DAY} THEN 600+(x-60000)//1000
        ELSE {LAST_LINEAR}+floor(log2(x//{DAY}))::BIGINT END""")
    db.execute(f"""CREATE MACRO bin_lower(k) AS CASE WHEN k<600 THEN k*100
        WHEN k<{LAST_LINEAR} THEN 60000+(k-600)*1000
        ELSE {DAY}*(1::BIGINT << (k-{LAST_LINEAR})) END""")
    db.execute("CREATE MACRO bin_upper(k) AS bin_lower(k+1)")


def histogram(db, daily_table, output_table, grouping=GROUP):
    """Exact integer-ms mass via endpoint bins and interior range differences.

    No interval-by-all-bins join. Most short PM intervals occupy a single bin.
    Quantiles use bucket upper bounds; integrated duration has no bucket error.
    """
    db.execute(f"""CREATE TEMP TABLE ages AS SELECT day_start,{grouping} AS group_name,metric,
        da-clock AS x,db-clock AS y,age_bin(da-clock) AS first_bin,age_bin(db-clock-1) AS last_bin
        FROM {daily_table},LATERAL (VALUES ('silence',received),('event_age',event)) v(metric,clock)
        WHERE exclusion IS NULL AND db>da""")
    db.execute("""CREATE TEMP TABLE partials AS
        SELECT day_start,group_name,metric,first_bin AS bin,
          sum(CASE WHEN first_bin=last_bin THEN y-x ELSE bin_upper(first_bin)-x END)::HUGEINT AS mass
        FROM ages GROUP BY day_start,group_name,metric,first_bin
        UNION ALL SELECT day_start,group_name,metric,last_bin,sum(y-bin_lower(last_bin))::HUGEINT
        FROM ages WHERE last_bin>first_bin GROUP BY day_start,group_name,metric,last_bin""")
    db.execute("""CREATE TEMP TABLE differences AS
        SELECT day_start,group_name,metric,bin,sum(delta)::BIGINT AS delta FROM (
          SELECT day_start,group_name,metric,first_bin+1 AS bin,1 AS delta FROM ages WHERE last_bin>first_bin+1
          UNION ALL SELECT day_start,group_name,metric,last_bin,-1 FROM ages WHERE last_bin>first_bin+1
        ) GROUP BY day_start,group_name,metric,bin""")
    db.execute(f"""CREATE TABLE {output_table} AS WITH domains AS (
        SELECT day_start,group_name,metric,min(bin) AS lo,max(bin) AS hi FROM differences GROUP BY day_start,group_name,metric
    ), dense AS (SELECT d.*,r.bin FROM domains d,LATERAL range(d.lo,d.hi+1) r(bin)), interiors AS (
        SELECT d.day_start,d.group_name,d.metric,d.bin,
          sum(coalesce(e.delta,0)) OVER(PARTITION BY d.day_start,d.group_name,d.metric ORDER BY d.bin ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS density
        FROM dense d
        LEFT JOIN differences e ON d.day_start=e.day_start AND d.group_name=e.group_name AND d.metric=e.metric AND d.bin=e.bin
    ), mass AS (
        SELECT day_start,group_name,metric,bin,mass FROM partials
        UNION ALL SELECT day_start,group_name,metric,bin,density*(bin_upper(bin)-bin_lower(bin)) FROM interiors WHERE density>0
    ) SELECT day_start,group_name,metric,bin,sum(mass)::HUGEINT AS duration_ms FROM mass
      GROUP BY day_start,group_name,metric,bin HAVING sum(mass)>0""")
    for table in ("ages","partials","differences"):
        db.execute(f"DROP TABLE {table}")


def fit_candidates(db, fit_end, helper, work):
    """Use the existing Rust Histogram/body_normal/candidates on sufficient bins.

    The SQL output is filtered before the native method sees it. Missing required
    groups propagate the original method's error; no zero default is invented.
    """
    import json
    import subprocess
    from dpt_health.projection import literal
    source=work/f"fit-{fit_end}.bins.jsonl"
    result=work/f"fit-{fit_end}.candidates.json"
    db.execute(f"COPY (SELECT day_start,group_name,metric,bin_lower(bin) AS lower,duration_ms AS weight FROM selected_histogram WHERE day_start<{fit_end} ORDER BY day_start,group_name,metric,bin) TO {literal(source)} (FORMAT JSON)")
    subprocess.run([str(helper),"fit-columnar-histograms",str(source),str(result)],check=True)
    candidates=[]
    for item in json.loads(result.read_text()):
        parameters={}
        for group in ("polymarket","chainlink","binance_trade","binance_depth"):
            h=item["health"]
            parameters[group]=dict(normal_s=h[group+"_silence_normal_ms"],normal_a=h[group+"_event_delay_normal_ms"],max_s=h[group+"_silence_ms"],max_a=h[group+"_event_delay_ms"])
        candidates.append(dict(identifier=item["name"],fit_end_utc_ms=fit_end,parameters=parameters,health=item["health"]))
    return candidates


def aggregates(db, table):
    cursor=db.execute(f"""SELECT source,condition_id,asset,state,sum(b-a)::HUGEINT AS duration_ms,
        count(*) AS fragments FROM {table} GROUP BY source,condition_id,asset,state ORDER BY source,condition_id,asset,state""")
    return [dict(zip([x[0] for x in cursor.description],row)) for row in cursor.fetchall()]


def evaluate(db, candidate, a, b, name, output):
    # Fit and validation are separate SQL domains. Future records cannot update
    # frozen candidates. Read preparation may include later data; fit excludes it.
    # Merge adjacent positive-demand pieces before applying observation bounds.
    # A change in active market count is not a source deselection.
    db.execute("""CREATE TEMP TABLE evaluation_scopes AS
        SELECT source,condition_id,asset,a AS evaluation_start,b FROM scopes WHERE source='polymarket'
        UNION ALL SELECT source,'','',min(a),max(b) FROM (
            SELECT *,sum((previous_end IS DISTINCT FROM a)::INTEGER) OVER(PARTITION BY source ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS period
            FROM (SELECT *,lag(b) OVER(PARTITION BY source ORDER BY a) AS previous_end FROM selections))
        GROUP BY source,period""")
    db.execute(f"""CREATE TEMP TABLE evaluation_domain AS
        SELECT * REPLACE(greatest(a,{a}-1) AS a,least(b,{b}) AS b) FROM selected_intervals WHERE a<{b} AND b>{a}-1
        UNION ALL SELECT f.* REPLACE(f.a AS a,least(f.b,s.a) AS b) FROM boundary_facts f JOIN scopes s USING(source,condition_id,asset)
        WHERE f.source='polymarket' AND s.a>={a} AND s.a<{b}
        UNION ALL SELECT i.* REPLACE(s.evaluation_start-1 AS a,s.evaluation_start AS b)
        FROM causal_intervals i JOIN evaluation_scopes s USING(source,condition_id,asset)
        WHERE s.source!='polymarket' AND s.evaluation_start>={a} AND s.evaluation_start<{b}
          AND i.a<=s.evaluation_start-1 AND i.b>s.evaluation_start-1""")
    candidate_intervals(db,candidate["parameters"],"evaluation_domain","candidate_spans")
    db.execute("""CREATE TEMP TABLE scoped_candidate_spans AS SELECT c.*,s.evaluation_start
        FROM candidate_spans c JOIN evaluation_scopes s USING(source,condition_id,asset)
        WHERE c.b>s.evaluation_start-1 AND c.a<s.b""")
    episodes(db,"scoped_candidate_spans","uncut_episodes",selection_scopes=True)
    db.execute(f"""CREATE TABLE candidate_episodes AS SELECT e.*
        REPLACE(greatest(e.a,s.evaluation_start,{a}) AS a),
        e.a<greatest(s.evaluation_start,{a}) AS left_censored,
        e.b=least(s.b,{b}) AS right_censored
        FROM uncut_episodes e JOIN evaluation_scopes s USING(source,condition_id,asset,evaluation_start)
        WHERE e.b>greatest(s.evaluation_start,{a})""")
    daily_intervals(db,"candidate_episodes","episode_daily")
    # Episode counts over a day are intersected fragments. They cannot be summed
    # into whole-window episode counts. A true transition at midnight belongs to
    # the new half-open day, using the actual previous positive-duration state.
    daily_cursor=db.execute("""SELECT source,condition_id,asset,day_start,state,
        sum(db-da)::HUGEINT AS duration_ms,count(*) AS episode_fragments,
        sum((NOT left_censored AND a=da AND previous_state IS NOT NULL AND previous_end=a)::INTEGER) AS transitions,
        sum((left_censored OR a<da)::INTEGER) AS left_censored_fragments,
        sum((right_censored OR b>db)::INTEGER) AS right_censored_fragments,
        max(db-da) AS longest_fragment_ms
        FROM episode_daily GROUP BY source,condition_id,asset,day_start,state ORDER BY source,condition_id,asset,day_start,state""")
    daily=[dict(zip([c[0] for c in daily_cursor.description],row)) for row in daily_cursor.fetchall()]
    whole_cursor=db.execute("""SELECT source,condition_id,asset,state,sum(b-a)::HUGEINT AS duration_ms,
        count(*) AS episodes,max(b-a) AS longest_episode_ms,
        sum((NOT left_censored AND previous_state IS NOT NULL AND previous_end=a)::INTEGER) AS transitions,
        sum(left_censored::INTEGER) AS left_censored_episodes,sum(right_censored::INTEGER) AS right_censored_episodes
        FROM candidate_episodes GROUP BY source,condition_id,asset,state ORDER BY source,condition_id,asset,state""")
    whole=[dict(zip([c[0] for c in whole_cursor.description],row)) for row in whole_cursor.fetchall()]
    # Concrete long-stall threshold crossings, with their actual clocks and
    # preceding state. This does not claim that Exceeded identifies a fault.
    from dpt_health.projection import literal
    db.execute(f"""COPY (SELECT source,condition_id,asset,a AS trigger_at,b AS episode_end,received,event,
        a-received AS silence_ms,a-event AS event_age_ms,previous_state,original_at AS last_fact_visible_at,
        (NOT left_censored AND a>original_at AND previous_state IS NOT NULL AND previous_end=a) AS crossed_without_new_message,
        left_censored AS evaluation_left_censored,right_censored AS evaluation_right_censored
        FROM candidate_episodes WHERE state='exceeded') TO {literal(output/(name+'.exceeded.parquet'))} (FORMAT PARQUET,COMPRESSION ZSTD)""")
    grouped={}
    markets={}
    for row in whole:
        group="chainlink" if row["source"].startswith("chainlink_") else row["source"]
        key=(group,row["state"])
        aggregate=grouped.setdefault(key,dict(group=group,state=row["state"],duration_ms=0,episodes=0,transitions=0))
        for field in ("duration_ms","episodes","transitions"):
            aggregate[field]+=row[field]
        if row["source"]=="polymarket":
            key=(row["condition_id"],row["state"])
            aggregate=markets.setdefault(key,dict(condition_id=row["condition_id"],state=row["state"],asset_ms=0,asset_episodes=0,asset_transitions=0))
            for field,target in (("duration_ms","asset_ms"),("episodes","asset_episodes"),("transitions","asset_transitions")):
                aggregate[target]+=row[field]
    denominators=defaultdict(int)
    for row in grouped.values():
        denominators[row["group"]]+=row["duration_ms"]
    for row in grouped.values():
        denominator=denominators[row["group"]]
        row["transitions_per_observed_hour"]=row["transitions"]*3_600_000/denominator if denominator else None
        row["hour_unit"]="asset-hours" if row["group"]=="polymarket" else "source-hours"
    result=dict(candidate=candidate,window=[a,b],states=whole,daily_states=daily,
                groups=list(grouped.values()),markets=list(markets.values()),
                exceeded_triggers=name+'.exceeded.parquet',duration_unit="PM: asset-ms; common: source-ms")
    for table in ("episode_daily","candidate_episodes","uncut_episodes","scoped_candidate_spans","candidate_spans","evaluation_domain","evaluation_scopes"):
        db.execute(f"DROP TABLE {table}")
    return result
