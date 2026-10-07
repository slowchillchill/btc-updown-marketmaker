"""Duration-weighted distributions and forward-only candidate evaluation."""
from .intervals import DAY

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
