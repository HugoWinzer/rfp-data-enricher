#!/usr/bin/env python3
return ("ok", 200, {"Content-Type": "text/plain; charset=utf-8"})




@app.get("/ready")
def ready():
try:
BQ.query("SELECT 1", location=BQ_LOCATION).result()
return jsonify({"ready": True, "bq_location": BQ_LOCATION}), 200
except Exception as e:
log.warning("ready check failed: %s", e)
return jsonify({"ready": False, "error": str(e)}), 503




@app.route("/", methods=["GET", "HEAD"])
def root():
if request.method == "HEAD":
return ("", 200, {})
try:
limit = int(request.args.get("limit", "10"))
except ValueError:
limit = 10
dry = request.args.get("dry") in ("1", "true", "True", "yes")


try:
if dry:
count = len(get_candidates(limit))
return jsonify({"processed": 0, "candidates": count, "status": "DRY_OK"}), 200
count = run_batch(limit)
return jsonify({"processed": count, "status": "OK"}), 200
except Exception as e:
log.exception("Batch failed")
return jsonify({"processed": 0, "status": "ERROR", "error": str(e)}), 500




@app.get("/stats")
def stats():
try:
# Overview counts
q1 = f"""
SELECT
COUNT(*) AS total,
COUNTIF(enrichment_status = 'OK') AS ok,
COUNTIF(enrichment_status IS NULL OR enrichment_status != 'OK') AS pending,
COUNTIF(ticket_vendor IS NOT NULL) AS have_vendor,
COUNTIF(capacity IS NOT NULL) AS have_capacity,
COUNTIF(avg_ticket_price IS NOT NULL) AS have_price
FROM {table_fqdn()}
"""
overview = list(BQ.query(q1, location=BQ_LOCATION).result())[0]
ov = {k: overview[k] for k in overview.keys()}


# Top vendors
q2 = f"""
SELECT ticket_vendor, COUNT(*) AS c
FROM {table_fqdn()}
WHERE ticket_vendor IS NOT NULL
GROUP BY 1
ORDER BY c DESC
LIMIT 15
"""
vendors = [{"ticket_vendor": r["ticket_vendor"], "count": r["c"]}
for r in BQ.query(q2, location=BQ_LOCATION).result()]
return jsonify({"overview": ov, "top_vendors": vendors}), 200
except Exception as e:
return jsonify({"error": str(e)}), 500




@app.get("/healthz")
@app.get("/healthz/")
@app.get("/_ah/health")
def _health_compat():
return ("ok", 200, {"Content-Type": "text/plain; charset=utf-8"})




if __name__ == "__main__":
port = int(os.environ.get("PORT", "8080"))
app.run(host="0.0.0.0", port=port)
