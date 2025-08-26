import os


@app.get("/ready")
def ready():
try:
BQ.query("SELECT 1", location=BQ_LOCATION).result()
return ("ready", 200, {"Content-Type": "text/plain; charset=utf-8"})
except Exception:
return ("not ready", 503, {"Content-Type": "text/plain; charset=utf-8"})




@app.get("/")
@app.get("/batch")
def index():
if request.method != "GET":
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


try:
count, status = run_batch(limit)
return jsonify({"processed": count, "status": status}), 200
except GPTQuotaExceeded as e:
# When STOP_ON_GPT_QUOTA is true, emit 429 so your caller/loop halts immediately.
code = 429 if STOP_ON_GPT_QUOTA else 200
return jsonify({"processed": 0, "status": "GPT_QUOTA", "error": str(e)}), code


except Exception as e:
log.exception("Batch failed")
return jsonify({"processed": 0, "status": "ERROR", "error": str(e)}), 500




@app.get("/stats")
def stats():
try:
q1 = f"""
SELECT
COUNT(*) total,
COUNTIF(enrichment_status = 'OK') ok,
COUNTIF(enrichment_status IS NULL OR enrichment_status != 'OK') pending,
COUNTIF(ticket_vendor IS NOT NULL) have_vendor,
COUNTIF(capacity IS NOT NULL) have_capacity,
COUNTIF(avg_ticket_price IS NOT NULL) have_price
FROM {table_fqdn()}
"""
j = BQ.query(q1, location=BQ_LOCATION).result()
row = list(j)[0]
return jsonify(dict(row)), 200
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
