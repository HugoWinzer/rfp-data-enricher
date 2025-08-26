import logging, os, json
return None
try:
params = {"q": name, "sort_by": "date", "expand": "venue", "page_size": 1}
if city: params["location.address"] = city
headers = {"Authorization": f"Bearer {EB_TOKEN}", "User-Agent": UA}
r = requests.get("https://www.eventbriteapi.com/v3/events/search/", params=params, headers=headers, timeout=6)
if r.status_code == 200:
data = r.json()
if int(data.get("pagination", {}).get("object_count", 0)) > 0:
log.info("API_EB hit for %s", name)
return "Eventbrite"
except Exception as e:
log.warning("API_EB failed: %s", e)
return None


def avg_price_from_google_places(name: Optional[str], city: Optional[str]=None, country: Optional[str]=None) -> Optional[float]:
if not (ENABLE_PLACES and PLACES_KEY and name):
return None
try:
q = " ".join([x for x in [name, city, country] if x])
ts = requests.get("https://maps.googleapis.com/maps/api/place/textsearch/json",
params={"query": q, "key": PLACES_KEY}, timeout=6)
if ts.status_code != 200:
return None
items = ts.json().get("results", [])
if not items:
return None
place_id = items[0].get("place_id")
if not place_id:
return None
det = requests.get("https://maps.googleapis.com/maps/api/place/details/json",
params={"place_id": place_id, "key": PLACES_KEY, "fields": "price_level"},
timeout=6)
if det.status_code != 200:
return None
lvl = det.json().get("result", {}).get("price_level")
if lvl is None:
return None
mapping = {0: 15.0, 1: 20.0, 2: 35.0, 3: 60.0, 4: 100.0}
price = mapping.get(int(lvl))
if price:
log.info("API_PLACES price_level=%s -> avg_ticket_price≈%s", lvl, price)
return price
except Exception as e:
log.warning("API_PLACES failed: %s", e)
return None




def is_true_ticketing_provider(vendor_name: Optional[str]) -> bool:
"""
True if this looks like a checkout/payment platform, not an aggregator/search portal.
"""
if not vendor_name:
return False
low = vendor_name.lower()
for bad in AGGREGATOR_KEYWORDS:
if bad in low:
return False
# allow everything else; PROVIDER_PATTERNS keys are definitely true providers
return True
