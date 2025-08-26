# Known script/widget/url substrings for ticketing providers (lowercase regex)
PROVIDER_PATTERNS = {
"Eventbrite": [r"eventbrite", r"evb\\.js", r"eventbrite\\.com/(?:e|o)/", r"data-eventbrite"],
"Ticketmaster": [r"ticketmaster", r"tm(?:ticket)?", r"ticketmaster\\."],
"See Tickets": [r"seetickets", r"see\\stickets"],
"Fever": [r"fever", r"feverup", r"fever-content"],
"Spektrix": [r"spektrix", r"system\\.spektrix"],
"Pretix": [r"pretix"],
"Weezevent": [r"weezevent"],
}


# Words that indicate search/aggregator/listing portals (not a checkout platform)
AGGREGATOR_KEYWORDS = [
"aggregator", "directory", "search", "price comparison", "viagogo", "stubhub", "resale"
]
