# Known script/widget/url substrings for ticketing providers (lowercase regex)
PROVIDER_PATTERNS = {
    "Eventbrite": [
        r"eventbrite",
        r"evb\.js",
        r"eventbrite\.com/(?:e|o)/",
        r"data-eventbrite",
    ],
    "Ticketmaster": [
        r"ticketmaster",
        r"tm(?:ticket)?",
        r"ticketmaster\.",
    ],
    "See Tickets": [
        r"seetickets",
        r"see\stickets",
    ],
    "Fever": [
        r"feverup\.com",
        r"feverup",
        r"fever\s?tickets",
    ],
    "Pretix": [
        r"pretix\.",
        r"widget\.pretix",
    ],
    "Ticket Tailor": [
        r"tickettailor",
        r"tickets\.tickettailor\.com",
    ],
    "Weezevent": [
        r"weezevent",
        r"wze\.io",
    ],
    "Eventix": [
        r"eventix\.",
        r"tickets\.eventix",
    ],
    "Universe": [
        r"universe\.com",
    ],
    "Spektrix": [
        r"spektrix",
        r"system\.spektrix",
    ],
    "YoYo": [
        r"yoyo(?:tickets|ticketing)?",
    ],
    "Billetto": [
        r"billetto",
    ],
    "TicketOne": [
        r"ticketone\.it",
    ],
}

# Words that indicate search/aggregator/listing portals (not a checkout platform)
AGGREGATOR_KEYWORDS = [
    "aggregator",
    "directory",
    "search",
    "price comparison",
    "ticketsuche",
    "ticcats",
    "viagogo",
    "stubhub",
    "trivialtickets",
]
