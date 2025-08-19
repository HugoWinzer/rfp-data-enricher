# src/vendor_patterns.py
from dataclasses import dataclass
from typing import List, Dict

@dataclass
class VendorSignature:
    name: str
    domains: List[str]
    script_substrings: List[str]
    link_keywords: List[str]  # words likely in ticket buttons/paths

VENDOR_SIGNATURES: List[VendorSignature] = [
    VendorSignature(
        name="Ticketmaster",
        domains=["ticketmaster.", "ticketmaster.fr", "ticketmaster.com"],
        script_substrings=["tm.ticketmaster", "ticketmaster.com/widget", "ticketmaster.eu"],
        link_keywords=["ticketmaster", "buy tickets", "billetterie", "acheter", "tickets"]
    ),
    VendorSignature(
        name="Eventbrite",
        domains=["eventbrite.", "eventbrite.fr", "eventbrite.com"],
        script_substrings=["eventbrite-widget", "eventbrite.com/static/widgets"],
        link_keywords=["eventbrite", "tickets", "register"]
    ),
    VendorSignature(
        name="Weezevent",
        domains=["weezevent.", "weezevent.com", "weezevent.fr"],
        script_substrings=["weezevent.com/js/"],
        link_keywords=["weezevent", "billetterie", "acheter", "reserver"]
    ),
    VendorSignature(
        name="Billetweb",
        domains=["billetweb.", "billetweb.fr", "billetweb.net"],
        script_substrings=["billetweb.fr/widget", "billetweb.net/widget"],
        link_keywords=["billetweb", "billetterie", "reserver"]
    ),
    VendorSignature(
        name="Digitick",
        domains=["digitick.", "digitick.com"],
        script_substrings=["digitick.com/widget"],
        link_keywords=["digitick", "billetterie"]
    ),
    VendorSignature(
        name="See Tickets",
        domains=["seetickets.", "seetickets.com", "seetickets.fr"],
        script_substrings=["seetickets.com/script", "shop.seetickets"],
        link_keywords=["see tickets", "seetickets", "tickets"]
    ),
    VendorSignature(
        name="Dice",
        domains=["dice.fm"],
        script_substrings=["dice.fm/embed", "widgets.dice.fm"],
        link_keywords=["dice", "tickets"]
    ),
    VendorSignature(
        name="AXS",
        domains=["axs.com"],
        script_substrings=["axs.com/embedded"],
        link_keywords=["axs", "tickets"]
    ),
    VendorSignature(
        name="Eventim",
        domains=["eventim.", "eventim.fr", "eventim.de", "eventim.com"],
        script_substrings=["eventim.", "eventim.com/widgets"],
        link_keywords=["eventim", "tickets"]
    ),
    VendorSignature(
        name="Shotgun",
        domains=["shotgun.live"],
        script_substrings=["shotgun.live/widget"],
        link_keywords=["shotgun", "tickets"]
    ),
]

# vendor priority if multiple are found (purchase-path first, then this order)
VENDOR_PRIORITY: Dict[str, int] = {
    "Ticketmaster": 10,
    "Eventbrite": 9,
    "Weezevent": 8,
    "Billetweb": 8,
    "See Tickets": 7,
    "Dice": 7,
    "AXS": 6,
    "Eventim": 6,
    "Digitick": 5,
    "Shotgun": 5,
}
