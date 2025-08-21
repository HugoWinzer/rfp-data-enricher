from dataclasses import dataclass
from typing import List, Dict


@dataclass
class VendorSignature:
    name: str
    domains: List[str]
    script_substrings: List[str]
    link_keywords: List[str]  # likely words on purchase buttons/paths


VENDOR_SIGNATURES: List[VendorSignature] = [
    VendorSignature(
        name="Ticketmaster",
        domains=["ticketmaster.", "ticketmaster.com", "ticketmaster.fr", "ticketmaster.es"],
        script_substrings=["tm.ticketmaster", "ticketmaster.com/widget", "ticketmaster.eu"],
        link_keywords=["ticketmaster", "buy tickets", "billets", "entradas"],
    ),
    VendorSignature(
        name="Eventbrite",
        domains=["eventbrite.", "eventbrite.com", "eventbrite.fr", "eventbrite.es"],
        script_substrings=["eventbrite.com/static/", "evb/ui/"],
        link_keywords=["eventbrite", "get tickets", "register"],
    ),
    VendorSignature(
        name="Weezevent",
        domains=["weezevent.", "weezevent.com", "weezevent.com/fr/"],
        script_substrings=["weezpay", "widget.weezevent"],
        link_keywords=["weezevent", "billetterie"],
    ),
    VendorSignature(
        name="Billetweb",
        domains=["billetweb.", "billetweb.fr"],
        script_substrings=["billetweb.fr/js", "widgets.billetweb"],
        link_keywords=["billetweb", "billetterie"],
    ),
    VendorSignature(
        name="See Tickets",
        domains=["seetickets.", "seetickets.com", "seetickets.fr"],
        script_substrings=["seetickets.com/tour", "widgets.seetickets"],
        link_keywords=["see tickets", "tickets"],
    ),
    VendorSignature(
        name="Dice",
        domains=["dice.fm", "dice.", "link.dice.fm"],
        script_substrings=["dice.fm/embed", "widgets.dice"],
        link_keywords=["dice", "tickets"],
    ),
    VendorSignature(
        name="AXS",
        domains=["axs.com", "shop.axs.com", "axs."],
        script_substrings=["axs.com/axs-widget"],
        link_keywords=["axs", "tickets"],
    ),
    VendorSignature(
        name="Eventim",
        domains=["eventim.", "eventim.de", "eventim.fr", "eventim.es", "eventim.co.uk"],
        script_substrings=["eventim", "etix"],
        link_keywords=["eventim", "tickets"],
    ),
    VendorSignature(
        name="Digitick",
        domains=["digitick.", "digitick.com"],
        script_substrings=["digitick.com/widget"],
        link_keywords=["digitick", "billetterie"],
    ),
    VendorSignature(
        name="Shotgun",
        domains=["shotgun.live", "shotgun.", "shotgun.xxx"],
        script_substrings=["shotgun.live/widget", "shotgun.live/embed"],
        link_keywords=["shotgun", "tickets"],
    ),
    # Added
    VendorSignature(
        name="Fever",
        domains=["feverup.com", "feverup."],
        script_substrings=["feverup.com/widget", "feverup.com/embeds"],
        link_keywords=["fever", "book now"],
    ),
    VendorSignature(
        name="Universe",
        domains=["universe.com", "universetickets."],
        script_substrings=["universe.com/embed"],
        link_keywords=["universe", "tickets"],
    ),
]

# Preference order when multiple are detected
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
    "Fever": 5,
    "Universe": 4,
}
