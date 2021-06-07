import os, sys
import json
import requests
import random


class Firefox_ua:
    PLATFORMS = [
        "X11; Ubuntu; Linux x86_64;",
        "X11; Linux x86_64;",
        "X11; Linux i686;",
        "Windows NT 6.1; Win64; x64;",
        "Macintosh; Intel Mac OS X x.y;",
        "iPhone; CPU iPhone OS 13_5_1 like Mac OS X;",
    ]

    RV_VERSIONS = [
        "84.0",
        "83.0",
        "74.0",
        "64.0",
        "67.0",
        "42.0",
    ]

    def gen(self):
        rv_vers = random.choice(self.RV_VERSIONS)
        return "Mozilla/5.0 (%s rv:%s) Gecko/20100101 Firefox/%s" % (random.choice(self.PLATFORMS), rv_vers, rv_vers)


class Chrome_ua:

    def gen(self):
        return "Mozilla/5.0 (%s) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36" % (random.choice(Firefox_ua.PLATFORMS))


class Opera_ua:

    def gen(self):
        return "Mozilla/5.0 (%s) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36 OPR/38.0.2220.41" % (random.choice(Firefox_ua.PLATFORMS))


class Safary_ua:

    def gen(self):
        return "Mozilla/5.0 (%s) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Mobile/15E148 Safari/604.1" % (random.choice(Firefox_ua.PLATFORMS))


class InternetExplorer_ua:

    def gen(self):
        return "Mozilla/5.0 (compatible; MSIE 9.0; Windows Phone OS 7.5; Trident/5.0; IEMobile/9.0)"


class Crawler_ua:
    def gen(self):
        variables = ["Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
                     "Mozilla/5.0 (compatible; YandexAccessibilityBot/3.0; +http://yandex.com/bots)", ]
        return random.choice(variables)


def gen_headers(bCrawler=False):
    headers = [
        Firefox_ua().gen(),
        Chrome_ua().gen(),
        Safary_ua().gen(),
        InternetExplorer_ua().gen(),
    ]
    if bCrawler:
        headers += [Crawler_ua().gen()]
    return {"User-Agent": random.choice(headers)}
