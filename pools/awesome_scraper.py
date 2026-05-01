"""Scrape awesome-bitshares README and generate HTML + latency map GIF."""

import glob
import os
import re

import markdown
import requests
from PIL import Image

PATH = os.path.dirname(os.path.abspath(__file__)) + "/"

CEX = """
    <ul>
    <li><a class=".awesome" target="_blank" href="https://www.gate.io/trade/BTS_BTC">GateIO BTS:BTC</a></li>
    <li><a class=".awesome" target="_blank" href="https://www.gate.io/trade/BTS_USDT">GateIO BTS:USDT</a></li><br>
    <li><a class=".awesome" target="_blank" href="https://global.bittrex.com/trade/bts-btc">Bittrex BTS:BTC</a></li><br>
    <li><a class=".awesome" target="_blank" href="https://www.binance.com/en/trade/BTS_BTC">Binance BTS:BTC</a></li>
    <li><a class=".awesome" target="_blank" href="https://www.binance.com/en/trade/BTS_USDT">Binance BTS:USDT</a></li><br>
    <li><a class=".awesome" target="_blank" href="https://poloniex.com/spot/BTS_USDT">Poloniex BTS:USDT</a></li>
    <li><a class=".awesome" target="_blank" href="https://poloniex.com/spot/BTS_BTC">Poloniex BTS:BTC</a></li><br>
    <li><a class=".awesome" target="_blank" href="https://hitbtc.com/bts-to-btc">HitBTC BTS:BTC</a></li>
    </ul>
    """

DEFAULT_EXPLORER = "btslens.pages.dev"


def generate_explorer_html(html):
    """Parse Blockchain Explorers section and generate explorer.html with sub-tabs."""
    explorers_section = html.get("Blockchain Explorers")
    if not explorers_section:
        print("WARNING: Blockchain Explorers section not found; skipping explorer generation")
        return

    link_pattern = re.compile(r'<a[^>]+href="([^"]*)"[^>]*>([^<]+)</a>')
    explorers = []
    for match in link_pattern.finditer(explorers_section):
        url = match.group(1)
        name = match.group(2).strip()
        if "github" in url.lower():
            continue
        domain = url.split("//")[-1].split("/")[0].split("#")[0]
        explorers.append((name, url, domain))

    if not explorers:
        print("WARNING: No explorer links found; skipping explorer generation")
        return

    buttons = []
    objects = []
    for idx, (name, url, domain) in enumerate(explorers):
        tab_id = f"E{idx}"
        is_default = DEFAULT_EXPLORER in domain
        active_class = ' class="active"' if is_default else ""
        active_style = ' style="display:block;"' if is_default else ""
        if is_default and "/" in url:
            data_src = url
        elif is_default:
            data_src = f"https://{domain}/dashboard"
        else:
            data_src = url

        buttons.append(f'<button class="tablinks"{active_class} onclick="switch_tab(event, \'{tab_id}\')">{name}</button>')
        if is_default:
            objects.append(f'<object id="{tab_id}" class="tabcontent" type="text/html" data="{data_src}"{active_style}></object>')
        else:
            objects.append(f'<object id="{tab_id}" class="tabcontent" type="text/html" data-src="{data_src}"></object>')

    text = (
        '<!DOCTYPE html>\n<html>\n<head>\n'
        '<link rel="stylesheet" href="main.css">\n'
        '<script type="text/javascript" src="tabs.js"></script>\n'
        '<style>.tabcontent{height:calc(100vh - 4vh);}</style>\n'
        '</head>\n<body>\n'
        '<div class="explorertab">\n'
        + "\n".join(buttons) + "\n"
        + "</div>\n"
        + "\n".join(objects) + "\n"
        + "</body>\n</html>"
    )

    with open("website/explorer.html", "w") as handle:
        handle.write(text)
    print(f"Generated explorer.html with {len(explorers)} explorers")


def main():
    URL = "https://raw.githubusercontent.com/bitshares/awesome-bitshares/master/README.md"

    data = requests.get(URL).text
    html = markdown.markdown(data)

    html = html.replace('src="logo.svg"', 'src="./images/bitshares_logo.svg"')
    html = html.replace('<a href="', '<a target="_blank" href="')

    img = (
        '<center><p><img src="./images/bitshares_logo.svg" alt="BitShares Blockchain" align="center"'
        ' style="width:25vw"></p></center>'
    )

    html = ["<h3>" + i for i in html.split("<h3>")][1:]
    html = {i.split("</h3>")[0][4:]: i.split("</h3>", 1)[1] for i in html}

    generate_explorer_html(html)

    text = (
        '<DOCTYPE html>\n<html>\n<body>\n<link rel="stylesheet" href="main.css">\n<link'
        ' rel="stylesheet" href="awesomestyle.css">\n<script type="text/javascript"'
        ' src="tabs.js"></script>\n'
    )
    text += img

    for idx, htm in enumerate(html):
        if idx % 5 == 0:
            if idx:
                text += "</div>\n"
            text += (
                '<div class="awesometab" style="position:relative;flex: 1;display: flex;'
                ' width: 100%;">\n'
            )
        text += (
            '<button class="tablinks" onclick="switch_tab(event,'
            f" '{idx}')\">{htm}</button>\n"
        )
    text += "</div>\n"

    for idx, htm in enumerate(html):
        text += f'<div id="{idx}" class="tabcontent">'
        if htm != "Exchanges":
            text += html[htm].split("<h2")[0]
        else:
            text += "<center><h2>Bitshares Decentralized Exchanges</h3></center>"
            text += html[htm].split("<h2")[0]
            text += "<br><br><center><h2>Centralized Exchanges</h3></center>"
            text += CEX
        text += "</div>"

    text = text.replace("<a ", '<a class=".awesome" ')
    text += "</body>\n</html>"

    print()

    with open("website/awesome.html", "w") as handle:
        handle.write(text)

    print("generating GIF...")

    fp_in = PATH + "latency_maps/map_*.png"
    fp_out = PATH + "website/images/map.gif"

    # https://pillow.readthedocs.io/en/stable/handbook/image-file-formats.html#gif

    img_paths = sorted(glob.glob(fp_in))
    print(img_paths)

    while len(img_paths) > 20:
        to_remove = img_paths.pop(0)
        os.remove(to_remove)

    imgs = (Image.open(f) for f in sorted(glob.glob(fp_in)))
    img = next(imgs)
    img.save(
        fp=fp_out,
        format="GIF",
        append_images=imgs,
        save_all=True,
        duration=333 + (1 / 3),
        loop=0,
    )
