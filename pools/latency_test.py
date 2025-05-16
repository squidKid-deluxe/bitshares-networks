"""
Bitshares Public Node Location Utility

litepresence 2019

def WTFPL_v0_March_1765():

    if any([stamps, licenses, taxation, regulation, fiat, etat]):
        try:
            print("no thank you")
        except BaseException:
            return [tar, feathers]

************ ALPHA RELEASE TO PUBLIC DOMAIN WITH NO WARRANTY ***********

# Confirms Chain ID, Blocktime, Ping, and Participation Rate
# No pybitshares dependencies
# Writes unique domains to file nodes.txt
# Uploads list and other latency data to jsonbin.io
# Includes Geolocation Data from ip-api.com
# Creates map from geolocation data and uploads to vgy.me
# Base map image auto downloads from imgur
# Saves map history
# Animated map history
"""

# STANDARD PYTHON MODULES
from multiprocessing import Process, Value
from json import loads as json_load
from json import dumps as json_dump
from calendar import timegm
from pprint import pprint
import traceback
import time
import sys
import os

# MODULES WHICH REQUIRE INSTALL
import matplotlib.pyplot as plt
import matplotlib.cbook as cbook
from websocket import create_connection as wss
import websocket
import requests
import socket

import asyncio
import time
import aiohttp
import json
from datetime import datetime  # For ISO date conversion

# CUSTOM MODULES
from bitshares_nodes import Nodes

# USER CONTROL PANEL
# ######################################################################
VERSION = "Bitshares latencyTEST 0.00000013"
# SELECT NODES TO TEST
RECENT = True  # Check Nodes.recent() seen in past few months
SINCE_181127 = True  # Check all nodes seen since core version 181127
TESTNET = False  # Include nodes with word "test" in domain
UNIVERSE = False  # Check all nodes in known Nodes.universe()
APASIA = False  # Check Nodes.apasia() infrastructure worker
# PING AND GEOLOCATE SEED NODES
SEEDS = False  # NOTE: system ping & geolocation only; plots in RED
# TEXT SCRAPE GITHUB FOR LATEST NODES
GITHUB = True  # Check all known github repos for nodes lists
GITHUB_MASTER = False  # Check only Bitshares UI Master List
# EXCLUSIONS
ONLY = False  # Test just Nodes.short_list() list (ignore all lists above)
EXCLUDE = True  # Exclude known bad nodes in Nodes.exclude()
# PLOT LATENCY MAP
GEOLOCATE = "http://ip-api.com/batch/"
IPAPI = True  # set to true to add geolocation data
PLOT = True  # set to true to plot
MAP_SAVE = True
MAP_FRAMES = 0
MAP_PAUSE = 2
# TEST SETTINGS
NOPRINT = False  # Reduced terminal printing
TRACE_DETAIL = False  # websocket.enableTrace
WRITE = False  # Write nodes.txt with unique list
LOOP = False  # Repeat latency test indefinitely
TIMEOUT = 5  # Websocket Timeout
CROP1 = 999  # Crop initial list for quick test (999 to disable)
CROP2 = 999  # Crop final list to fastest responders (999 to disable)
REPEAT = 3600  # Repeat frequency of latecy retest loop
# PROXY GITHUB RAW CONTENT
PROXY_GITHUB = False  # Proxy Github raw content (my ISP blocks)
PROXY = "www.textise.net/showText.aspx?strURL="
# SHARE RESULTS TO WEB
# FREE WWW.VGY.ME IMAGE HOSTING
MAP_UPLOAD = False  # Upload map to vgy.me image sharing
# FREE WWW.JSONBIN.IO JSON API HOSTING
API_CREATE = False  # Create a new api endpoint at jsonbin.io
API_UPLOAD = False  # Share your latency test data via jsonbin api
BIN = "set API_CREATE to True to use create_jsonbin() utility"
KEY = "get an api key to share your data on the web from jsonbin.io"
# ######################################################################
# BITSHARES MAINNET CHAIN ID
ID = "4018d7844c78f6a6c41c6a552b898022310fc5dec06da467ee7905a8dad512c8"
# LOCATION IN WHICH THIS SCRIPT IS RUNNING
PATH = str(os.path.dirname(os.path.abspath(__file__))) + "/"


# REMOTE PROCEDURE CALL TO BITSHARES PUBLIC API NODES
# ######################################################################
async def wss_handshake(node):
    """Asynchronously create a websocket connection to a BitShares public RPC node."""
    if not hasattr(wss_handshake, "session") or not wss_handshake.session:
        wss_handshake.session = aiohttp.ClientSession()
    try:
        ws = await wss_handshake.session.ws_connect(node, timeout=TIMEOUT)  # Await the connection
        return ws  # Return the websocket object
    except Exception as e:
        print(f"Error during handshake with {node}: {e}")
        return None

async def wss_query(rpc, params):
    """Asynchronously send and receive RPC to a public node."""
    query = json.dumps({"method": "call", "params": params, "jsonrpc": "2.0", "id": 1})
    try:
        await rpc.send_str(query)
        msg = await rpc.receive()
        if msg.type == aiohttp.WSMsgType.TEXT:
            ret = json.loads(msg.data)
            try:
                ret = ret["result"]  # Extract result if available
            except KeyError:
                pass  # No result key; return the full response
            return ret
        elif msg.type == aiohttp.WSMsgType.CLOSED:
            print(f"Websocket closed with code {rpc.close_code}")
            return None
        elif msg.type == aiohttp.WSMsgType.ERROR:
            print(f"Websocket error: {rpc.exception()}")
            return None
    except Exception as e:
        print(f"Error during query: {e}")
        return None

async def rpc_chain_id(rpc):
    """Asynchronously get the chain ID."""
    ret = await wss_query(rpc, ["database", "get_chain_properties", []])
    if ret:
        return ret.get("chain_id")  # Safely return chain_id
    return None

async def rpc_blocktime_participation(rpc):
    """Asynchronously get block time and participation."""
    ret = await wss_query(rpc, ["database", "get_objects", [["2.1.0"]]])
    if ret and len(ret) > 0:
        ret = ret[0]  # Access the first element
        unix = from_iso_date(ret["time"])
        participation = (bin(int(ret["recent_slots_filled"])).count("1") / 1.28)  # Calculate participation
        return unix, participation
    return None, None


# TEXT PIPE INTERPROCESS COMMUNICATION
# ######################################################################
def race_write(doc="", text=""):
    """
    Concurrent Write to File Operation
    """
    text = str(text)
    i = 0
    while True:
        time.sleep(0.05 * i**2)
        i += 1
        try:
            with open(doc, "w+") as handle:
                handle.write(text)
                handle.close()
                break
        except Exception as error:
            msg = str(type(error).__name__) + str(error.args)
            print(msg)
            try:
                handle.close()
            except BaseException:
                pass
        finally:
            try:
                handle.close()
            except BaseException:
                pass


# MAP_UPLOAD RESULTS TO FREE WEB HOSTING
# ######################################################################
def upload_to_vgy():
    """
    Upload map image to free image hosting service api (no key needed)
    """
    image_url = ""
    try:
        url = "https://vgy.me/upload"
        files = {"file": open("nodemap.png", "rb")}
        response = requests.post(url, files=files)
        del files
        image_url = json_load(response.text)["image"]
    except BaseException:
        print("vgy failed")
    print(image_url)
    return image_url


def create_jsonbin():
    """
    run this subscript to create a new jsonbin
    """
    url = "https://api.jsonbin.io/b/"
    headers = {
        "Content-Type": "application/json",
        "secret-key": KEY,
        "private": "false",
    }
    data = {"UNIX": str(int(time.time()))}
    data = json_load(requests.post(url, json=data, headers=headers).text)
    print(data, "\n")
    print("Write down your new BIN: ", data["id"])


def upload_to_jsonbin(data):
    """
    upload data from latency test to jsonbin.io
    you will need to create an api key to use this feature
    see create_jsonbin()
    """
    url = "https://api.jsonbin.io/b/" + BIN
    headers = {
        "Content-Type": "application/json",
        "secret-key": KEY,
        "private": "false",
    }
    about = {
        "UNIX": str(int(time.time())),
        "OWNER": "litepresence",
        "MISSION": "Bitshares Public Node Latency Testing",
        "UTC": str(time.strftime("%a, %d %b %Y %H:%M:%S", time.gmtime())),
        "LOCATION": "USA EAST",
        "SOURCE_CODE": (
            "https://github.com/litepresence/extinction-event/blob/"
            + "GITHUB_MASTER/EV/latencyTEST.py"
        ),
    }
    data = data.update(about)
    data["DICT_KEYS"] = str(list(data.keys()))
    req = requests.put(url, json=data, headers=headers)
    del data
    print("reading jsonbin...")
    url += "/latest"
    print(url)
    req = requests.get(url, headers=headers, timeout=(6, 30))
    del url
    del headers
    print(req.text)
    del req


# FORMATTING AND PARSING
# ######################################################################


def clean(raw):
    """
    Remove quotes and commas from strings; convert to space
    """
    ret = str(raw).replace('"', " ").replace("'", " ")
    ret = ret.replace(",", " ").replace(";", " ").replace("&", " ")
    return ret


def parse(cleaned):
    """
    Return list of words beginning with wss
    """
    # print (ret)
    return [url for url in cleaned.split() if url.startswith("wss")]


def validate(nodes):
    """
    Remove suffixes for each domain
    """
    validated = nodes[:]
    for item, _ in enumerate(validated):
        if validated[item].endswith("/"):
            validated[item] = validated[item][:-1]
    for item, _ in enumerate(validated):
        if validated[item].endswith("/ws"):
            validated[item] = validated[item][:-3]
    for item, _ in enumerate(validated):
        if validated[item].endswith("/wss"):
            validated[item] = validated[item][:-4]
    return sorted(list(set(validated)))


def suffix(nodes):
    """
    Add suffixes for each domain
    """
    _wss = [(item + "/wss") for item in nodes]
    _ws = [(item + "/ws") for item in nodes]
    nodes = nodes + _wss + _ws
    return sorted(nodes)


def from_iso_date(iso):
    """
    Returns unix epoch given iso8601 datetime
    """
    return int(timegm(time.strptime((iso + "UTC"), "%Y-%m-%dT%H:%M:%S%Z")))


def to_html(data, newlines="\n", font="sans-serif", size="4vw"):
    """
    make a json list (of places, nodes, etc.) into webpage-parseable HTML
    with the following format:

    ***
    <!DOCTYPE html>
    <html>
    <head>
        <link href="main.css" rel="stylesheet">
        <style>
            *{
                font-size:@@@;
                $$$
            }
        </style>
    </head>
    <body>
    ***
    </body>
    </head>
    """
    text = (
        to_html.__doc__.split("***")[1]
        .replace("$$$", "font-family:" + font + ";")
        .replace("@@@", size)
    )
    if newlines == "\n":
        text += "\n    ".join(["<p>" + str(i) + "</p>" for i in data])
    else:
        text += "<p>"
        text += json_dump(data)
        text += "</p>"
    text += to_html.__doc__.split("***")[2]
    return text


# DOWNLOAD UTILITIES
# ######################################################################
def download_text(key):
    """
    ascii artwork stored in pastebin
    """
    urls = {"bitshares": "https://pastebin.com/raw/xDJkyBrS"}
    try:
        return (requests.get(urls[key], timeout=(6, 30))).text
    except BaseException:
        return ""


def textise(url):
    """
    My ISP is blocks github?  Textise to the rescue!
    """
    uri = "https://www.textise.net/showText.aspx?strURL=https%253A//"
    return uri + url.split("://")[1]


def scrape_github():
    """
    Lists of Bitshares nodes can be found on various github repos
    These lists change location, format, and content over time
    They are often in apiConfig.js but can be elsewhere as well
    """
    githubs = ["/bitshares/bitshares-ui/master/app/api/apiConfig.js"]
    if not GITHUB_MASTER:
        githubs += Nodes.github()
    # scrape from github
    urls = []
    http = "https://"
    raw = "raw.githubusercontent.com"
    if PROXY_GITHUB:  # text proxy
        uri = http + PROXY + raw
    else:
        uri = http + raw
    for _, git in enumerate(githubs):
        url = uri + git
        urls.append(url)
    print("scraping github for Bitshares nodes...")
    validated = []
    repos = []
    for url in urls:
        attempts = 3
        while attempts > 0:
            try:
                raw = requests.get(url, timeout=(6, 30)).text
                ret = validate(parse(clean(raw)))
                if ret:
                    repos.append(url)
                del raw
                repo = url.replace(uri, "")
                repo = "/".join(repo.split("/", 3)[:3])
                print(("found %s nodes at %s" % (len(ret), repo)))
                validated += ret
                attempts = 0
            except BaseException:
                print(("failed to connect to %s" % url))
                attempts -= 1
    return validated, repos


def get_basemap():
    """
    Use cached copy of basemap from the script's parent folder
    otherwise download basemap from imgur
    """
    url = "https://i.imgur.com/yIVogZH.png"
    image = "images/basemap.png"
    location = PATH + image
    print(location)
    try:
        basemap = cbook.get_sample_data(location)
    except BaseException:
        download_img(url, image)
        basemap = cbook.get_sample_data(location)
    return basemap


def download_img(url, local_file):
    """
    Download map image from web and write to disk as a *.png
    """
    while True:
        ret = requests.get(url, stream=True, timeout=(6, 30))
        if ret.status_code == 200:
            with open(local_file, "wb") as handle:
                for chunk in ret.iter_content(1024):
                    handle.write(chunk)
            break


# DRAW LATENCY MAP
# ######################################################################
def plot(geo, speed, mean_speed):
    """
    Given list of geolocated nodes plot them on world map
    """
    try:
        plt.close()
    except BaseException:
        pass
    print("plotting...")
    fig, axis = plt.subplots(figsize=(12, 24), facecolor="black")
    # remove tick markers
    plt.xticks([])
    plt.yticks([])
    # plot basemap
    axis.imshow(plt.imread(PATH + "images/basemap.png"), extent=[-180, 180, -90, 90])
    fig.tight_layout()
    # plot transparent magenta signal strength at each location
    lons = []
    lats = []
    for item, loc in enumerate(geo):
        print(item, loc)
        try:
            lon = float(geo[item][1]["lon"])
            lat = float(geo[item][1]["lat"])
            lons.append(lon)
            lats.append(lat)
            location = geo[item][0]
            try:
                relative_speed = float(speed[item][1]) / mean_speed
                m_size = 15 / relative_speed
            except BaseException:
                m_size = 15
            if m_size > 100:
                m_size = 100
            elif m_size < 30:
                m_size = 30
            print(lon, lat, location, relative_speed, m_size)
            plt.plot(lon, lat, "mo", markersize=m_size, alpha=0.25)
        except BaseException:
            print("skipping", geo[item])
    # plot small solid yellow dot on each location
    try:
        plt.plot([lons], [lats], marker="o", markersize=4, color="yellow", alpha=1)
    except BaseException:
        print(traceback.format_exc())
    # Add extended format timestamp to map
    utc = str(time.strftime("%Y-%m-%e %H:%M", time.gmtime())) + " UTC"
    plt.text(0, -75, utc, alpha=0.5, color="w", size=15)


def plot_seed_nodes(seeds):
    """
    Add seed node locations to world map
    """
    x_vals = []
    y_vals = []
    for seed in seeds:
        if float(seed[1]) > 0:
            try:
                x_val = float(seed[2]["lon"])
                y_val = float(seed[2]["lat"])
                x_vals.append(x_val)
                y_vals.append(y_val)
            except BaseException:
                print(traceback.format_exc())
                print("skipping", seed)
    plt.plot([x_vals], [y_vals], marker="o", markersize=3, color="red", alpha=1)


def save_figure():
    """
    Save the plotted map to hard drive as a *png file
    """
    location = PATH + "latency_maps/map.png"
    plt.savefig(location, dpi=120, bbox_inches="tight", pad_inches=0)
    if MAP_SAVE:
        location = PATH + "latency_maps/map_" + str(int(time.time())) + ".png"
        plt.savefig(location, dpi=120, bbox_inches="tight", pad_inches=0)


def map_animate():
    """
    Instead of performing a latency test, show animated map history
    """
    # list of items in directory
    maps = sorted(os.listdir(PATH + "latency_maps/"))
    # only those that are png and have unix timestamp
    maps = [i for i in maps if ((len(i) > 10) and (".png" in i))]
    # limited to the user specified number of frames
    maps = maps[-MAP_FRAMES:]
    print(maps)
    # create a new figure with no ticks, black background
    fig, axis = plt.subplots(figsize=(12, 24), facecolor="black")
    plt.xticks([])
    plt.yticks([])
    fig.tight_layout()
    # begin animation loop
    images = []
    while True:
        # cache the images in the map list
        if not images:
            for png in maps:
                location = PATH + "latency_maps/" + png
                images.append(plt.imread(location))
        # get a latest map list from the directory
        new_maps = sorted(os.listdir(PATH + "latency_maps/"))
        new_maps = [i for i in new_maps if ((len(i) > 10) and (".png" in i))]
        new_maps = new_maps[-MAP_FRAMES:]
        # add any new images from the directory
        if maps != new_maps:
            print("updating live animation...")
            maps = [i for i in new_maps if i not in maps]
            for png in maps:
                location = PATH + "latency_maps/" + png
                images.append(plt.imread(location))
        # limit animation frames to window of latest data
        images = images[-MAP_FRAMES:]
        # plot, pause, clear, new map... repeat
        for image in images:
            axis.imshow(image, extent=[-180, 180, -90, 90])
            plt.pause(MAP_PAUSE)
            axis.clear()
        # refresh map list
        if maps != new_maps:
            maps = new_maps
            # stop animation of not a looping test
            if not LOOP:
                axis.imshow(images[-1], extent=[-180, 180, -90, 90])
                plt.show()


# TURN TERMINAL PRINTING ON AND OFF
# ######################################################################
def block_print():
    """
    temporarily disable printing terminal by diverting to devnull
    """
    if NOPRINT:
        sys.stdout = open(os.devnull, "w")


def enable_print():
    """
    re-enable printing to terminal
    """
    if NOPRINT:
        sys.stdout = sys.__stdout__


# LATENCY TESTING AND GEOLOCATION
# ######################################################################
def test_seeds():
    """
    Ping and GEOLOCATE seed nodes
    """
    seeds = []
    hosts = []
    cities = []
    if SEEDS:
        # scrape list of seed nodes from github:
        http = "https://"
        url = (
            "raw.githubusercontent.com/bitshares/"
            + "bitshares-core/master/libraries/app/application.cpp"
        )
        url = http + PROXY + url if PROXY_GITHUB else http + url
        ret = requests.get(url, timeout=(6, 30)).text
        ret = ret.replace(" ", "").replace(",", "").split("seeds={")[1].split("}")[0]
        ret = ret.split("//")
        ret = [item for item in ret if '"' in item]
        ret = [item.split('"')[1] for item in ret]
        ret = [item.split(":")[0] for item in ret]
        print(ret)
        seeds = []
        print("pinging and geolocating seed nodes...\n")
        for public_ip in ret:
            time.sleep(0.5)
            cmd = "ping -c 1 " + public_ip
            pong = os.popen(cmd).read()
            try:
                latency = int(pong.split("time=")[1].split(" ms")[0])
            except BaseException:
                latency = 0
            # some ips are not recognized by public_ip-api.com; substitute ipinfo.info
            # manually:
            if public_ip == "seeds.bitshares.eu":
                public_ip = "45.76.70.247"
            url = GEOLOCATE + public_ip
            try:
                ret = requests.get(url, headers={}, timeout=(6, 30)).text
                print(ret)
                ret = json_load(ret)
                entries_to_remove = (
                    "org",
                    "countryCode",
                    "timezone",
                    "region",
                    "status",
                    "zip",
                )
                try:
                    hosts.append(ret["as"] + " - " + ret["isp"])
                    cities.append(ret["city"] + ", " + ret["country"])
                except:
                    pass
                for entry in entries_to_remove:
                    ret.pop(entry, None)
                ret["public_ip"] = ret.pop("query")
                ret = (public_ip, latency, ret)
                print(ret)
                seeds.append(ret)
            except BaseException:
                print(traceback.format_exc())
        print("")
    return seeds, hosts, cities


async def ping(node):
    """Asynchronous version of your ping function."""
    rpc = None
    result = 222222  # Default return value in case of failure

    try:
        start = time.time()
        rpc = await wss_handshake(node)
        if rpc is None:
            return result  # Handshake failed
        
        ping_latency = time.time() - start
        chain = await rpc_chain_id(rpc)
        if chain is None:
            return result  # Chain ID retrieval failed

        blocktime, participation = await rpc_blocktime_participation(rpc)
        if blocktime is None or participation is None:
            return result  # Blocktime or participation retrieval failed

        block_latency = time.time() - blocktime

        if chain != ID:
            result = 333333
        elif participation < 90:
            result = 444444
        elif block_latency < (ping_latency + 10):
            result = ping_latency
        else:
            if TESTNET:
                result = ping_latency
            else:
                result = 111111

    except Exception as error:
        print(f"Error in ping for {node}: {str(type(error).__name__)} {str(error.args)}")

    finally:
        if rpc and not rpc.closed:
            await rpc.close()

    return result



def select_nodes():
    """
    Use user controls and bitsharesNODES.py to gather node lists
    """
    begin = time.time()
    since_181127 = []
    recent = []
    excluded = []
    universe = []
    apasia = []
    validated = []
    github_nodes = []
    repos = []
    only = Nodes.short_list()
    if ONLY:
        validated = only
    else:
        if APASIA:
            apasia = Nodes.apasia()
        if RECENT:
            recent = Nodes.recent()
        if EXCLUDE:
            excluded = Nodes.excluded()
        if UNIVERSE:
            universe = Nodes.universe()
        if SINCE_181127:
            since_181127 = Nodes.since_181127()
        if GITHUB or GITHUB_MASTER:
            print("searching github for nodes...")
            github_nodes, repos = scrape_github()
        validated += recent + apasia + since_181127 + universe + github_nodes
        if excluded:
            print(("remove %s known bad nodes" % len(excluded)))
            validated = [item for item in validated if item not in excluded]
        if not TESTNET:
            print("skipping known testnet nodes")
            validated = [item for item in validated if "test" not in item]

    validated = sorted(list(set(validate(parse(clean(validated))))))
    # gather list of nodes from github
    block_print()
    print("=====================================")
    print(("found %s nodes stored in script" % len(validated)))
    pprint(validated)
    # test triplicate; add /ws and /wss suffixes to all validated websockets
    validated = suffix(validated)
    validated = validated[:CROP1]
    # attempt to contact each websocket
    pinging = min(CROP1, len(validated))
    return pinging, validated, excluded, begin, repos


def geolocation(unique, pinged):
    """
    Use ip-api to get latitude and longitude for each node
    """
    ret = {}
    geo = []
    hosts = []
    cities = []
    
    # Batch size
    batch_size = 100
    
    # Process unique IPs in batches
    for i in range(0, len(unique), batch_size):
        batch = unique[i:i + batch_size]
        time.sleep(10)  # Sleep to avoid hitting rate limits
        
        if IPAPI:
            print("geolocating...")
            # Prepare the list of public IPs
            public_ips = []
            for item in batch:
                public_ip = (validate([item])[0])[6:]
                public_ip = public_ip.split(":")[0].split("/")[0]
                
                # Handle specific IP replacements
                ip_replacements = {
                    "freedom.bts123.cc": "121.42.8.104",
                    "ws.gdex.top": "106.15.82.97",
                    "bitshares.dacplay.org": "120.55.181.181",
                    "crazybit.online": "39.108.95.236",
                    "citadel.li": "37.228.129.75",
                    "bts.liuye.tech": "27.195.68.51",
                    "japan.bitshares.apasia.tech": "133.11.93.0"
                }
                public_ip = ip_replacements.get(public_ip, socket.gethostbyname(public_ip))
                public_ips.append(public_ip)

            try:
                req = requests.post(GEOLOCATE, json=public_ips, headers={}, timeout=(15, 30))
                # print(req.text)
                response_data = req.json()

                print(response_data)
                
                for index, ip_data in enumerate(response_data):
                    if 'status' in ip_data and ip_data['status'] != 'fail':
                        entries_to_remove = (
                            "org",
                            "countryCode",
                            "timezone",
                            "region",
                            "zip",
                        )
                        hosts.append(ip_data["as"] + " - " + ip_data["isp"])
                        cities.append(ip_data["city"] + ", " + ip_data["country"])
                        for entry in entries_to_remove:
                            ip_data.pop(entry, None)
                        ip_data["ip"] = ip_data.pop("query")
                        print(ip_data)
                        geo.append((pinged[i + index], ip_data))
            except Exception as e:
                print(f"Error fetching geolocation data: {e}")
                raise e
    
    return geo, hosts, cities



async def _async_spawn(pinging, validated):
    """Asynchronous version of spawn, using asyncio for concurrency."""
    pinged, timed, stale, expired, testnet, down, forked = [], [], [], [], [], [], []
    
    # Limit the number of concurrent ping tasks
    semaphore = asyncio.Semaphore(pinging)
    
    async def ping_with_semaphore(node):
        async with semaphore:
            try:
                return node, await asyncio.wait_for(ping(node), TIMEOUT)
            except asyncio.TimeoutError:
                return node, TIMEOUT  # Use a specific value to indicate timeout
            except Exception as e:
                print(f"Error pinging {node}: {e}")
                return node, 222222  # Use a specific value for other errors

    tasks = [ping_with_semaphore(node) for node in validated]
    results = await asyncio.gather(*tasks)

    for node, response_time in results:
        if response_time == -1:  # Assuming -1 means the node is down
            down.append(node)
        elif response_time == 111111:  # Head block is stale
            stale.append(node)
        elif response_time == 222222:  # Connect failed
            down.append(node)  # Already handled, but keeping for clarity
        elif response_time == 333333:  # Wrong chain ID
            testnet.append(node)
        elif response_time == TIMEOUT:  # Timeout reached
            expired.append(node)
        else:
            pinged.append(node)  # Connect success
            timed.append(response_time)  # Add response time
    
    # Sort websockets by latency
    pinged = [x for _, x in sorted(zip(timed, pinged))]
    timed = sorted(timed)
    
    return pinged, timed, stale, expired, testnet, down, forked


def spawn(pinging, validated):
    """Synchronous wrapper for the async spawn function."""
    try:
        # Run the async function using asyncio.run(), as in the execution results
        return asyncio.run(_async_spawn(pinging, validated))
    except RuntimeError as e:
        if "asyncio.run() cannot be called from a running event loop" in str(e):
            # If an event loop is already running (unlikely in a standard script), handle it
            print("Warning: Event loop issue detected. Ensure you're in a standard script.")
            loop = asyncio.get_event_loop()
            return loop.run_until_complete(_async_spawn(pinging, validated))
        else:
            raise  # Re-raise other errors


# def spawn(pinging, validated):
#     """
#     Launch timed subprocesses to test each node in validated list
#     """
#     pinged, timed, stale, expired, testnet, down, forked = [], [], [], [], [], [], []
#     for node in validated:
#         if len(pinged) < pinging:
#             # use multiprocessing module to enforce TIMEOUT
#             num = Value("d", 999999)
#             ping_process = Process(target=ping, args=(node, num))
#             ping_process.start()
#             ping_process.join(TIMEOUT)
#             if ping_process.is_alive() or (num.value > TIMEOUT):
#                 ping_process.terminate()
#                 ping_process.join()
#                 if num.value == 111111:  # head block is stale
#                     stale.append(node)
#                 elif num.value == 222222:  # connect failed
#                     down.append(node)
#                 elif num.value == 333333:  # wrong chain id
#                     testnet.append(node)
#                 elif num.value == 444444:
#                     forked.append(node)
#                 elif num.value == 999999:  # TIMEOUT reached
#                     expired.append(node)
#             else:
#                 pinged.append(node)  # connect success
#                 timed.append(num.value)  # connect success time
#             print(("ping:", ("%.2f" % num.value), node))
#             if TRACE_DETAIL:
#                 print("#" * 50, "\n\n\n")
#     # sort websockets by latency
#     pinged = [x for _, x in sorted(zip(timed, pinged))]
#     timed = sorted(timed)
#     return pinged, timed, stale, expired, testnet, down, forked


def thresh(previous_unique):
    """
    Seperate responding from non-responding nodes
    """
    if previous_unique:
        print("")
        print("previous unique list")
        print(previous_unique)
        print("")
    pinging, validated, excluded, begin, githubs = select_nodes()
    hosts = []
    cities = []
    if pinging:
        print("=====================================")
        enable_print()
        print(
            "%s pinging %s nodes; TIMEOUT %s sec; est %.1f minutes"
            % (time.ctime(), pinging, TIMEOUT, TIMEOUT * len(validated) / 120.0)
        )
        block_print()
        print("=====================================")
        pinged, timed, stale, expired, testnet, down, forked = spawn(pinging, validated)
        unknown = sorted(
            list(set(validated).difference(pinged + down + stale + expired + testnet))
        )
        unique = []
        speed = []
        for item, _ in enumerate(pinged):
            if pinged[item].strip("/ws") not in [j.strip("/ws") for j in unique]:
                unique.append(pinged[item])
                speed.append((pinged[item], int(timed[item] * 1000) / 1000.0))
                time.sleep(1)
        # get average speed of nested list
        speeds = [speed[i][1] for i, _ in enumerate(speed)]
        mean_speed = sum(speeds) / len(speeds)
        # report outcome
        print("")
        print(
                len(pinged),
                "of",
                len(validated),
                "nodes are active with latency less than",
                TIMEOUT,
        )
        print("")
        print(("fastest node", pinged[0], "with latency", ("%.2f" % timed[0])))
        print("mean latency", mean_speed)
        if excluded:
            for item, _ in enumerate(excluded):
                print(((item + 1), "EXCLUDED", excluded[item]))
        if unknown:
            for item, _ in enumerate(unknown):
                print(((item + 1), "UNTESTED", unknown[item]))
        if testnet:
            for item, _ in enumerate(testnet):
                print(((item + 1), "TESTNET", testnet[item]))
        if expired:
            for item, _ in enumerate(expired):
                print(((item + 1), "TIMEOUT", expired[item]))
        if stale:
            for item, _ in enumerate(stale):
                print(((item + 1), "STALE", stale[item]))
        if down:
            for item, _ in enumerate(down):
                print(((item + 1), "DOWN", down[item]))
        if forked:
            for item, _ in enumerate(forked):
                print(((item + 1), "FORKED", forked[item]))
        if pinged:
            for item, _ in enumerate(pinged):
                print(((item + 1), "GOOD PING", "%.2f" % timed[item], pinged[item]))
        if unique:
            for item, _ in enumerate(unique):
                print(((item + 1), "UNIQUE:", unique[item]))
        print("UNIQUE LIST:")
        print(unique)
        geo, _hosts, _cities = geolocation(unique, pinged)
        hosts += _hosts
        cities += _cities
        print("geo", geo)
    try:
        del excluded
        del unknown
        del testnet
        del expired
        del stale
        del down
        del forked
        del pinged
    except UnboundLocalError as error:
        print(error.args)
    print("")
    enable_print()
    image_url = ""
    elapsed = time.time() - begin
    print("elapsed:", "%.1f" % elapsed, "fastest:", ("%.3f" % timed[0]), unique[0])
    seeds, _hosts, _cities = test_seeds()
    hosts += _hosts
    cities += _cities
    hosts = sorted(list(set(hosts)))
    cities = sorted(list(set(cities)))
    print("HOSTING SERVICES")
    for host in hosts:
        print(host)
    print("CITIES")
    for city in cities:
        print(city)
    with open("servers.html", "w") as handle:
        handle.write(to_html(hosts))
        handle.close()
    with open("places.html", "w") as handle:
        handle.write(to_html(cities))
        handle.close()
    no_suffix = validate(Nodes.universe())
    if WRITE:
        race_write(doc="nodes.txt", text=str(unique))
    if PLOT:
        plot(geo, speed, mean_speed)
    if PLOT and SEEDS:
        plot_seed_nodes(seeds)
    if MAP_UPLOAD:
        image_url = upload_to_vgy()
    if API_UPLOAD:
        data = {
            "UNIVERSE": str(no_suffix),
            "COUNT": (str(len(unique)) + "/" + str(len(no_suffix))),
            "LIVE": str(unique),
            "PING": str(speed),
            "URLS": str(githubs),
            "GEO": str(geo),
            "SEEDS": str(seeds),
            "MAP_URL": str(image_url),
        }
        upload_to_jsonbin(data)

    if PLOT:
        save_figure()
    try:
        del timed
        del no_suffix
        del speed
        del geo
        del githubs
        del image_url
        del seeds
    except UnboundLocalError as error:
        print(traceback.format_exc())
    try:
        print("")
        print("live now, not live last round")
        print([i for i in unique if i not in previous_unique])
        print("live last round, not live now")
        print([i for i in previous_unique if i not in unique])
    except BaseException as error:
        print(traceback.format_exc())
    # update the previous unique list
    previous_unique = unique[:]
    del unique
    with open("nodes.html", "w") as handle:
        handle.write(to_html(previous_unique, font="mono", newlines="", size="0.9vw"))
        handle.close()
    with open("nodes.json", "w") as handle:
        handle.write(json_dump(previous_unique))
        handle.close()

    return previous_unique


# PRIMARY EVENT LOOP
# ######################################################################
def loop(logo):
    """
    Repeat latency test indefinitely
    """
    print("")
    previous_unique = []
    while True:
        print("\033c")
        print(logo)
        start = time.time()
        try:
            previous_unique = thresh(previous_unique)
            print("elapsed: ", (time.time() - start))
            time.sleep(REPEAT)
        # no matter what happens just keep verifying book
        except Exception as error:
            print(traceback.format_exc())
            print(type(error).__name__, error.args, error)
            time.sleep(1)


def update():
    """
    Run one latency test
    """
    print(
        "Acquiring low latency connection to Bitshares DEX"
        + ", this may take a few minutes..."
    )
    previous_unique = []
    while True:
        try:
            thresh(previous_unique)
            break
        # not satisfied until verified once
        except Exception as error:
            print(traceback.format_exc())
            print(type(error).__name__, error.args, error)
    asyncio.run(wss_handshake.session.close())
    wss_handshake.session = None


def main():
    """
    Primary Event Sequence
    """
    print("\033c")  # clear screen
    # Get logo and change terminal header
    logo = download_text("bitshares") + "\n\n"
    sys.stdout.write("\x1b]2;" + "Bitshares latencyTEST" + "\x07")
    if TRACE_DETAIL:
        websocket.enableTrace(True)
    # select mode
    if API_CREATE:
        create_jsonbin()
    if MAP_FRAMES:
        animation_process = Process(target=map_animate)
        animation_process.start()
    if LOOP:
        loop(logo)
    else:
        print("\033c")
        print(logo)
        update()


if __name__ == "__main__":
    main()
