import requests
import time
import Queue
import threading
import logging
import json
import signal
import sys

from logging.config import fileConfig
from pyquery import PyQuery as pq

fileConfig('logging_config.ini')
logger = logging.getLogger()
global IS_TERM

BASE_URL = "http://my.oschina.net"
SIMU_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36'
COUNT = 5

headers = {'User-Agent': SIMU_AGENT}


def signal_handler(signal, frame):
    global IS_TERM
    print 'You pressed Ctrl+C!'
    IS_TERM = True
    sys.exit(1)


def get_relations(basedoc):
    result = list()
    try:
        doc = basedoc
        fans = doc.find(".fans-item")
        flist = [fans.eq(i) for i in range(len(fans))]

        while True:
            for fan in flist:
                username = fan.find(".username").text()
                url = fan.find(".username a").attr("href")
                result.append({"name": username, "link": url})

            pages = doc.find("#friend-page-pjax a")

            if len(pages) == 0:
                break

            last_link = pages.eq(len(pages) - 1).attr("href")
            last_number = pages.eq(len(pages) - 1).text()

            if last_number.encode("utf-8").isdigit():
                break

            r = requests.get(BASE_URL + "/" + last_link, headers=headers)
            doc = pq(r.text)
            fans = doc.find(".fans-item")
            flist = [fans.eq(i) for i in range(len(fans))]

        return result
    except Exception as e:
        return result


def get_user_info(url):
    try:
        r = requests.get(url + "/fans", headers=headers)
        doc = pq(r.text)
        user_info = dict()

        # get user information
        user_info["url"] = url
        user_info["nickname"] = doc.find(".user-info .nickname").text()
        user_info["post"] = doc.find(".user-info .post").text()
        user_info["address"] = doc.find(".user-info .address").text()
        user_info["score"] = doc.find(".integral .score-num").text()
        user_info["fans_number"] = doc.find(".fans .score-num").text()
        user_info["follow_number"] = doc.find(".follow .score-num").text()
        join_time = doc.find(".join-time").text()
        user_info["join_time"] = join_time[
            4:15].encode("ascii", "ignore").strip()

        # get fans
        user_info["fans"] = get_relations(doc)

        # get follows, fellow is a wrong spelling
        rf = requests.get(url + "/fellow", headers=headers)
        user_info["follow"] = get_relations(pq(rf.text))

        return user_info
    except Exception as e:
        return None


class Scarper(threading.Thread):
    def __init__(self, threadName, queue):
        super(Scarper, self).__init__(name=threadName)
        self._stop = False

        self._base_url = BASE_URL
        self._base_user = "masokol"
        self._check_point = dict()
        self._task_queue = queue

    def _pull(self, url):
        user = get_user_info(url)
        if user is None:
            return

        self._write(user)
        for u in user["fans"]:
            logger.debug("check a fan {}".format(json.dumps(u)))
            if not self._check_point.has_key(u["link"]):
                logger.debug("put one task {}".format(u["link"]))
                self._task_queue.put(u)
        for u in user["follow"]:
            logger.debug("check a follow {}".format(json.dumps(u)))
            if not self._check_point.has_key(u["link"]):
                logger.debug("put one task {}".format(u["link"]))
                self._task_queue.put(u)

    def _write(self, user):
        if self._check_point.has_key(user["url"]):
            return
        logger.debug(json.dumps(user))

        # TODO support unicode logging here
        logger.info("name={}, join={}, post={}, address={}, score={}, fans_number={}, follow_number={}".format(
            user["nickname"].encode("utf8"),
            user["join_time"],
            user["post"].encode("utf8"),
            user["address"].encode("utf8"),
            user["score"].encode("utf8"),
            user["fans_number"].encode("utf8"),
            user["follow_number"].encode("utf8")))
        self._check_point[user["url"]] = True

    def init(self):
        url = self._base_url + "/" + self._base_user
        r = requests.get(url, headers=headers)
        self._pull(url)

    def run(self):
        global IS_TERM
        logger.debug("start working")

        try:
            while True:
                logger.debug("pull one task ...")
                item = self._task_queue.get(False)
                logger.debug("get one task {} ".format(json.dumps(item)))
                self._pull(item["link"])
                time.sleep(0.1)
                if IS_TERM:
                    break
        except KeyboardInterrupt:
            sys.exit()
        except Exception as e:
            print e


def main():
    global IS_TERM

    IS_TERM = False
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    queue = Queue.Queue()
    worker = Scarper("oschina_scraper", queue)
    worker.init()
    worker.start()

    for i in range(COUNT):
        puller = Scarper("oschina_scraper_{}".format(i), queue)
        puller.start()

    # http://stackoverflow.com/questions/4136632/ctrl-c-i-e-keyboardinterrupt-to-kill-threads-in-python
    while True:
        worker.join(600)
        if not worker.isAlive():
            break

main()
