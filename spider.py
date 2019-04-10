#!/usr/bin/env python
# -*- coding:utf-8 -*-
# 
# Created by liutongtong on 2018/7/5 01:08
#

import datetime
import logging
import pymysql
import queue
import random
import re
import threading
import time
import urllib.request

from bs4 import BeautifulSoup

from log import set_logging
from settings import MYSQL_CONFIG, ROBOT_USER_AGENTS


USER_AGENT_LIST = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/11.1.1 Safari/605.1.15',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:60.0) Gecko/20100101 Firefox/60.0'
]


class ProxySpider(object):
    logger = logging.getLogger(__name__ + '.ProxySpider')
    config = {
        'proxy_sources': ['xicidaili', 'kuaidaili', 'ip3336', 'data5u', '66ip'],
        'test_threads': 16,
        'test_timeout': 5,
        'test_cases': [
            # ('https://www.baidu.com/', '030173', 'utf-8'),
            ('http://dongqiudi.com/archives/1?page=10', '"tab_id":"1"', 'utf-8'),
        ]
    }

    def __init__(self, proxy_config=None):
        self.config.update(proxy_config or {})
        self.candidate_proxies = queue.Queue()
        self.verified_proxies = queue.Queue()

    def run(self):
        self.logger.info('Start ProxySpider...')
        self.crawl_proxies()
        self.verify_proxies()
        self.save_proxies()
        self.logger.info('ProxySpider Stopped...\n')

    def crawl_proxies(self):
        self.logger.info('Start crawling new proxies...')
        threads = []
        for proxy_source in self.config['proxy_sources']:
            t = ProxyCrawler(proxy_source, self.candidate_proxies)
            threads.append(t)
            t.start()
        for t in threads:
            t.join()

    def verify_proxies(self):
        self.logger.info('Start verifying new proxies...')
        threads = []
        for i in range(self.config['test_threads']):
            t = ProxyVerifier(self.candidate_proxies, self.verified_proxies,
                              self.config['test_cases'], self.config['test_timeout'])
            threads.append(t)
            t.start()
        self.candidate_proxies.join()
        for i in range(self.config['test_threads']):
            self.candidate_proxies.put(None)
        self.verified_proxies.put(None)

    def save_proxies(self):
        self.logger.info('Start saving new proxies...')
        saver = ProxySaver(self.verified_proxies)
        saver.run()


class ProxyCrawler(threading.Thread):
    logger = logging.getLogger(__name__ + '.ProxyCrawler')

    def __init__(self, proxy_source, candidate_proxies):
        super().__init__()
        self.proxy_source = proxy_source
        self.candidate_proxies = candidate_proxies
        self.logger.info('Start a new ProxyCrawler thread...')

    def run(self):
        getattr(self, 'crawl_proxies_from_' + self.proxy_source)()

    def crawl_proxies_from_xicidaili(self):
        source_url = 'http://www.xicidaili.com/nn/{}'
        self.logger.info('Start crawling proxies from xicidaili...')
        try:
            for page in range(1, 4):
                soup = self._get_soup(source_url.format(page))
                trs = soup.find('table', id='ip_list').find_all('tr')[1:]
                for tr in trs:
                    tds = tr.find_all('td')
                    ip = tds[1].text
                    port = tds[2].text
                    protocol = tds[5].text.lower()
                    self.candidate_proxies.put((ip, port, protocol))
        except Exception as e:
            self.logger.warning('Failed to crawl proxies from xicidaili. Exception: %s', e)

    def crawl_proxies_from_kuaidaili(self):
        source_url = 'https://www.kuaidaili.com/free/inha/{}'
        self.logger.info('Start crawling proxies from kuaidaili...')
        try:
            for page in range(1, 11):
                soup = self._get_soup(source_url.format(page))
                trs = soup.find('div', id='list').find_all('tr')[1:]
                for tr in trs:
                    tds = tr.find_all('td')
                    ip = tds[0].text
                    port = tds[1].text
                    protocol = tds[3].text.lower()
                    self.candidate_proxies.put((ip, port, protocol))
        except Exception as e:
            self.logger.warning('Failed to crawl proxies from kuaidaili. Exception: %s', e)

    def crawl_proxies_from_ip3336(self):
        source_url = 'http://www.ip3366.net/?stype=1&page={}'
        self.logger.info('Start crawling proxies from ip3336...')
        try:
            for page in range(1, 11):
                soup = self._get_soup(source_url.format(page))
                trs = soup.find('div', id='list').find_all('tr')[1:]
                for tr in trs:
                    tds = tr.find_all('td')
                    ip = tds[0].text
                    port = tds[1].text
                    protocol = tds[3].text.lower()
                    self.candidate_proxies.put((ip, port, protocol))
        except Exception as e:
            self.logger.warning('Failed to crawl proxies from ip3336. Exception: %s', e)

    def crawl_proxies_from_data5u(self):
        source_url = 'http://www.data5u.com/free/gngn/index.shtml'
        self.logger.info('Start crawling proxies from data5u...')
        try:
            soup = self._get_soup(source_url)
            uls = soup.find_all('ul', class_='l2')
            for ul in uls:
                lis = ul.find_all('li')
                ip = lis[0].text
                port = lis[1].text
                protocol = lis[3].text.lower()
                self.candidate_proxies.put((ip, port, protocol))
        except Exception as e:
            self.logger.warning('Failed to crawl proxies from data5u. Exception: %s', e)

    def crawl_proxies_from_66ip(self):
        source_url = 'http://www.66ip.cn/nmtq.php?getnum=300&isp=0&anonymoustype=3&' \
                     'start=&ports=&export=&ipaddress=&area=1&proxytype=2&api=66ip'
        self.logger.info('Start crawling proxies from 66ip...')
        try:
            soup = self._get_soup(source_url)
            contents = soup.find('body').text
            protocol = 'http'
            proxies = re.findall(r'\d+.\d+.\d+.\d+:\d+', contents)
            for proxy in proxies:
                ip, port = proxy.split(':')
                self.candidate_proxies.put((ip, port, protocol))
        except Exception as e:
            self.logger.warning('Failed to crawl proxies from 66ip. Exception: %s', e)

    def _get_soup(self, url):
        for i in range(5):
            try:
                request = urllib.request.Request(url)
                request.add_header("User-Agent", random.choice(ROBOT_USER_AGENTS))
                html = urllib.request.urlopen(request, timeout=10).read()
                return BeautifulSoup(html, 'lxml')
            except Exception as e:
                self.logger.warning('Failed to crawl html %s. Exception: %s', url, e)
                self.logger.info('Try again after 1s...')
                time.sleep(1)
        return None


class ProxyVerifier(threading.Thread):
    logger = logging.getLogger(__name__ + '.ProxyVerifier')

    def __init__(self, candidate_proxies, verified_proxies, test_cases, timeout):
        super().__init__()
        self.candidate_proxies = candidate_proxies
        self.verified_proxies = verified_proxies
        self.test_cases = test_cases
        self.timeout = timeout
        self.logger.info('Start a new ProxyVerifier thread...')

    def run(self):
        while True:
            elements = self.candidate_proxies.get()
            if elements is None:
                break
            ip, port, protocol = elements
            proxy = '{}://{}:{}'.format('http', ip, port)
            if self.verify_proxy(proxy):
                self.logger.info('Proxy %s is verified.', proxy)
                self.verified_proxies.put((ip, port, protocol))
            self.candidate_proxies.task_done()

    def verify_proxy(self, proxy):
        proxy_handler = urllib.request.ProxyHandler({'http': proxy, 'https': proxy})
        opener = urllib.request.build_opener(proxy_handler)
        opener.addheaders = [('User-Agent', random.choice(USER_AGENT_LIST))]
        try:
            for url, code, charset in self.test_cases:
                html = opener.open(url, timeout=self.timeout).read().decode(charset)
                if code not in html:
                    return False
            return True
        except Exception:
            return False


class ProxySaver(object):
    logger = logging.getLogger(__name__ + '.ProxySaver')
    insert_proxy_sql = 'INSERT INTO proxies (`ip`, `port`, `protocol`) VALUES (%(ip)s, %(port)s, %(protocol)s)'
    update_proxy_sql = 'UPDATE proxies SET ds=(%(ds)s) ' \
                       'WHERE `ip`=%(ip)s AND `port`=%(port)s AND `protocol`=%(protocol)s'

    def __init__(self, verified_proxies):
        self.conn = pymysql.connect(**MYSQL_CONFIG)
        self.verified_proxies = verified_proxies
        self.logger.info('Start a ProxySaver...')

    def run(self):
        while True:
            elements = self.verified_proxies.get()
            if elements is None:
                break
            ip, port, protocol = elements
            try:
                with self.conn.cursor() as cur:
                    cur.execute(self.insert_proxy_sql, {'ip': ip, 'port': port, 'protocol': protocol})
                self.conn.commit()
                self.logger.info('Proxy %s is inserted.', '{}://{}:{}'.format(protocol, ip, port))
            except pymysql.err.IntegrityError:
                with self.conn.cursor() as cur:
                    cur.execute(self.update_proxy_sql, {'ip': ip, 'port': port, 'protocol': protocol,
                                                        'ds': datetime.datetime.now()})
                self.conn.commit()
                self.logger.info('Proxy %s is updated.', '{}://{}:{}'.format(protocol, ip, port))
            self.verified_proxies.task_done()


if __name__ == '__main__':
    # import os
    # log_file = os.path.join(os.path.dirname(__file__), 'spider.log')
    # set_logging(log_file=log_file)
    set_logging()
    spider = ProxySpider()
    spider.run()
