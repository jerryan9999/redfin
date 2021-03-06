# -*- coding: utf-8 -*-

# Scrapy settings for redfin project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#     http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html
#     http://scrapy.readthedocs.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'redfin'

SPIDER_MODULES = ['redfin.spiders']
NEWSPIDER_MODULE = 'redfin.spiders'


# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT =  "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36"

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 24

# Configure a delay for requests for the same website (default: 0)
# See http://scrapy.readthedocs.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
DOWNLOAD_DELAY = 0.5
# The download delay setting will honor only one of:
CONCURRENT_REQUESTS_PER_DOMAIN = 24
CONCURRENT_REQUESTS_PER_IP = 1

# Disable cookies (enabled by default)
COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
#   'Connectin': 'keep-alive',
#   'Host': 'www.redfin.com',
#   'Accept-Encoding':'gzip, deflate, sdch, br'#

#}

# Download timeout value
DOWNLOAD_TIMEOUT = 60      # default timeout is 3mins(180s)

# Enable or disable spider middlewares
# See http://scrapy.readthedocs.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    'redfin.middlewares.RedfinSpiderMiddleware': 543,
#}

LOGSTATS_INTERVAL = 60.0
IPPOOLSTATS_INTERVAL = 180.0

# Enable or disable downloader middlewares
# See http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
  'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware':None,
  'redfin.downloadermiddlewares.rotate_useragent.RotateUserAgentMiddleware':530,
  'redfin.downloadermiddlewares.rotateproxy.TopProxyMiddleware':760,
  'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 790,
  'redfin.downloadermiddlewares.http2https.HttpschangeMiddleware':793,
}

# Enable or disable extensions
# See http://scrapy.readthedocs.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
#}

# Configure item pipelines
# See http://scrapy.readthedocs.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    'redfin.pipelines.RedfinRoomPipeline' : 400
}

EXTENSIONS = {
  'redfin.extensions.logstats.IpLogStats':100,
}

RETRY_ENABLED = True
RETRY_TIMES = 3  # initial response + 3 retries = 4 requests
RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 404]
RETRY_PRIORITY_ADJUST = -1

# Enable and configure the AutoThrottle extension (disabled by default)
# See http://doc.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = False
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = 'httpcache'
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'

# Logging 
import time
LOG_LEVEL = 'INFO'   # default: 'DEBUG'
LOG_FILE = "logs/scrapy_%s_%s.log"%(BOT_NAME, int(time.time()))

import yaml
with open("config.yml") as f:
  CONFIG = yaml.load(f)
