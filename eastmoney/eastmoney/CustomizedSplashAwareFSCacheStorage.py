# -*- coding: utf-8 -*-
from __future__ import absolute_import
from scrapy_splash.cache import SplashAwareFSCacheStorage
from eastmoney.SplashAwareDupeFilter import splash_request_fingerprint 
import os 

class CustomizedSplashAwareFSCacheStorage(SplashAwareFSCacheStorage):
    def _get_request_path(self, spider, request):
        key = splash_request_fingerprint(request)
        return os.path.join(self.cachedir, spider.name, key[0:2], key)