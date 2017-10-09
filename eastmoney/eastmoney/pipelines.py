# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
import urllib
import logging
import hashlib
from scrapy.exceptions import DropItem
from eastmoney.BloomFilter import BloomFilter 
from scrapy.utils.request import request_fingerprint
from scrapy.utils.python import to_bytes
from w3lib.url import canonicalize_url

logger = logging.getLogger('GuBa_Post')
class EastmoneyPipeline(object):
    def process_item(self, item, spider):
        return item
class MongoPipeline(object):
    def __init__(self,mongo_uri,mongo_db,username,password):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.username = username
        self.password = password
        self.collection_name = 'gubapost'

    @classmethod
    def from_crawler(cls,crawler):
        return cls(
            mongo_uri = crawler.settings.get('MONGO_URI'),
            mongo_db = crawler.settings.get('MONGO_DATABASE'),
            username = urllib.quote_plus(crawler.settings.get('MONGO_USER')),
            password = urllib.quote_plus(crawler.settings.get('MONGO_PWD'))
            )

    def open_spider(self,spider):
        try:
            self.client = pymongo.MongoClient(
                self.mongo_uri,
                username = self.username,
                password = self.password
                )
            self.db = self.client[self.mongo_db]
        except Exception as err:
            print(str(err))

    def close_spider(self,spider):
        self.client.close()

    def process_item(self,item,spider):
        valid = True
        if not item['content']:
            valid = False
            raise DropItem('missing content of post on %s',item['url'])
        if valid:
            post = [{
            'url':item['url'],
            'title':item['title'],
            'date':item['date'],
            'content':item['content']
            }]
            self.db[self.collection_name].insert(post)
            logger.info('Item wrote into MongoDB database %s/%s'%
                (self.mongo_db,self.collection_name))
            return item
            
#使用布隆过滤将item进行过滤，排除已经储存的item，代替url去重进行增量爬取
class DupeFilterPipeline(object):
    def __init__(self,redis_host,redis_port,filter_db,filter_key):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.filter_db = filter_db
        self.filter_key = filter_key
        self.bf = BloomFilter(redis_host,redis_port,filter_db,blockNum=1,key=filter_key)

    @classmethod
    def from_crawler(cls,crawler):
        return cls(
            redis_host=crawler.settings.get('REDIS_HOST'),
            redis_port=crawler.settings.get('REDIS_PORT'),
            filter_db=crawler.settings.get('FILTER_DB'),
            filter_key=crawler.settings.get('FILTER_KEY')
            )
    def item_seen(self,item):
        fp = hashlib.sha1()
        fp.update(to_bytes(item['content']))
        fp.update(to_bytes(item['author']))
        fp.update(to_bytes(canonicalize_url(item['url'])))
        fp = fp.hexdigest()
        if self.bf.isContains(fp):
            return True
        else:
            self.bf.insert(fp)
            return False
    def process_item(self,item):
        if item_seen(item):
            raise DropItem('already parsed content dropped %s',item['content'])
        else:
            return item

        

