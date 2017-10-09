# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
import urllib
import logging
from scrapy.exceptions import DropItem


logger = logging.getLogger('Sina_Fin_News')
class CrawlerPipeline(object):
    def process_item(self, item, spider):
        return item
class MongoPipeline(object):
    def __init__(self,mongo_uri,mongo_db,username,password):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.username = username
        self.password = password
        self.collection_name = 'sinafintop'

    @classmethod
    def from_crawler(cls,crawler):
        return cls(
            mongo_uri = crawler.settings.get('MONGO_URI'),
            mongo_db = crawler.settings.get('MONGO_DATABASE',),
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
        if (not item['content']) and (not item['summary']):
            valid = False
            raise DropItem('missing both content and summary of news from %s' %(item['url']))
        if valid:
            news = [{
            'url':item['url'],
            'title':item['title'],
            'date':item['date'],
            'summary':item['summary'],
            'content':item['content']
            }]
            self.db[self.collection_name].insert(news)
            logger.info('Item wrote to MongoDB database %s/%s' %
                (self.mongo_db,self.collection_name))
            return item

