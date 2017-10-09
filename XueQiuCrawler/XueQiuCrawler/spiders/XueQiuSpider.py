# -*- coding: utf-8 -*-
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy.spiders.crawl import Rule
from scrapy.linkextractors import LinkExtractor
from scrapy_redis.spiders import RedisCrawlSpider
from XueQiuCrawler.items import XueQiuItem
from scrapy_splash import SplashRequest

from datetime import datetime

import redis
import random
import requests
import logging

logger = logging.getLogger('XueQiu_Fin_News')
class NewsSpider(RedisCrawlSpider):
    def __init__(self,*args,**kwargs):
        super(NewsSpider,self).__init__(*args,**kwargs)
        self.headers = {
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.78 Safari/537.36',
        }
        self.cookies = {
            'xq_a_token':'ed965d6ca0f68aa2f0b4a80a510e86fe5c02784d', 
        }
        self.website_possible_httpstatus_list = [304,302]
        self.handle_httpstatus_list = [301,302,204,206,404,500,504,400]
        self.proxy_failed = {}
        self.pool = redis.ConnectionPool(host='localhost', port=6379)
        self.r = redis.Redis(connection_pool=self.pool)
        self.proxy = self.r.hgetall('useful_proxy').keys()
    
    name = "XueQiuNews"
    allowed_domains = ["xueqiu.com"]
    redis_key = 'XueQiuTop:start_urls'


    def change_proxy(self,req):
        if len(self.proxy) <= 10:
            self.proxy = self.r.hgetall('useful_proxy').keys()
        try :
            req.meta['retry']
        except:
            req.meta['retry'] = 0

        if req.meta['retry']>3:
            req.dont_filter = False
        else: 
            if 'splash' in req.meta.keys():
                try: 
                    proxy = req.meta['splash']['args']["proxy"]
                    if proxy in self.proxy_failed:
                        self.proxy_failed[proxy]+=1
                    else:
                        self.proxy_failed[proxy]=1
                    if self.proxy_failed[proxy]==1:
                        logger.info('invalid or banned proxy %s was deleted',proxy)
                        self.proxy.remove(proxy)
                        self.r.hdel('useful_proxy',proxy)
                    new_proxy = 'http://' + random.choice(self.proxy)
                    logger.info('using proxy %s',new_proxy)
                    req.meta['splash']['args']["proxy"] = new_proxy
                    del req.meta['splash']['_replaced_args']
                    req.dont_filter = True
                    req.meta['retry']+=1
                    return req
                except:
                    new_proxy = 'http://' + random.choice(self.proxy)
                    logger.info('using proxy %s',new_proxy)
                    req.meta['splash']['args']["proxy"] = new_proxy
                    req.dont_filter = True
                    req.meta['retry'] = 1
                    return req
            else:
                try: 
                    proxy = req.meta["proxy"]
                    if proxy in self.proxy_failed:
                        self.proxy_failed[proxy]+=1
                    else:
                        self.proxy_failed[proxy]=1
                    if self.proxy_failed[proxy]==2:
                        logger.info('invalid or banned proxy %s was deleted',proxy)
                        self.proxy.remove(proxy)
                        self.r.hdel('useful_proxy',proxy)
                    new_proxy = 'http://' + random.choice(self.proxy)
                    logger.info('using proxy %s',new_proxy)
                    req.meta["proxy"] = new_proxy
                    req.meta["retry"] += 1
                    req.dont_filter = True
                    return req
                except:
                    new_proxy = 'http://' + random.choice(self.proxy)
                    logger.info('using proxy %s',new_proxy)
                    req.meta["proxy"] = new_proxy
                    req.dont_filter = True
                    req.meta['retry'] = 1
                    return req
            '''proxy = req.meta["proxy"]
            if proxy in self.proxy_failed:
                self.proxy_failed[proxy]+=1
            else:
                self.proxy_failed[proxy]=1
            if self.proxy_failed[proxy]==2:
                logger.info('invalid or banned proxy %s was deleted',proxy)
                self.proxy.remove(proxy)
                self.r.hdel('useful_proxy',proxy)
            new_proxy = 'http://' + random.choice(self.proxy)
            logger.info('using proxy %s',new_proxy)
            req.meta["proxy"] = new_proxy
            del req.meta['splash']['_replaced_args']'''
 

    def start_request_from_data(self,data):
        url = bytes_to_str(data, self.redis_encoding)
        return self.make_requests_from_url(url)
    
    def make_requests_from_url(self,url):
        req = Request(url,method='GET',callback='parse',headers = self.headers,cookies = self.cookies)
        return req
    #处理返回的str格式response获取其中的news url 加入队列
    def parse(self,response):
        script = """
        function main(splash,args)
            splash.images_enabled = False
            assert(splash:go{
                args.url,
                headers = args.headers,
            })
            assert(splash:wait(args.wait))
            return {
                html = splash:html(),
            }
        end
        """
        false = ''
        true = ''
        null = ''
        #处理str格式response，首先去除尾部的换行符
        text = response.body.strip()
        #转化str2dict
        try:
            text = eval(text)
            #循环dict数据获取news url 并生成新的request加入队列
            lis = text['list']
            for i in range(len(lis)):
                data = eval(lis[i]['data'])
                url = 'https://xueqiu.com' + data['target'].strip('\\')
                date = int(str(data['created_at'])[0:10])
                date = datetime.fromtimestamp(date)
                date = date.strftime('%Y%m%d')
                title = data['topic_title'].decode('utf-8')
                proxy = 'http://' + random.choice(self.proxy)
                yield SplashRequest(url,callback='parse_item',endpoint='execute',args={'lua_source':script,'wait':0.1,},headers=self.headers,cookies=self.cookies,meta={'date':date,'title':title})
            para1 = text['next_max_id']
            para2 = text['next_id']
            logger.info('id%s,nextid%s'%(para2,para1))
            tol_proxy = 'http://' + random.choice(self.proxy)
            next_url = 'https://xueqiu.com/v4/statuses/public_timeline_by_category.json?'+'id='+str(para2)+'&max_id='+str(para1)+'&count=20&category=105'
            req = Request(next_url,method='GET',callback='parse',headers = self.headers,cookies = self.cookies,)
            yield req
        except:
            req = response.request
            logger.info('%s',req.meta)
            new_req = self.change_proxy(req)
            logger.info("Banned%d")
            yield new_req
    
    def parse_item(self,response):
        script = """
        function main(splash,args)
            splash.images_enabled = False
            assert(splash:go{
                args.url,
                headers = args.headers,
            })
            assert(splash:wait(args.wait))
            return {
                html = splash:html(),
            }
        end
        """
        if (response.status != 200) and (response.status not in self.website_possible_httpstatus_list):
            req = response.request
            new_req = self.change_proxy(req)
            logger.info("Error code%d,url%s"%(response.status,response.url))
            yield new_req
        else:
            logger.info("Parse function called on %s", response.url)
            try:
                sel = Selector(text = response.data['html'])
                item = XueQiuItem()
                item['url'] = response.url
                date = response.request.meta['date']
                item['date'] = date
                if not date:
                    req = response.request
                    logger.info('%s'%response.data)
                    new_req = self.change_proxy(req)
                    logger.info("Error code%d,but date not found",response.status)
                    yield new_req
                title = response.request.meta['title']
                item['title'] = title
                if not title:
                    req = response.request
                    logger.info('%s'%response.data)
                    logger.info("Error code%d,but title not found",response.status)
                    new_req = self.change_proxy(req)
                    yield new_req
                else:
                    paragraph = sel.xpath('//*[@class="status-content"]/div[@class="detail"] |//*[@class="status__content status__content--textoverflow"]')
                    content = ''
                    for c in paragraph.xpath('.//text()'):
                        cont = c.extract()
                        for j in range(len(cont)):
                            content = content + cont[j].strip()
                    content.replace(u'\ax0',u' ')
                    item['content'] = content
                    logger.info("%s pulished in %s" %(title,date))
                    yield item
            except:
                req = response.request
                logger.info("Error code%d,content %s"%(response.status,response.body))
                new_req = self.change_proxy(req)
                yield req

