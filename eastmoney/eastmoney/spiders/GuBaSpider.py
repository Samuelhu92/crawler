#-*- coding: utf-8 -*-
from scrapy.selector import Selector
from scrapy.spiders.crawl import Rule
from scrapy.http import Request
from scrapy_splash import SplashRequest
from scrapy.linkextractors import LinkExtractor
from scrapy_redis.spiders import RedisCrawlSpider
from eastmoney.items import PostItem 

import redis
import random
import logging

logger = logging.getLogger('GuBa_Post')

class GuBaSpider(RedisCrawlSpider):
    def __init__(self,*args,**kwargs):
        super(GuBaSpider,self).__init__(*args,**kwargs)
        self.headers = {

        }
        self.cookies = {

        }
        self.website_possible_httpstatus_list = [304,302]
        self.handle_httpstatus_list = [301,302,204,206,404,500,504]
        self.proxy_failed = {}
        self.pool = redis.ConnectionPool(host='localhost', port=6379)
        self.r = redis.Redis(connection_pool=self.pool)
        self.proxy = self.r.hgetall('useful_proxy').keys()

    name = "GuBaPost"
    allowed_domains = ["guba.eastmoney.com"]
    redis_key = 'GuBaPost:start_urls'

    def change_proxy(self,req):
        if len(self.proxy) <= 10:
            self.proxy = self.r.hgetall('useful_proxy').keys()
        try :
            req.meta['retry']
        except:
            req.meta['retry'] = 0           
        if req.meta['retry'] > 3:
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


    def parse(self,response):
        try:
            sel = Selector(response)
        except:
            req = response.request
            logger.info('%s',req.meta)
            new_req = self.change_proxy(req)
            logger.info("Banned%s",response.url)
            yield new_req
        else:
            logger.info('Parsing post list on %s',response.url)
            urllist = sel.xpath('//ul[@class = "newlist"]//li')
            for l in urllist:
                url = 'http://guba.eastmoney.com' + l.xpath('.//a[2]/@href').extract()[0].split('.html')[0]+'_1.html'
                title = l.xpath('.//a[2]/@title').extract()[0]
                #logger.info('store %s,%s'%(url,title))
                yield Request(url,callback='parse_item',meta={'title':title})

    def parse_item(self,response):
        if (response.status != 200) and (response.status not in self.website_possible_httpstatus_list):
            req = response.request
            new_req = self.change_proxy(req)
            logger.info("Error code%d,url%s"%(response.status,response.url))
            yield new_req
        else:
            logger.info('Parsing post on %s',response.url)
            isitem = False
            isitem_new = False
            try:
                sel = Selector(response)
            except:
                req = response.request
                new_req = self.change_proxy(req)
                logger.info("page not in foramt on %s",response.url)
            else:
                if (sel.xpath('//*[@class = "stockcodec"]')) or (sel.xpath('//*[@class = "zwlitext stockcodec"]')):
                    item = PostItem()
                    try:
                        date = sel.xpath('//div[@class = "zwfbtime"]/text()').extract()[0].split(' ')[1]
                        author = sel.xpath('//*[@id="zwconttbn"]/strong/a/text()').extract()
                        content = ''
                        paragraph = sel.xpath('//*[@class = "stockcodec"]')
                        for p in paragraph:
                            c = p.xpath('.//text()').extract()
                            for j in range(len(c)):
                                content = content + c[j].strip()
                        title = response.request.meta['title']
                        item['url'] = response.url
                        item['title'] = title
                        item['content'] = content
                        item['date'] = date
                        item['author'] = author
                        logger.info("post %s pulished in %s" %(title,date))
                        isitem = True
                        yield item
                    except:
                        pass
                    finally:
                        comments = sel.xpath('//*[@class = "zwlitxt"]')
                        for c in comments:
                            item_new = PostItem()
                            date_new = c.xpath('.//*[@class = "zwlitime"]/text()').extract()
                            date_new = date_new[0].split(' ')[1]
                            author_new = c.xpath('.//*[@class="zwnick"]/a/text()').extract()
                            contents = c.xpath('.//*[@class = "zwlitext stockcodec"]')
                            item_new['url'] = response.url
                            item_new['author'] = author_new
                            item_new['date'] = date_new
                            item_new['title'] = response.request.meta['title']
                            content_new=''
                            if contents.xpath('.//img'):
                                emoj = contents.xpath('.//img/@title').extract()
                                for e in emoj:
                                    content_new = content_new+e
                            else:
                                for cont in contents:
                                    content_new = content_new + cont.xpath('.//text()').extract()[0].strip()
                            item_new['content'] = content_new
                            logger.info("comments %s pulished in %s" %(content_new,date_new))
                            isitem_new = True
                            yield item_new
                        #防止实际中出现不存在的评论页面但是存在名为‘stockcodec’的class从而避开最初的检验产生无穷新页
                        if isitem or isitem_new:
                            page = int(response.url.split('_')[1].split('.')[0])
                            new_page = page+1
                            new_url = response.url.split('_')[0]+'_'+str(new_page)+'.html'
                            yield Request(new_url,callback='parse_item',meta={'title':response.request.meta['title']})
                            logger.info('store %s',new_url)
                else:
                    pass
           





