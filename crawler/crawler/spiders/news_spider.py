
# -*- coding: utf-8 -*-
from scrapy.selector import Selector
from scrapy.spiders.crawl import Rule
from scrapy.http import Request
from scrapy.linkextractors import LinkExtractor
from scrapy_redis.spiders import RedisCrawlSpider
from crawler.items import NewsItem
import logging
logger = logging.getLogger('Sina_Fin_News')
class NewsSpider(RedisCrawlSpider):
	def __init__(self,*args,**kwargs):
		super(NewsSpider,self).__init__(*args,**kwargs)
		self.headers = {
			"Accept": "*/*",
		    "Accept-Encoding": "gzip,deflate",
		    "Accept-Language": "zh-CN,zh;q=0.8",
		    "Connection": "keep-alive",
		    "Content-Type":" application/x-www-form-urlencoded; charset=UTF-8",
		}
		self.cookies = {
			'UOR': r'www.baidu.com,finance.sina.com.cn,', 
			'U_TRS1': r'0000000e.a96d3944.5994f18d.99727aa5', 
			'SINAGLOBAL': r'220.248.57.42_1502933393.142171',
			'bdshare_firstime': 1503647427699,
			'SUB': r'_2AkMu6sbAf8NxqwJRmP0WyGrgbox3ywvEieKYtjcbJRMyHRl-yD9jqncDtRB7j_laUrNuHz9H2SfjnEapN4jmQw..', 
			'SUBP': r'0033WrSXqPxfM72-Ws9jqgMF55529P9D9WWu._VT6dURBXvUHNCLOu.M',
			'SR_SEL' : '1_511', 
			'SGUID': '1505299329457_719073', 
			'vjuids': r'-490d1dc99.15e7f077719.0.f614814ac8f1b', 
			'ArtiFSize': 14,
			'vjlast': 1505712892, 
			'Apache': '220.248.57.42_1505781786.324061', 
			'ULV': '1505781797348:30:18:6:220.248.57.42_1505781786.324061:1505781781842', 
			'afpCT': 1, 
			'rotatecount': 4, 
			'CNZZDATA1252916811': r'1773430241-1505720165-http%253A%252F%252Ffinance.sina.com.cn%252F%7C1505781702',
			'CNZZDATA5661630': r'cnzz_eid%3D291815867-1505718782-http%253A%252F%252Ffinance.sina.com.cn%252F%26ntime%3D1505779140',
			'CNZZDATA5399792': r'cnzz_eid%3D454494877-1505718554-http%253A%252F%252Ffinance.sina.com.cn%252F%26ntime%3D1505778347', 
			'lxlrttp': 1504173940,
		}
		self.website_possible_httpstatus_list = [304,302]
		self.handle_httpstatus_list = [301,302,204,206,404,500]
	name = "SinaFinNews"
	allowed_domains = ["finance.sina.com.cn"]
	redis_key = 'SinaFinaTop:start_urls'
	rules = [
		Rule(LinkExtractor(allow = (r"^http://top.finance.sina.com.cn/ws/GetTopDataList.php\?"), tags = ('script'), attrs=('src')), callback='parse_url'),
		Rule(LinkExtractor(allow = (r"^http://finance.sina.com.cn/([^#]*)+$")), callback = 'parse_item')
		]
	#处理返回的str格式response获取其中的news url 加入队列
	def parse_url(self,response):
		#处理str格式response，首先去除尾部的换行符和分好
		false = ''
		true = ''
		null = ''
		text = response.body.strip().strip(';')
		#去除头部的变量名获得dict形式的str数据
		pos = text.find("conf")
		text = text[pos-2:]
		#转化str2dict
		text = eval(text)
		#循环dict数据获取news url 并生成新的request加入队列
		data = text['data']
		for i in range(len(data)):
			url = data[i]['url']
			url = ''.join(url.split('\\'))
			headers = self.headers
			headers['referer'] = response.url
			#headers["referer"] = response.url
			yield Request(url,method='GET',callback='parse_item',headers = headers,cookies = self.cookies)

	def parse_item(self,response):
		if response.status == 302:
			url = response.url
			headers = self.headers
			headers['referer'] = response.url
			req = Request(url,method='GET',callback='parse_item',headers = headers,cookies = self.cookies)
			req.meta["change_proxy"] = True
			yield req
		elif (response.status ==403) or (response.status == 404):
			req = response.request
			req.meta["change_proxy"] = True
			logger.info("Error code%d",response.status)
			yield req
		else:
			logger.info("Parse function called on %s", response.url)
			sel = Selector(response)
			item = NewsItem()
			item['url'] = response.url
			d = sel.xpath('//*[@class="time-source" or @id="pub_date"]/text()').extract()
			date = ''
			for m in range(len(d)):
				date = date + d[m].strip()
			item['date'] = date
			t = sel.xpath('//*[@id="artibodyTitle"]/text()').extract()
			title = ''
			for n in range(len(t)):
				title = title +t[n].strip()
			item['title'] = title
			if not title:
				req = response.request
				req.meta["change_proxy"] = True
				logger.info("Error code%d,but title not found",response.status)
				yield req
			else:
				paragraph = sel.xpath('//*[@id="artibody"]')
				summary = ''
				content = ''
				for p in paragraph.xpath('.//p'):
					s = p.xpath('.//span/text()').extract()
					c = p.xpath('.//text()').extract()
					for i in range(len(s)):
						summary = summary + s[i].strip()
					for j in range(len(c)):
						content = content + c[j].strip()
				summary.replace(u'\ax0',u' ')
				content.replace(u'\ax0',u' ')
				item['content'] = content
				item['summary'] = summary
				yield item

