import random
import json
import redis
from useragents import agents
from scrapy.downloadermiddlewares.useragent import UserAgentMiddleware
from scrapy.downloadermiddlewares.retry import RetryMiddleware

class ModifiedUserAgentMiddleware(UserAgentMiddleware):
    def __init__(self,*args,**kwargs):
        super(ModifiedUserAgentMiddleware,self).__init__(*args,**kwargs)

    def process_request(self,request,spider):
        self.user_agent = random.choice(agents)
        request.headers["User-Agent"] = self.user_agent

