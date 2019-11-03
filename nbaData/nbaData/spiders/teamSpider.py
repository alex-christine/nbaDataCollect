mport scrapy
import json
import datetime
from nbaData.items import scheduleGame
from scrapy.loader import ItemLoader

class scheduleSpider(scrapy.Spider):

    name = 'teams'

    def start_requests(self):
        urls = ['https://data.nba.net/prod/v2/2019/teams.json'.format(datetime.datetime.today().year)]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)


    def parse(self, response):
        # TODO make this do stuff