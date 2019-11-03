import scrapy
import json
import datetime
from nbaData.items import scheduleGame
from scrapy.loader import ItemLoader

class scheduleSpider(scrapy.Spider):

    name = 'schedule'

    def start_requests(self):
        urls = ['https://data.nba.com/data/10s/v2015/json/mobile_teams/nba/{0}/league/00_full_schedule_week.json'.format(datetime.datetime.today().year)]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)


    def parse(self, response):
        schedule = json.loads(response.body).get('lscd')

        def createGameObject(game_info):
            home = game_info.get('h')
            away = game_info.get('v')

            loader = ItemLoader(item = scheduleGame())
            loader.add_value('idGame', game_info.get('gid'))
            loader.add_value('gameDate', game_info.get('gdte'))
            loader.add_value('gameTime', game_info.get('etm'))
            loader.add_value('gameArena', game_info.get('an'))
            loader.add_value('gameCity', game_info.get('ac'))
            loader.add_value('gameState', game_info.get('as'))
            loader.add_value('idTeamA', away.get('tid'))
            loader.add_value('aRecord', away.get('re'))
            loader.add_value('aTeam', away.get('ta'))
            loader.add_value('aScore', away.get('s'))
            loader.add_value('idTeamH', home.get('tid'))
            loader.add_value('hRecord', home.get('re'))
            loader.add_value('hTeam', home.get('ta'))
            loader.add_value('hScore', home.get('s'))

            game_object = loader.load_item()

            return game_object

        for month in schedule:
            sch = month.get('mscd').get('g')


            for game in sch:
                if int(game.get('gid')) > 21900000:
                    schedule_game = createGameObject(game)
                    yield schedule_game
