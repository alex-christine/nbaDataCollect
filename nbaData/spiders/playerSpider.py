import scrapy
import json
import datetime
from nbaData.items import playerItem
from scrapy.loader import ItemLoader

class playerSpider(scrapy.Spider):

    name = 'players'

    def start_requests(self):
        urls = ['https://www.nba.com/players/active_players.json']

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)


    def createPlayerObject(self, player_info):
        loader = ItemLoader(item = playerItem())

        # Gets the team tricode out of the dataset
        teamData = player_info.get('teamData')
        triCode = teamData.get('tricode')

        loader.add_value('playerId', player_info.get('personId'))
        loader.add_value('firstName', player_info.get('firstName'))
        loader.add_value('lastName', player_info.get('lastName'))
        loader.add_value('heightFt', player_info.get('heightFeet'))
        loader.add_value('heightIn', player_info.get('heightInches'))
        loader.add_value('weightLbs', player_info.get('weightPounds'))
        loader.add_value('teamTri', triCode)
        loader.add_value('position', player_info.get('pos'))
        loader.add_value('allStar', player_info.get('isAllStar'))

        player_object = loader.load_item()

        return player_object

        


    def parse(self, response):
        playersResp = json.loads(response.body)
        
        for player in playersResp:
            yield self.createPlayerObject(player)