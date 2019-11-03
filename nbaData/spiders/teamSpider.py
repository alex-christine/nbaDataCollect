import scrapy
import json
import datetime
import pandas as pd
from nbaData.items import teamItem
from scrapy.loader import ItemLoader


class teamSpider(scrapy.Spider):

    name = 'teams'

    def start_requests(self):
        urls = ['https://data.nba.net/prod/v2/{}/teams.json'.format(datetime.datetime.today().year)]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)


    def parse(self, response):
        teams = json.loads(response.body).get('league').get('standard')
        
        def createTeamObject(team_info):
            loader = ItemLoader(item = teamItem())

            loader.add_value('idTeam', team_info.get('teamId'))
            loader.add_value('teamCity', team_info.get('city'))
            loader.add_value('teamName', team_info.get('nickname'))
            loader.add_value('teamCode', team_info.get('tricode'))
            loader.add_value('conference', team_info.get('confName'))
            loader.add_value('division', team_info.get('divName'))

            team_object = loader.load_item()

            return team_object

        for team in teams:
            if team.get('isNBAFranchise') == True:
                teamLoaded = createTeamObject(team)
                yield teamLoaded

        