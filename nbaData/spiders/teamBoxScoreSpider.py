import scrapy
import json
import datetime
import pandas as pd
import numpy as np
import psycopg2
from pandas.io.json import json_normalize
from nbaData.items import teamBoxScore
from scrapy.loader import ItemLoader
from nbaData.pipelines import sqlDatabase, sqlHost, sqlUser

class TeamBoxScoreSpider(scrapy.Spider):

    name = 'teamBoxScores'

    def start_requests(self):

        url = 'https://data.nba.com/data/10s/v2015/json/mobile_teams/nba/{0}/scores/gamedetail/00{1}_gamedetail.json'

        missGames = self.getMissingGames()

        for idGame in missGames:
            urlGame = url.format(datetime.datetime.today().year, idGame)
            yield scrapy.Request(url = urlGame, callback = self.parse)


    # TODO make this method list and delete all entries whose last modify date is not greater than game date
    def getMissingGames(self):
        # Connect to sql database
        teamBoxConn = psycopg2.connect(host=sqlHost, database=sqlDatabase, user=sqlUser)
        teamBoxCur = teamBoxConn.cursor() 

        teamBoxConn.close()

        missingGames = []

        return missingGames


    def parse(self, response):

        def createBoxScoreObject(gameId, stats, location):
            teamStats = stats.get('tstsg')

            loader = ItemLoader(item=teamBoxScore())

            loader.add_value('idGame', gameId)
            loader.add_value('idTeam', stats.get('tid'))
            loader.add_value('location', location)
            loader.add_value('pts', stats.get('s'))
            loader.add_value('fga', teamStats.get('fga'))
            loader.add_value('fgm', teamStats.get('fgm'))
            loader.add_value('fg3a', teamStats.get('tpa'))
            loader.add_value('fg3m', teamStats.get('tpm'))
            loader.add_value('fta', teamStats.get('fta'))
            loader.add_value('ftm', teamStats.get('ftm'))
            loader.add_value('oreb', teamStats.get('oreb'))
            loader.add_value('dreb', teamStats.get('dreb'))
            loader.add_value('reb', teamStats.get('reb'))
            loader.add_value('ast', teamStats.get('ast'))
            loader.add_value('stl', teamStats.get('stl'))
            loader.add_value('blk', teamStats.get('blk'))
            loader.add_value('fouls', teamStats.get('pf'))
            loader.add_value('tov', teamStats.get('tov'))
            loader.add_value('ptsFastBreak', teamStats.get('fbpts'))
            loader.add_value('fgmFastBreak', teamStats.get('fbptsa'))
            loader.add_value('fgaFastBreak', teamStats.get('fbptsm'))
            loader.add_value('ptsPaint', teamStats.get('pip'))
            loader.add_value('fgmPaint', teamStats.get('pipm'))
            loader.add_value('fgaPaint', teamStats.get('pipa'))
            loader.add_value('bigLead', teamStats.get('ble'))
            loader.add_value('secondChancePts', teamStats.get('scp'))
            loader.add_value('teamReb', teamStats.get('tmreb'))
            loader.add_value('teamTov', teamStats.get('tmtov'))
            loader.add_value('ptsOffTov', teamStats.get('potov'))
            loader.add_value('ptsQ1', stats.get('q1'))
            loader.add_value('ptsQ2', stats.get('q2'))
            loader.add_value('ptsQ3', stats.get('q3'))
            loader.add_value('ptsQ4', stats.get('q4'))
            loader.add_value('ptsOT', (int(stats.get('ot1')) + int(stats.get('ot2')) + int(stats.get('ot3')) + int(stats.get('ot4'))))

            scoreItem = loader.load_item()

            return scoreItem

        game = json.loads(response.body).get('g')

        idGame = game.get('gid')

        homeEntry = createBoxScoreObject(idGame, game.get('vls'), 'A')
        awayEntry = createBoxScoreObject(idGame, game.get('hls'), 'H')

        yield homeEntry
        yield awayEntry
