import scrapy
import json
import pandas as pd
import numpy as np
import datetime
from nbaData.items import playByPlay
from scrapy.loader import ItemLoader
import psycopg2
from nbaData.pipelines import sqlDatabase, sqlHost, sqlUser


class PlayByPlaySpider(scrapy.Spider):

    name = 'plays'

    
    # Clears any box score entries that were created with "incomplete data"
    # Incomplete is defined here as having been last updated on the same day game ocurred
    def clearIncompleteGames(self):
        teamBoxConn = psycopg2.connect(host=sqlHost, database=sqlDatabase, user=sqlUser)
        teamBoxCur = teamBoxConn.cursor() 

        # The logic here is that if a game was last updated on the same day it ocurred it could have 
        # still been going on when the data was pulled. Therefore these entries are deleted. This
        # occurs before the missing game list is generated so they will simply be pulled and reloaded
        clearGamesQuery = """
            DELETE FROM play_by_play
            WHERE game_id IN ( 
                SELECT gm.game_id
                FROM games gm 
                        LEFT JOIN play_by_play pbp ON gm.game_id = pbp.game_id
                        WHERE pbp.game_id IS NOT NULL
                        AND pbp.last_modify_date <= gm.Game_date
                        AND gm.game_date <= current_date);  """

        teamBoxCur.execute(clearGamesQuery)
        teamBoxConn.commit()
        teamBoxConn.close()


     # Gets a list of all games that have happened that are not in the database
    def getMissingGames(self):
        # Connect to sql database
        teamBoxConn = psycopg2.connect(host=sqlHost, database=sqlDatabase, user=sqlUser)
        teamBoxCur = teamBoxConn.cursor() 


        # This query gets any games that have happened but do not have an id in team_box_score
        # At the moment this game only checks for games that ocurred "yesterday or before" - today not included
        # 
        # If < is changed to <= in last row this will become more of a live score checker - today will be included
        missingGamesQuery = """ 
            SELECT gm.game_id
            FROM games gm 
                    LEFT JOIN play_by_play pbp ON gm.game_id = pbp.game_id
                    WHERE pbp.game_id IS NULL
                    AND gm.game_date < current_date; """


        teamBoxCur.execute(missingGamesQuery)
        gamesList = teamBoxCur.fetchall()

        teamBoxConn.close()

        missingGames = []

        # Flattens the list of one element tuples - quirk of psycopg2
        if (len(gamesList) > 0):
            for game in gamesList:
                missingGames.append(game[0])

        return missingGames


    def start_requests(self):
        url = 'https://data.nba.com/data/10s/v2015/json/mobile_teams/nba/{0}/scores/pbp/00{1}_full_pbp.json'

        missingGames = self.getMissingGames()

        for idGame in missingGames:
            urlGame = url.format(datetime.datetime.today().year, idGame)
            yield scrapy.Request(url = urlGame, callback = self.parse)


    def parse(self, response):

        def createPlaysObject(gameID, period, event):
            loader = ItemLoader(item = playByPlay())

            loader.add_value('idGame', gameID)
            loader.add_value('quarter', period)
            loader.add_value('event', event.get('evt'))
            loader.add_value('clock', event.get('cl'))
            loader.add_value('description', event.get('de'))
            loader.add_value('locX', event.get('locX'))
            loader.add_value('locY', event.get('locY'))
            loader.add_value('opt1', event.get('opt1'))
            loader.add_value('opt2', event.get('opt2'))
            loader.add_value('mType', event.get('mtype'))
            loader.add_value('eventType', event.get('etype'))
            loader.add_value('idPlayerO', event.get('opid'))
            loader.add_value('idTeam', event.get('tid'))
            loader.add_value('idPlayer', event.get('pid'))
            loader.add_value('scoreH', event.get('hs'))
            loader.add_value('scoreA', event.get('vs'))
            loader.add_value('idPlayerE', event.get('epid'))
            loader.add_value('idTeamOf', event.get('oftid'))
            loader.add_value('ordering', event.get('ord'))

            playsItem = loader.load_item()

            return playsItem

        game = json.loads(response.body).get('g')

        idGame = game.get('gid')


        for quarter in game.get('pd'):
            eventList = quarter.get('pla')
            numQ = quarter.get('p')

            for gameEvent in eventList:
                yield createPlaysObject(idGame, numQ, gameEvent)
