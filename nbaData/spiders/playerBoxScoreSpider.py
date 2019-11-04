import scrapy
import json
import psycopg2
import pandas as pd
import datetime
from nbaData.items import playerBoxScore
from scrapy.loader import ItemLoader
from nbaData.pipelines import sqlDatabase, sqlHost, sqlUser


class PlayerBoxScoreSpider(scrapy.Spider):

    name = 'playerBoxScores'


    # Clears any box score entries that were created with "incomplete data"
    # Incomplete is defined here as having been last updated on the same day game ocurred
    def clearIncompleteGames(self):
        playerBoxConn = psycopg2.connect(host=sqlHost, database=sqlDatabase, user=sqlUser)
        playerBoxCur = playerBoxConn.cursor() 

        # The logic here is that if a game was last updated on the same day it ocurred it could have 
        # still been going on when the data was pulled. Therefore these entries are deleted. This
        # occurs before the missing game list is generated so they will simply be pulled and reloaded
        clearGamesQuery = """
            DELETE FROM player_box_scores
            WHERE game_id IN ( 
                SELECT gm.game_id
                FROM games gm 
                        LEFT JOIN player_box_scores pbs ON gm.game_id = pbs.game_id
                        WHERE pbs.game_id IS NOT NULL
                        AND pbs.last_modify_date <= gm.Game_date
                        AND gm.game_date <= current_date);  """

        playerBoxCur.execute(clearGamesQuery)
        playerBoxConn.commit()
        playerBoxConn.close()


    # Gets any games that have occurred (from games in SQL) and are not in player_box_scores
    def getMissingGames(self):
        # Connect to sql database
        playerBoxConn = psycopg2.connect(host=sqlHost, database=sqlDatabase, user=sqlUser)
        playerBoxCur = playerBoxConn.cursor() 


        # This query gets any games that have happened but do not have an id in player_box_score
        # At the moment this game only checks for games that ocurred "yesterday or before" - today not included
        # 
        # If < is changed to <= in last row this will become more of a live score checker - today will be included
        missingGamesQuery = """ 
            SELECT gm.game_id
            FROM games gm 
                    LEFT JOIN player_box_scores pbs ON gm.game_id = pbs.game_id
                    WHERE pbs.game_id IS NULL
                    AND gm.game_date < current_date; """


        playerBoxCur.execute(missingGamesQuery)
        gamesList = playerBoxCur.fetchall()

        playerBoxConn.close()

        missingGames = []

        # Flattens the list of one element tuples - quirk of psycopg2
        if (len(gamesList) > 0):
            for game in gamesList:
                missingGames.append(game[0])

        return missingGames


    # Clears incomplete entries from database, generates list of missing entries
    # and calls urls 
    def start_requests(self):

        url = 'https://data.nba.com/data/10s/v2015/json/mobile_teams/nba/{0}/scores/gamedetail/00{1}_gamedetail.json'

        missingGames = self.getMissingGames()

        for idGame in missingGames:
            urlGame = url.format(datetime.datetime.today().year, idGame)
            yield scrapy.Request(url = urlGame, callback = self.parse)


    def parse(self, response):

        def createPlayerBoxScore(gameID, teamID, location, stats):
            loader = ItemLoader(item=playerBoxScore())

            loader.add_value('idGame', gameID)
            loader.add_value('idTeam', teamID)
            loader.add_value('idPlayer', stats.get('pid'))
            loader.add_value('gameLocation', location)
            loader.add_value('name', (str(stats.get('fn')) + ' ' +  str(stats.get('ln'))))
            loader.add_value('number', stats.get('num'))
            loader.add_value('pos', stats.get('pos'))
            loader.add_value('status', stats.get('status'))
            loader.add_value('memo', stats.get('memo'))
            loader.add_value('mins', stats.get('min'))
            loader.add_value('sec', stats.get('sec'))
            loader.add_value('totalSec', stats.get('totsec'))
            loader.add_value('pts', stats.get('pts'))
            loader.add_value('plusMinus', stats.get('pm'))
            loader.add_value('fga', stats.get('fga'))
            loader.add_value('fgm', stats.get('fgm'))
            loader.add_value('fg3a', stats.get('tpa'))
            loader.add_value('fg3m', stats.get('tpm'))
            loader.add_value('fta', stats.get('fta'))
            loader.add_value('ftm', stats.get('ftm'))
            loader.add_value('oreb', stats.get('oreb'))
            loader.add_value('dreb', stats.get('dreb'))
            loader.add_value('reb', stats.get('reb'))
            loader.add_value('ast', stats.get('ast'))
            loader.add_value('stl', stats.get('stl'))
            loader.add_value('blk', stats.get('blk'))
            loader.add_value('blkA', stats.get('blka'))
            loader.add_value('pf', stats.get('pf'))
            loader.add_value('tf', stats.get('tf'))
            loader.add_value('tov', stats.get('tov'))
            loader.add_value('ptsFastBreak', stats.get('fbpts'))
            loader.add_value('fgaFastBreak', stats.get('fbptsa'))
            loader.add_value('fgmFastBreak', stats.get('fbptsm'))
            loader.add_value('ptsPaint', stats.get('pip'))
            loader.add_value('fgaPaint', stats.get('pipa'))
            loader.add_value('fgmPaint', stats.get('pipm'))

            scoreItem = loader.load_item()
            return scoreItem

        game = json.loads(response.body).get('g')
        idGame = game.get('gid')


        homeTeam = game.get('hls')
        idHomeTeam = homeTeam.get('tid')

        for homePlayer in homeTeam.get('pstsg'):
            yield createPlayerBoxScore(idGame, idHomeTeam, 'H', homePlayer)


        awayTeam = game.get('vls')
        idAwayTeam = awayTeam.get('tid')

        for awayPlayer in awayTeam.get('pstsg'):
            yield createPlayerBoxScore(idGame, idAwayTeam, 'A', awayPlayer)
