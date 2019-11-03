import scrapy
import json
import datetime
import psycopg2
from nbaData.items import activePlayer
from scrapy.loader import ItemLoader
from scrapy.exceptions import DropItem, CloseSpider
from nbaData.pipelines import sqlDatabase, sqlHost, sqlUser


class ActivePlayersSpider(scrapy.Spider):

    name = 'activePlayers'


    # Flattens the response from a single column Sql query (list of tuples in psycopg2)
    def flattenSingleColResp(self, sqlResponse, removeDuplicates=True):
        colEnts = []

        if (len(sqlResponse) > 0):
            for entry in sqlResponse:
                colEnts.append(entry[0])
            
            if (removeDuplicates):
                # Cast to set to remove duplicates
                colEnts = set(colEnts)
            
            colEnts = list(colEnts)
            return colEnts


    # Link is based on dates - requires query to get game dates that are missing from SQL
    def determineDates(self):
        # Connect to sql database
        actPlayConn = psycopg2.connect(host=sqlHost, database=sqlDatabase, user=sqlUser)
        actPlayCur = actPlayConn.cursor() 


        # Checks for game_dates corresponding to games that have a game_id but do not exist in active_players
        missingDatesQuery = """ 
            SELECT gm.game_date
            FROM games gm 
                    LEFT JOIN active_players act ON gm.game_id = act.game_id
                    WHERE act.game_id IS NULL
                    AND gm.game_date <= current_date; """

        actPlayCur.execute(missingDatesQuery)
        missingDates = actPlayCur.fetchall()

        # Checks the length and returns a flattened list
        if (len(missingDates) == 0):
            return []

        missList = []

        for missed in missingDates:
            missList.append(missed[0])

        
        # Delete the other games from active_players on missing dates
        delMissDateGames = """ 
            DELETE FROM active_players
            WHERE game_id IN (
                SELECT game_id
                FROM games
                WHERE game_date IN (
                        SELECT gm.game_date
                        FROM games gm 
                                LEFT JOIN active_players act ON gm.game_id = act.game_id
                                WHERE act.game_id IS NULL
                                AND gm.game_date <= current_date))"""
        
        actPlayCur.execute(delMissDateGames)
        actPlayConn.commit()

        actPlayConn.close()
        
        return missList


    # TODO Make this elimintate all games that happen on a day thats missing a game
    def removeByDate(self, gameDates):
       # Connect to sql database
        actPlayConn = psycopg2.connect(host=sqlHost, database=sqlDatabase, user=sqlUser)
        actPlayCur = actPlayConn.cursor()


        # TODO make query that returns game_ids of games that ocurred on a date
        gamesDateQuery = """ 
            SELECT gm.game_id
            FROM games gm 
                    LEFT JOIN active_players act ON gm.game_id = act.game_id
                    WHERE act.game_id IS NOT NULL
                    AND gm.game_date <= current_date; """

        actPlayCur.execute(gamesDateQuery)
        gameIdsDate = actPlayCur.fetchall()

        gameIdsDate = self.flattenSingleColResp(gameIdsDate)
        
        return gameIdsDate



    # Starts the requests for the spider
    def start_requests(self):
        
        missingDates = self.determineDates()
        

        # If there is nothing missing close the spider
        if (len(missingDates) == 0): raise CloseSpider('NO ACTION REQUIRED: Data is already updated')

        
        # Request url
        urls = ['https://stats.nba.com/js/data/leaders/00_active_starters_{0}.json']

        # Loop that for some reason causes error if removed
        for url in urls:

            # Formats the url with a date
            for dateMiss in missingDates:
                nbaDate = dateMiss.strftime('%Y%m%d')

                requestUrl = url.format(nbaDate)
                
                yield scrapy.Request(url=requestUrl, callback=self.parse)


    # Breaks down the response into activePlayer items
    def parse(self, response):

        # Create a loader
        def createaActPlayObject(game, team, player):
            loader = ItemLoader(item=activePlayer())

            loader.add_value('idGame', game)
            loader.add_value('idTeam', team)
            loader.add_value('idPlayer', player.get('pid'))
            loader.add_value('name', player.get('name'))
            loader.add_value('status', player.get('status'))

            if (player.get('status') == 'I'):
                loader.add_value('pos', 'Inactive')
            else:
                loader.add_value('pos', player.get('pos'))
            
            loader.add_value('memo', player.get('memo'))

            player_item = loader.load_item()

            return player_item
        

        # Drop item if 404 error - caused calling date before data is loaded
        if (response.status == 404):
            raise DropItem ('Link not active: ' + str(response.request.url))

        active = json.loads(response.body)
        games = active.keys()


        # Iterate through the games on a date
        for i in games:
            game = active.get(i)
            id_game = int(i)

            # Split into home and away teams
            home = game.get('htm')
            away = game.get('vtm')

            home_id = game.get('htmid')
            away_id = game.get('vtmid')

            # Create activePlayers items
            for homePlayer in home:
                home_entry = createaActPlayObject(id_game, home_id, homePlayer)
                yield home_entry

            for awayPlayer in away:
                away_entry = createaActPlayObject(id_game, away_id, awayPlayer)
                yield away_entry
