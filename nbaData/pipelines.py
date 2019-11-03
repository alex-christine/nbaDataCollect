# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import json
import datetime
import pandas as pd
import psycopg2
from scrapy.exceptions import CloseSpider
from nbaData.items import scheduleGame
from pandas.io.json import json_normalize

# Information for SQL Database
sqlHost = 'localhost'
sqlDatabase = 'nba'
sqlUser = 'alexchristine'

# Flattens a single column response from sql into a one dimensional list
def flattenSingleColResp(sqlResponse, removeDuplicates=True):
    colEnts = []

    if (len(sqlResponse) > 0):
        for entry in sqlResponse:
            colEnts.append(entry[0])
        
        if (removeDuplicates):
            # Cast to set to remove duplicates
            colEnts = set(colEnts)
        
        colEnts = list(colEnts)
        return colEnts


# Default pipeline
class NbadataPipeline(object):
    def process_item(self, item, spider):
        return item


# Pipeline for active_players - writes objects to postreSQL database nba
class activePlayerWriterPipeline(object):

    # Initialize the class variables
    def __init__(self):

        # Initialize connection to database
        self.actPlayConn = psycopg2.connect(host=sqlHost, database=sqlDatabase, user=sqlUser)
        self.actPlayCur = self.actPlayConn.cursor()

        # Empty list to contain missing games
        self.sqlMissingGames = []


    # Get a list of games missing from the database
    def getMissingGames(self):

        # Selects the game_ids from games that have no match in active_players
        # thus creating a list of game_id's that are not in active player.
        # Includes check that game has not occurred yet (game_date < today)
        missingGamesQuery = """ 
            SELECT gm.game_id
            FROM games gm 
                    LEFT JOIN active_players act ON gm.game_id = act.game_id
                    WHERE act.game_id IS NULL
                    AND gm.game_date <= current_date; """

        # Execute query
        self.actPlayCur.execute(missingGamesQuery)
        gamesMiss = self.actPlayCur.fetchall()

        # Flatten response
        gamesMiss = flattenSingleColResp(gamesMiss)

        # Return a list of missing game_id's
        return gamesMiss


    # Opens the spider
    def open_spider(self, spider):
        if spider.name != 'activePlayers':
            return

        # Initializes the list of missing games
        self.sqlMissingGames = self.getMissingGames()


    # Closes spider and commits changes to database
    def close_spider(self, spider):
        if spider.name != 'activePlayers':
            return

        # Commit changes to SQL
        self.actPlayConn.commit()
        self.actPlayConn.close()



    # Process for dealing with missing games: if the game_id is in the missingGames
    # list, the item is added to the database (based on the assumption that the 
    # logic behind the dates plugged into the url is correct)
    def process_item(self, item, spider):
        if spider.name != 'activePlayers':
            return item
        
        # Creates a dict of the itemLoader
        actPlayItem = dict(item)


        # If the game_id is missing from the database - adds it
        if (actPlayItem.get('idGame') in self.sqlMissingGames):


            # Query to insert data to active_players
            actPlayQuery = """ 
                INSERT INTO active_players (player_id, game_id, team_id, position, memo, status)
                VALUES (%s, %s, %s, %s, %s, %s);"""

            # Pulling the data out of the loader
            playerId = actPlayItem.get('idPlayer')
            gameId = actPlayItem.get('idGame')
            teamId = actPlayItem.get('idTeam')
            position = actPlayItem.get('pos')
            memo = actPlayItem.get('memo')
            status = actPlayItem.get('status')

            # Combine the arguments into a tuple - order based on query
            actPlayArgs = (playerId, gameId, teamId, position, memo, status)

            # print (actPlayQuery % actPlayArgs)
            self.actPlayCur.execute(actPlayQuery, actPlayArgs)

        else:
            # Message to acknowledge game was scraped but not added to database
            print ('Game: ' + str(actPlayItem.get('idGame')) + ' is already in database - item skipped')

        return item


# Pipeline for player parsing - writes objects to postreSQL database nba
class playerWriterPipeline(object):
    
    # Initializes class variables
    def __init__(self):
        # Conenction to nba database
        self.playersConn = psycopg2.connect(host=sqlHost, database=sqlDatabase, user=sqlUser)
        self.playersCur = self.playersConn.cursor()
        
        # List for players contained in SQL
        self.playersInSQL = []
        
        # Teams directory generated off SQL database
        # workaround for no teamID in this pull
        self.teamsDirectory = pd.DataFrame()

        # List of arguments that will be fed to update queries
        self.updateArgs = []

        # Directory to link columns in SQL to itemLoader
        self.sqlColNames = pd.DataFrame([['player_id', 'playerId'], 
                                         ['first_name', 'firstName'],
                                         ['last_name', 'lastName'],
                                         ['height_ft', 'heightFt'],
                                         ['height_in', 'heightIn'],
                                         ['weight_lbs', 'weightLbs'],
                                         ['team_id', 'teamTri'], # This will be a special case
                                         ['position', 'position'],
                                         ['is_all_star', 'allStar']])
        self.sqlColNames.columns = ['sql', 'scrapy']

        # Cast as string
        self.sqlColNames['sql'] = self.sqlColNames['sql'].astype(str)
        self.sqlColNames['scrapy'] = self.sqlColNames['scrapy'].astype(str)


    # Creates list of player already in database
    def getExistingPlayers(self):
        selPlayersQuery = "SELECT player_id FROM players"
        self.playersCur.execute(selPlayersQuery)
        playersCol = self.playersCur.fetchall()

        # List to contain player_id from SQL
        playerIds = []

        # psycopg2 returns single column queries as a list of tuples
        # Section "flattens" list to playerIds
        if (len(playersCol) > 0):

            for playerSqlEnt in playersCol:
                    playerIds.append(playerSqlEnt[0])
            
            # Casting to set gets rid of duplicate values
            playerIds = set(playerIds)
            playerIds = list(playerIds)

        return playerIds


    # Creates team directory from SQL tables
    # DataFrame[team_id, tricode] returned
    def createTeamsDirect(self):
        getDirQuery = """
                        SELECT team_id, tricode FROM teams;"""

        self.playersCur.execute(getDirQuery)
        teamsList = self.playersCur.fetchall()

        teamsDirectory = pd.DataFrame(teamsList)
        teamsDirectory.columns = ['teamId', 'tricode']

        return teamsDirectory
    

    # Returns a list of the column names of the players (SQL) table
    # TODO make this order columns correctly
    def getPlayersCols(self):
        # Query to return column names
        playersColQuery = "SELECT column_name, ordinal_position FROM information_schema.columns WHERE table_name = 'players' ORDER BY ordinal_position ASC;"
        
        # Execute query
        self.playersCur.execute(playersColQuery)
        colNames = self.playersCur.fetchall()

        # Flatten and return response
        colNames = flattenSingleColResp(colNames)
        return colNames


    # Translate tricode to id
    def triToId(self, triLetter):
        # Looks up teamId based on tricode
        playerTeam = self.teamsDirectory[self.teamsDirectory.tricode == triLetter]
        team_id = int(playerTeam['teamId'])
        return team_id


    # Returns the entry for a single player as single-row DataFrame
    def getPlayerEntry(self, player_id):
        
        # Create a tuple for indexing purposes
        selPlayerId = (player_id,)

        # SQL query for players
        selPlayerQuery = "SELECT * FROM players WHERE player_id = %s;"
        
        # Execute query
        self.playersCur.execute(selPlayerQuery, selPlayerId)
        playerEntry = self.playersCur.fetchall()
        
        # player data entry
        playerEntry = pd.DataFrame(playerEntry)
        playerEntry.columns = list(self.sqlColNames['sql'])

        # Commit changes
        self.playersConn.commit()
        return playerEntry

    # Runs through update arguments and sends each one to SQL as query
    def updatePlayers(self):
        if (len(self.updateArgs) != 0):
            updateQuery = "UPDATE players SET {0} = {1} WHERE player_id = {2};"

            strUpdateQuery =  "UPDATE players SET {0} = '{1}' WHERE player_id = {2};"
            
            for updateItem in self.updateArgs:

                columnName = str(updateItem[0])

                if (columnName != 'first_name' and columnName != 'last_name' and columnName != 'position'):
                    execQuery = updateQuery.format(columnName, updateItem[1], updateItem[2])
                else:
                    execQuery = strUpdateQuery.format(columnName, updateItem[1], updateItem[2])
                
        
                
                print ('Pushing ' + columnName + ' update to '+ str(updateItem[1]) + ' for ' + str(updateItem[2]) + ' to SQL')
                # print(updateQuery)

                self.playersCur.execute(execQuery)
                self.playersConn.commit()

    # Opens spider and queries SQL for existing players
    def open_spider(self, spider):
        if spider.name != 'players':
            return

        self.playersInSQL = self.getExistingPlayers()
        self.teamsDirectory = self.createTeamsDirect()


    # Closes spider and commits changes to SQL
    def close_spider(self, spider):
        if spider.name != 'players':
            return

        self.updatePlayers()

        self.playersConn.commit()
        self.playersConn.close()


    # Handles the processing of each item
    # If it does not exist in SQL - adds it
    # If it exists but was added before game occurred - update fields
    # If it does not need modification - prints message indicating it was looked at
    def process_item(self, item, spider):
        if spider.name != 'players':
            return item

        # Creates dict of item
        playerItem = dict(item)

        teamId = self.triToId(playerItem.get('teamTri'))

        # Gets playerId from dict
        playerId = playerItem.get('playerId')


        # If player is not in database - add them
        if (playerId not in self.playersInSQL):

            insertPlayersArgs = (playerId, playerItem.get('firstName'), 
                                 playerItem.get('lastName'), playerItem.get('heightFt'), 
                                 playerItem.get('heightIn'), playerItem.get('weightLbs'), 
                                 teamId, playerItem.get('position'), 
                                 playerItem.get('allStar'))

            # Query to insert player into players (SQL)
            insertPlayersQuery = """ 
                INSERT INTO players (player_id, first_name, last_name, height_ft, height_in, weight_lbs, team_id, position, is_all_star)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"""

            self.playersCur.execute(insertPlayersQuery, insertPlayersArgs)


        else: 
            
            # Runs query to get the player's data from SQL
            playerEntrySql = self.getPlayerEntry(playerId)

            # Iterate through each of the columns of SQL entry
            for colName in list(self.sqlColNames['sql']):
                
                # Get comporable column in load
                loaderName = self.sqlColNames[self.sqlColNames['sql'] == colName]
                loaderName = str(loaderName['scrapy'].values[0])

                # Gets the SQL value for the column
                sqlVal = playerEntrySql[colName][0]

                # If the column is team_id - performs lookup to translate
                # tricode to team_id (dictionary) based of SQL columns
                if colName != 'team_id':
                    loaderVal = playerItem.get(loaderName)
                else:
                    loaderVal = teamId

                # If the values are not equal - save them in list and print message  
                if (sqlVal != loaderVal):

                    updateColArgs = [colName, loaderVal, playerId]
                    self.updateArgs.append(updateColArgs)

                    print ('Player: ' + str(playerId) + ' ' +
                            colName + ' updated from: ' + str(sqlVal) +
                            ' to: ' + str(loaderVal))

        # Returns the item to the pipeline
        return item

        
# Pipeline for team parsing - writes objects to postgreSQL database nba
class teamWriterPipeline(object):
    
    # Initializes the class
    def __init__(self):

        # Initializes the connection to the postgreSQL database
        self.teamsConn = psycopg2.connect(host=sqlHost, database=sqlDatabase, user=sqlUser)
        self.teamsCur = self.teamsConn.cursor()

        # List to contain entries already in SQL
        self.teamsInSQL = []


    # Returns a list containing the team_id (Primary Key) entries present in SQL
    # If none are present returns an empty list
    def getExistingTeams(self):
        selTeamsQuery = "SELECT team_id FROM teams"
        self.teamsCur.execute(selTeamsQuery)
        teamsCol = self.teamsCur.fetchall()

        # List to contain team_id from SQL
        teamIds = []

        # psycopg2 returns single column queries as a list of tuples
        # Section "flattens" list to teamIds
        if (len(teamsCol) > 0):

            for teamSqlEnt in teamsCol:
                    teamIds.append(teamSqlEnt[0])
            
            # Casting to set gets rid of duplicate values
            teamIds = set(teamIds)
            teamIds = list(teamIds)

        return teamIds

    # Opens the spider
    def open_spider(self, spider):
        if spider.name != 'teams':
            return

        # Checks database for existing entries
        self.teamsInSQL = self.getExistingTeams()

    # Closes the spider
    def close_spider(self, spider):
        if spider.name != 'teams':
            return
        
        # Commits data changes to postgreSQL
        self.teamsConn.commit()
        self.teamsConn.close()


    # Process pipeline items
    def process_item(self, item, spider):
        if spider.name != 'teams':
            return item

        # Create dict of scrapy item
        teamLoad = dict(item)

        # Check if idTeam exists in team_id (SQL - Primary Key)
        if (teamLoad.get('idTeam') not in self.teamsInSQL):
            
            teamsArgs = (teamLoad.get('idTeam'), teamLoad.get('teamCity'), 
                        teamLoad.get('teamName'), teamLoad.get('teamCode'), 
                        teamLoad.get('conference'), teamLoad.get('division'))

            # Query to insert item into teams (SQL)
            teamsQuery = """
                INSERT INTO teams (team_id, team_city, team_name, tricode, conference, division) 
                VALUES (%s, %s, %s, %s, %s, %s);"""
            
            # Executes the query to load data
            self.teamsCur.execute(teamsQuery, teamsArgs)

        else:

            # Print message acknowledging that item exists in database alredy
            print ("Team: " + str(teamLoad.get('idTeam')) + ' - ' + 
                   teamLoad.get('teamCity') + ' ' + teamLoad.get('teamName') + 
                   ' already exists in database - Not Added')

        return item


# Pipeline for the schedule objects
class scheduleWriterPipeline(object):

    # Initializes the class variables
    def __init__(self):
        # Conenction to nba database
        self.gamesConn = psycopg2.connect(host=sqlHost, database=sqlDatabase, user=sqlUser)
        self.gamesCur = self.gamesConn.cursor()
        
        # List of games contained in SQL
        self.gamesInSQL = []

        # List of games that do not need updating
        self.gamesIgnore = []


    # Returns a list of all game_id's in SQL
    def getExistingGames(self):
        selGamesQuery = "SELECT game_id FROM games"
        self.gamesCur.execute(selGamesQuery)
        gamesCol = self.gamesCur.fetchall()

        # List to contain game_id from SQL
        gameIds = []

        # psycopg2 returns single column queries as a list of tuples
        # Section "flattens" list to gameIds
        if (len(gamesCol) > 0):

            for gameSqlEnt in gamesCol:
                    gameIds.append(gameSqlEnt[0])
            
            # Casting to set gets rid of duplicate values
            gameIds = set(gameIds)
            gameIds = list(gameIds)

        return gameIds


    # Returns a list of games that do not need modification
    def getIgnoreGames(self):
        selGamesQuery = "SELECT game_id FROM games WHERE game_date < last_modify_date"
        self.gamesCur.execute(selGamesQuery)
        gamesCol = self.gamesCur.fetchall()

        # List to contain game_id from SQL
        gameIds = []

        # psycopg2 returns single column queries as a list of tuples
        # Section "flattens" list to gameIds
        if (len(gamesCol) > 0):

            for gameSqlEnt in gamesCol:
                    gameIds.append(gameSqlEnt[0])
            
            # Casting to set gets rid of duplicate values
            gameIds = set(gameIds)
            gameIds = list(gameIds)

        return gameIds

    
    # Opens spider and queries SQL for existing games
    def open_spider(self, spider):
        if spider.name != 'schedule':
            return

        self.gamesInSQL = self.getExistingGames()
        self.gamesIgnore = self.getIgnoreGames()


    # Closes the spider and commits data edits to SQL
    def close_spider(self, spider):
        if spider.name != 'schedule':
            return

        self.gamesConn.commit()
        self.gamesConn.close()


    # Returns a list of the win loss record, record[0] is wins
    # record[1] is losses, if len(output) is 1 record does not exist yet
    def splitRecord(self, record):
        wlRec = str(record).split('-')
        return wlRec


    # Handles the processing of each item
    # If it does not exist in SQL - adds it
    # If it exists but was added before game occurred - update fields
    # If it does not need modification - prints message indicating it was looked at
    def process_item(self, item, spider):
        if spider.name != 'schedule':
            return item
    
        # Creates dict of each item
        gameLoad = dict(item)

        # ID of the game in question
        gameId = gameLoad.get('idGame')


        # Check if game is in SQL - if not - adds it
        if (gameId not in self.gamesInSQL):
            
            gameDateTime = datetime.datetime.strptime(gameLoad.get('gameDate') + ' ' + gameLoad.get('gameTime'), '%Y-%m-%d %H:%M:%S')

            homeRecord = self.splitRecord(gameLoad.get('hRecord'))
            awayRecord = self.splitRecord(gameLoad.get('aRecord'))

            # If the list is empty the record does not exist - set to 0-0
            if (len(homeRecord) > 1):
                homeWins = homeRecord[0]
                homeLosses = homeRecord[1]
            else:
                homeWins = 0
                homeLosses = 0
            
            # If the list is empty the record does not exist - set to 0-0
            if (len(awayRecord) > 1):
                awayWins = awayRecord[0]
                awayLosses = awayRecord[1]
            else:
                awayWins = 0
                awayLosses = 0

            # Combine the query arguments - note column order in query statement
            gamesArgs = (gameLoad.get('idGame'), str(gameDateTime.date()), 
                         str(gameDateTime.time()), str(gameLoad.get('gameCity')),
                         str(gameLoad.get('gameArena')), gameLoad.get('idTeamH'), 
                         homeWins, homeLosses, gameLoad.get('hScore'),
                         gameLoad.get('idTeamA'), awayWins, awayLosses,
                         gameLoad.get('aScore'), str(datetime.datetime.today().date()))

            # Query to insert item into games (SQL)
            gamesQuery = """
                INSERT INTO games (game_id, game_date, game_time, game_location, game_arena, home_team_id, home_wins_record, home_loss_record, home_score, away_team_id, away_wins_record, away_loss_record, away_score, last_modify_date) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
            
            # Executes the query to load data
            self.gamesCur.execute(gamesQuery, gamesArgs)

        # Checks if game was loaded before it ocurred - update if so
        elif (gameId in self.gamesInSQL and gameId not in self.gamesIgnore):

            # List object for updated Data
            updatedHomeRecord = self.splitRecord(gameLoad.get('hRecord'))
            updatedAwayRecord = self.splitRecord(gameLoad.get('aRecord'))

            updateGamesArgs = (updatedHomeRecord[0], updatedHomeRecord[1], gameLoad.get('hScore'),
                               updatedAwayRecord[0], updatedAwayRecord[1], gameLoad.get('aScore'), gameId)

            # Query to update game entry
            updateGamesQuery = """
                UPDATE games
                SET home_wins_record = %s, home_loss_record = %s, home_score = %s, away_wins_record = %s, away_loss_record = %s, away_score = %s
                WHERE game_id = %s;"""

            # Executes the query to update data in games (SQL)
            self.gamesCur.execute(updateGamesQuery, updateGamesArgs)


            print ("Game: " + str(gameId) + " has been updated in the database - Wins, Losses, Score for Home and Away updated")
        
        # Game does not need modification
        else:

            # Print message acknowledging that item exists in database alredy
            print ("game: " + str(gameLoad.get('idGame')) +  
                   ' already exists in database - Not Added')

        # Return the item to the pipeline
        return item

