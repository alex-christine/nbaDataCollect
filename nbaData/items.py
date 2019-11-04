# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from datetime import date
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose, TakeFirst, Compose

# Helper Methods for data collection

# Cleans the score object and returns -1 if none is available
def cleanScore(x):
    try:
        return int(x)
    except:
        return -1


    # Checks if height is present if not returns 0
def cleanIntVal(x):
    if str(x).isnumeric():
        return int(x)
    
    return 0


# Cleans the time input
def cleanTime(x):
    return str(x).split('T')[1]


def noMemo(x):
    return x if x != ' ' and x != '' else 'No memo'


def noStatus(x):
    return x if x!= '' else 'NA'


def noPos(x):
    return x if x!= '' else 'Bench'


def makeInt(x):
    try:
        return int(x)
    except:
        return 0


def makeKey(x):
    try:
        if (int(x) != 0) : return int(x)
        
        return None

    except:
        return None


# Defines Play by Play Item
class playByPlay(scrapy.Item):

    idGame = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    quarter = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    event = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    clock = scrapy.Field(
        input_processor = MapCompose(str),
        output_processor = TakeFirst()
    )

    description = scrapy.Field(
        input_processor = MapCompose(str),
        output_processor = TakeFirst()
    )

    locX = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    locY = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    opt1 = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    opt2 = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    mType = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    eventType = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    idPlayerO = scrapy.Field(
        input_processor = MapCompose(makeKey),
        output_processor = TakeFirst()
    )

    idTeam  = scrapy.Field(
        input_processor = MapCompose(makeKey),
        output_processor = TakeFirst()
    )

    idPlayer = scrapy.Field(
        input_processor = MapCompose(makeKey),
        output_processor = TakeFirst()
    )

    scoreH = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    scoreA = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    idPlayerE = scrapy.Field(
        input_processor = MapCompose(makeKey),
        output_processor = TakeFirst()
    )

    idTeamOf = scrapy.Field(
        input_processor = MapCompose(makeKey),
        output_processor = TakeFirst()
    )

    ordering = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )


# playerBoxScoreItem
class playerBoxScore(scrapy.Item):

    idGame = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    idPlayer = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    idTeam = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    gameLocation = scrapy.Field(
        input_processor = MapCompose(str),
        output_processor = TakeFirst()
    )

    name = scrapy.Field(
        input_processor = MapCompose(str),
        output_processor = TakeFirst()
    )

    number = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    pos = scrapy.Field(
        input_processor = MapCompose(noPos),
        output_processor = TakeFirst()
    )

    status = scrapy.Field(
        input_processor = MapCompose(noStatus),
        output_processor = TakeFirst()
    )

    memo = scrapy.Field(
        input_processor = MapCompose(noMemo),
        output_processor = TakeFirst()
    )

    mins = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    sec = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    totalSec = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    pts = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    plusMinus = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    fga = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    fgm = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    fg3a = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    fg3m = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    fta = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    ftm = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    oreb = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    dreb = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    reb = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    ast = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    stl = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    blk = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    blkA = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    pf = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    tf = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    tov = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    ptsFastBreak = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    fgaFastBreak = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    fgmFastBreak = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    ptsPaint = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    fgaPaint = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    fgmPaint = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )


# Defines the teamBoxScore Object
class teamBoxScore(scrapy.Item):
    idGame = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    idTeam = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    location = scrapy.Field(
        input_processor = MapCompose(str),
        output_processor = TakeFirst()
    )

    pts = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    fga = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    fgm = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    fg3a = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    fg3m = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    fta = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    ftm = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    oreb = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    dreb = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    reb = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    ast = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    stl = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    blk = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    fouls = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    tov = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    ptsFastBreak = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    fgmFastBreak = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    fgaFastBreak = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    ptsPaint = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    fgmPaint = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    fgaPaint = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    bigLead = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    secondChancePts = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    teamReb = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    teamTov = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    ptsOffTov = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    ptsQ1 = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    ptsQ2 = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    ptsQ3 = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    ptsQ4 = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    ptsOT = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )


# Default pipeline
class NbadataItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


# Defines the activePlayer object
class activePlayer(scrapy.Item):

    idGame = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    ) # Game ID

    idTeam = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    ) # Team ID

    idPlayer = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    ) # Player ID

    name = scrapy.Field(
        input_processor = MapCompose(str),
        output_processor = TakeFirst()
    ) # player name

    status = scrapy.Field(
        input_processor = MapCompose(noStatus, str),
        output_processor = TakeFirst()
    ) # status for game

    pos = scrapy.Field(
        # Position object is now iterable - take first
        input_processor = Compose(list.pop, noPos, str),
        output_processor = TakeFirst()
    ) # Position

    memo = scrapy.Field(
        input_processor = MapCompose(noMemo, str),
        output_processor = TakeFirst()
    ) # Memo


# Defines item structure for player items
class playerItem(scrapy.Item):
    playerId = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    ) # Player ID

    firstName = scrapy.Field(
        input_processor = MapCompose(str),
        output_processor = TakeFirst()
    ) # First Name

    lastName = scrapy.Field(
        input_processor = MapCompose(str),
        output_processor = TakeFirst()
    ) # Last Name

    heightFt = scrapy.Field(
        input_processor = MapCompose(cleanIntVal),
        output_processor = TakeFirst()
    ) # Height - Feet Only

    heightIn = scrapy.Field(
        input_processor = MapCompose(cleanIntVal),
        output_processor = TakeFirst()
    ) # Height - Inches only

    weightLbs = scrapy.Field(
        input_processor = MapCompose(cleanIntVal),
        output_processor = TakeFirst()
    ) # Weight in pounds

    teamTri = scrapy.Field(
        input_processor = MapCompose(str),
        output_processor = TakeFirst()
    ) # Team Tricode - will be converted to ID based on SQL table in pipeline

    position = scrapy.Field(
        input_processor = MapCompose(str),
        output_processor = TakeFirst()
    ) # Position string

    allStar = scrapy.Field(
        input_processor = MapCompose(bool),
        output_processor = TakeFirst()
    ) # Is All Star?


# Defines item structure for team items
class teamItem(scrapy.Item):
    idTeam = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    ) # Team ID

    teamCity = scrapy.Field(
        input_processor = MapCompose(str),
        output_processor = TakeFirst()
    ) # Team City

    teamName = scrapy.Field(
        input_processor = MapCompose(str),
        output_processor = TakeFirst()
    ) # Team Name

    teamCode = scrapy.Field(
        input_processor = MapCompose(str),
        output_processor = TakeFirst()
    ) # Three Letter Code

    conference = scrapy.Field(
        input_processor = MapCompose(str),
        output_processor = TakeFirst()
    ) # Team City

    division = scrapy.Field(
        input_processor = MapCompose(str),
        output_processor = TakeFirst()
    ) # Team City


# Defines pipe;line for schedule items
class scheduleGame(scrapy.Item):
    idGame = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    ) # Game ID

    gameDate = scrapy.Field(
        input_processor = MapCompose(str),
        output_processor = TakeFirst()
    ) # Game date

    gameTime = scrapy.Field(
        input_processor = MapCompose(cleanTime),
        output_processor = TakeFirst()
    ) # Eastern start time

    gameArena = scrapy.Field(
        input_processor = MapCompose(str),
        output_processor = TakeFirst()
    ) # Arena

    gameCity = scrapy.Field(
        input_processor = MapCompose(str),
        output_processor = TakeFirst()
    ) # Game City

    gameState = scrapy.Field(
        input_processor = MapCompose(str),
        output_processor = TakeFirst()
    ) # Game State

    idTeamA = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    aRecord = scrapy.Field(
        input_processor = MapCompose(str),
        output_processor = TakeFirst()
    )

    aTeam = scrapy.Field(
        input_processor = MapCompose(str),
        output_processor = TakeFirst()
    )

    aScore = scrapy.Field(
        input_processor = MapCompose(cleanScore),
        output_processor = TakeFirst()
    )

    idTeamH = scrapy.Field(
        input_processor = MapCompose(int),
        output_processor = TakeFirst()
    )

    hRecord = scrapy.Field(
        input_processor = MapCompose(str),
        output_processor = TakeFirst()
    )

    hTeam = scrapy.Field(
        input_processor = MapCompose(str),
        output_processor = TakeFirst()
    )

    hScore = scrapy.Field(
        input_processor = MapCompose(cleanScore),
        output_processor = TakeFirst()
    )

