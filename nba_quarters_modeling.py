# -*- coding: utf-8 -*-
"""
Created on Mon Mar  4 17:40:22 2019

@author: neilb
"""

from nba_api.stats.static import teams
from nba_api.stats.endpoints import leaguegamefinder
import os
import pandas as pd
import numpy as np
import pickle
import requests
import json
import random
from time import sleep

# CONSTANTS AND GLOBAL VARIABLES
WD = os.getcwd() + '/'
CURR_YEAR = '2018'
get_games_flag = False
OUTPUT_NAME = 'nba_scores_by_q.csv'

def get_games_list(get_games_bool):
    # function for gettting all games played in the NBA for a given season
    # inputs:
    #   get_games_bool: if True, get new game_id list, else get from pickle file
    
    print('    Getting list of game_ids...')
    if get_games_bool:
        print('        Getting new list of game_ids...')
        nba_teams = teams.get_teams()
        all_games = []
        
        # loop through each nba team to get all games
        for team in nba_teams:
            print('            Currently getting data for: {}'.format(team['full_name']))
            curr_team_id = team['id']
    
            gamefinder = leaguegamefinder.LeagueGameFinder(team_id_nullable=curr_team_id)
            games = gamefinder.get_data_frames()[0]
        
            # filter games only from the requested season
            curr_year_games = games[games.SEASON_ID.str[-4:] == CURR_YEAR]
            
            # add to overall games list        
            all_games.extend(curr_year_games.GAME_ID.unique().tolist())
        
        # after getting all game_ids for each team, get list of unique ids        
        all_games_unique = list(set(all_games))
        pickle.dump(all_games_unique, open("gameid_list.p", "wb"))
    
        # return all a list of all unique game ids
        return all_games_unique
    
    # else read game_id list from saved pickle file
    else:
        print('        Getting list of game_ids from pickle file...')
        all_games_unique = pickle.load(open("gameid_list.p", "rb") )
        return all_games_unique
        
def check_existing_games(games):
    # function for updating game_id list by removing game_ids that have already
    # been done in a previous run; this function removes existing 
    if os.path.isfile(WD + OUTPUT_NAME):
        print('        Previous data found, checking game_ids...')
        try:
            prev_data = pd.read_csv(WD + OUTPUT_NAME)
            old_ids = prev_data['game_id'].unique().tolist()
            
            cleaned_list = np.setdiff1d(games,old_ids)
            return cleaned_list
        except:
            print('        Empty dataset found, starting fresh...')
            return games
    # if no previous file exists, start fresh
    else:
        print('        No previous data found, starting fresh...')
        return games
        
def get_scores_by_quarter(games):
    # function for getting scores by quarter for every nba game
    # inputs: 
    #    games: list of game_ids from nba_stats api
    print('    Getting scores by quarter for each game_id...')
    games = check_existing_games(games)
    
    # save count for tracking progress through queries
    count = 1
    total = len(games)
    scores_by_quarter_df = pd.DataFrame()
    
    # checkpoints for backing up data
    cp1 = False
    cp2 = False
    cp3 = False
    
    # loop through each nba game based on game_id
    for game in games:
        # calculate % done so far
        percentage_done = count/total*100

        print('        Currently getting quarter scores for game_id: {} ({}/{}, {}%)'.format(game, count, total, round(percentage_done, 2)))
        data_by_game_id_url = 'https://stats.nba.com/stats/boxscoresummaryv2?GameID={}'.format(game)
       
        # add headers to spoof access from a browser
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.96 Safari/537.36'}
        
        # add delay between requests for large lists of games to prevent being
        # rate limited
        if total > 50:
            sleep(random.uniform(0.15, 0.4))
        else:
            sleep(random.uniform(0.05, 0.1))
        
        # make HTTP request to nba stats and get json output
        data_by_gameid_json = requests.get(data_by_game_id_url, headers=headers)
        data = json.loads(data_by_gameid_json.text)
    
        # get home team based on last three chars of game code
        game_code = data['resultSets'][0]['rowSet'][0][5]
        home_team = game_code[-3:]
        print('            Game_code: {}'.format(game_code))
        
        # need to keep track of game_id #, home team, away team, 
        # points scored, and quarter #
        home_team = []
        away_team = []
        home_points = []
        away_points = []
        quarter_num = []
        for quarter in range(4): 
            print('            Currently working on Q{}...'.format(1+quarter))
            # for both the home and away team, get quarter scores
            # home team first
            home_team.append((data['resultSets'][5]['rowSet'][0][4]))
            home_points.append((data['resultSets'][5]['rowSet'][0][8+quarter]))
            
            # away team
            away_team.append((data['resultSets'][5]['rowSet'][1][4]))
            away_points.append((data['resultSets'][5]['rowSet'][1][8+quarter]))
            quarter_num.append(1+quarter)
            # END quarter loop
            
        # create dataframe for all quarters in single game
        game_data_df = pd.DataFrame({'home_team': home_team,
                                     'away_team': away_team,
                                     'home_points': home_points,
                                     'away_points': away_points,
                                     'quarter': quarter_num})
    
        # save game id to column
        game_data_df['game_id'] = game
        # combine data from each game and save final result to csv
        scores_by_quarter_df = pd.concat([scores_by_quarter_df, game_data_df])
        
        if percentage_done >= 75 and cp3 == False:
            print('            75% checkpoint reached, saving data...')
            scores_by_quarter_df.to_csv('nba_scores_by_q.csv', index = False)
            cp3 = True
        elif percentage_done >= 50 and cp2 == False:
            print('            50% checkpoint reached, saving data...')
            scores_by_quarter_df.to_csv('nba_scores_by_q.csv', index = False) 
            cp2 = True
        elif percentage_done >= 25 and cp1 == False:
            print('            25% checkpoint reached, saving data...')
            scores_by_quarter_df.to_csv('nba_scores_by_q.csv', index = False)
            cp1 = True
            
        count += 1
        # END game loop
        
    scores_by_quarter_df.to_csv('nba_scores_by_q.csv', index = False)
    

def main():
    games_list = get_games_list(get_games_flag)
    get_scores_by_quarter(games_list)
    
if __name__ == '__main__':
    main()