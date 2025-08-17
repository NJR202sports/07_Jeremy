"""
Filename:    scawler_salary.py
Author:      Shin Yuan Huang
Created:     2025-07-30 
Last Update: 2025-08-02 15:22:51
Description: NBA salary data from www.hoopshype.com
"""

import urllib.request as req
from bs4 import BeautifulSoup as bs
import pandas as pd
import time
import random


def team_season_salary(team_name, year, url_num):

    url = f"https://www.hoopshype.com/salaries/teams/{team_name}/{url_num}/?season={year}"
    # print(url)

    # 儲存的路徑
    # dirname = os.path.join('.', 'salary')
    # if not os.path.exists(dirname):
    #     os.makedirs(dirname)

    # 取得網頁內容
    r = req.Request(url)
    r.add_header('user-agent',
                 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36')

    # 開啟網址並讀取html內容
    resp = req.urlopen(r)
    content = resp.read()
    html = bs(content, 'html.parser')


    # players
    players = html.find_all('td', {"class": "vTd-Ji__vTd-Ji"})
    player_list = []
    for player in players:
        player_list.append(player.text)
    player_list = player_list[:-1]
    # player_list[-1] = 'total'

    # salary
    year_items = html.find_all('th', {"colspan": "1", "class": "RLrCiX__RLrCiX"})
    salaryAll = html.find_all('td', {"class": "RLrCiX__RLrCiX"})
    salary_list = []
    for salary in salaryAll:
        salary_list.append(int(salary.text.strip()[1:].replace(',', '')))
    salary_list = salary_list[:-len(year_items)][::len(year_items)]

    return player_list, salary_list


if __name__ == '__main__':

    team_inf = {
        'ATL': ['atlanta-hawks', 1],
        'BOS': ['boston-celtics', 2],
        'BKN': ['brooklyn-nets', 17],
        'CHA': ['charlotte-hornets', 5312],
        'CHI': ['chicago-bulls', 4],
        'CLE': ['cleveland-cavaliers', 5],
        'DAL': ['dallas-mavericks', 6],
        'DEN': ['denver-nuggets', 7],
        'DET': ['detroit-pistons', 8],
        'GSW': ['golden-state-warriors', 9],
        'HOU': ['houston-rockets', 10],
        'IND': ['indiana-pacers', 11],
        'LAC': ['los-angeles-clippers', 12],
        'LAL': ['los-angeles-lakers', 13],
        'MEM': ['memphis_grizzlies', 29],
        'MIA': ['miami-heat', 14],
        'MIL': ['milwaukee-bucks', 15],
        'MIN': ['minnesota-timberwolves', 16],
        'NOP': ['new-orleans-pelicans', 3],
        'NYK': ['new-york-knicks', 18],
        'OKC': ['oklahoma-city-thunder', 25],
        'ORL': ['orlando-magic', 19],
        'PHI': ['philadelphia-76ers', 20],
        'PHX': ['phoenix-suns', 21],
        'POR': ['portland-trail-blazers', 22],
        'SAC': ['sacramento-kings', 23],
        'SAS': ['san-antonio-spurs', 24],
        'TOR': ['toronto-raptors', 28],
        'UTA': ['utah-jazz', 26],
        'WAS': ['washington-wizards', 27]
    }

    all_rows = []

    for year in range(2015, 2026):

        for _, team_num in team_inf.items():

            team_name = team_num[0]
            url_num = team_num[1]

            # print(year, team_name)

            player_list, salary_list = team_season_salary(team_name, year, url_num)

            # 檢查 player / salary 數量一致
            try:
                if len(player_list) != len(salary_list):
                    raise ValueError(
                        f"[資料不一致] {year} 年 {team_name}：players={len(player_list)}, salaries={len(salary_list)}")

                for i in range(len(player_list)):
                    all_rows.append({
                        'year': year,
                        'team': team_name,
                        'players': player_list[i],
                        'salary': salary_list[i]
                    })

            except Exception as e:
                print(f"發生錯誤於 {team_name} {year}：{e}")

            time.sleep(random.uniform(3, 5))  # 輕微延遲，避免封鎖



    df = pd.DataFrame(all_rows)
    # print(df)

    # 根據 "teams" 的值取得排序順序
    # df_sorted = df.sort_values(by='team', ignore_index=True)

    # save
    df.to_csv('nba_players_salary.csv', index=False)
    # with open('salary.json', 'w', encoding='utf-8') as f:
    #     json.dump(row, f, indent=4, ensure_ascii=False)

