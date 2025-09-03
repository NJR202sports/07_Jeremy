"""
Filename:    scawler_salary.py
Author:      Shin Yuan Huang
Created:     2025-07-30 
Description: NBA salary data from www.hoopshype.com
"""

import urllib.request as req
import bs4 as bs
import pandas as pd
import os
import time
import random
import lxml
from data_ingestion.mysql import upload_data_to_mysql, upload_data_to_mysql_upsert, nba_players_salary_table

def nba_players_salary(year):

    team_inf = {
        'atlanta-hawks': [1],
        'boston-celtics': [2],
        'brooklyn-nets': [17],
        'charlotte-hornets': [5312],
        'chicago-bulls': [4],
        'cleveland-cavaliers': [5],
        'dallas-mavericks': [6],
        'denver-nuggets': [7],
        'detroit-pistons': [8],
        'golden-state-warriors': [9],
        'houston-rockets': [10],
        'indiana-pacers': [11],
        'los-angeles-clippers': [12],
        'los-angeles-lakers': [13],
        'memphis-grizzlies': [29],
        'miami-heat': [14],
        'milwaukee-bucks': [15],
        'minnesota-timberwolves': [16],
        'new-orleans-pelicans': [3],
        'new-york-knicks': [18],
        'oklahoma-city-thunder': [25],
        'orlando-magic': [19],
        'philadelphia-76ers': [20],
        'phoenix-suns': [21],
        'portland-trail-blazers': [22],
        'sacramento-kings': [23],
        'san-antonio-spurs': [24],
        'toronto-raptors': [28],
        'utah-jazz': [26],
        'washington-wizards': [27]
    }

    all_rows = []

    for team_name, url_num in team_inf.items():

        url = f"https://www.hoopshype.com/salaries/teams/{team_name}/{url_num[0]}/?season={year}"
        print(url)

        # 取得網頁內容
        r = req.Request(url)
        r.add_header('user-agent',
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/1')

        # 開啟網址並讀取html內容
        resp = req.urlopen(r)
        content = resp.read()
        html = bs.BeautifulSoup(content)


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


        # 檢查 player / salary 數量一致
        try:
            if len(player_list) != len(salary_list):
                raise ValueError(
                    f"[資料不一致] {year} 年 {team_name}：players={len(player_list)}, salaries={len(salary_list)}")

            for i in range(len(player_list)):
                all_rows.append({
                    'year': year,                    
                    'players': player_list[i],
                    'team': team_name,
                    'salary': salary_list[i]
                })

        except Exception as e:
            print(f"發生錯誤於 {team_name} {year}：{e}")

        time.sleep(random.uniform(3, 5))  # 輕微延遲，避免封鎖

    # save
    dirname = "nba_players_salary"
    if not os.path.exists(dirname):
        os.mkdir(dirname)

    df = pd.DataFrame(all_rows)
    df['team_cut'] = df["team"].str.split('-')
    df['team'] = df['team_cut'].str[-1]
    df.drop(columns=['team_cut'], inplace=True)
    # df.index += 1
    # fn = os.path.join(dirname, f"nba_players_salary_{year}.csv")
    # df.to_csv(fn, encoding="utf-8-sig")
    
    data = df.to_dict(orient='records') # 將 DataFrame 轉換為字典列表
    upload_data_to_mysql_upsert(table_obj=nba_players_salary_table, data=data)
    print(f"nba_players_salary_{year} has been uploaded to mysql.")

   




if __name__ == '__main__':

    years = list(range(2021,2026))

    for year in years:

        nba_players_salary(year)

# print(nba_players_salary(2018))