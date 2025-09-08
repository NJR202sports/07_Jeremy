import urllib.request as req
import os
from bs4 import BeautifulSoup as bs
import pandas as pd
import numpy as np
from data_ingestion.mysql import upload_data_to_mysql, upload_data_to_mysql_upsert, nba_players_state_table

def nba_players_state(year:int):

    url = f"https://www.basketball-reference.com/leagues/NBA_{year}_totals.html"

    resp = req.urlopen(url)
    content = resp.read()
    html = bs(content, 'html.parser')

    table = html.find("tbody")
    rows = table.find_all("tr")
    players = []
    names_seen = set()
        
    for row in rows:
    # 跳過空列或是有 'class=thead' 的分隔列
        if row.get("class") == ['thead']:
            continue

        # 抓球員名字與隊伍代碼
        name_cell = row.find("td", {"data-stat": "name_display"})
        age_cell = row.find("td", {"data-stat": "age"})
        team_cell = row.find("td", {"data-stat": "team_name_abbr"})
        pos_cell = row.find("td", {"data-stat": "pos"})
        g_cell = row.find("td", {"data-stat": "games"})
        gs_cell = row.find("td", {"data-stat": "games_started"})
        mp_cell = row.find("td", {"data-stat": "mp"})
        fg_cell = row.find("td", {"data-stat": "fg"})
        fga_cell = row.find("td", {"data-stat": "fga"})
        fg_pct_cell = row.find("td", {"data-stat": "fg_pct"})
        p3_cell = row.find("td", {"data-stat": "fg3"})
        pa3_cell = row.find("td", {"data-stat": "fg3a"})
        p3_pct_cell = row.find("td", {"data-stat": "fg3_pct"})
        p2_cell = row.find("td", {"data-stat": "fg2"})
        pa2_cell = row.find("td", {"data-stat": "fg2a"})
        p2_pct_cell = row.find("td", {"data-stat": "fg2_pct"})
        efg_pct_cell = row.find("td", {"data-stat": "efg_pct"})
        ft_cell = row.find("td", {"data-stat": "ft"})
        fta_cell = row.find("td", {"data-stat": "fta"})
        ft_pct_cell = row.find("td", {"data-stat": "ft_pct"})
        orb_cell = row.find("td", {"data-stat": "orb"})
        drb_cell = row.find("td", {"data-stat": "drb"})
        trb_cell = row.find("td", {"data-stat": "trb"})
        ast_cell = row.find("td", {"data-stat": "ast"})
        stl_cell = row.find("td", {"data-stat": "stl"})
        blk_cell = row.find("td", {"data-stat": "blk"})
        tov_cell = row.find("td", {"data-stat": "tov"})
        pf_cell = row.find("td", {"data-stat": "pf"})
        pts_cell = row.find("td", {"data-stat": "pts"})


        name = name_cell.text.strip()
        age = age_cell.text.strip()
        team = team_cell.text.strip()
        pos = pos_cell.text.strip()
        g = g_cell.text.strip()    
        gs = gs_cell.text.strip()     
        mp = mp_cell.text.strip()     
        fg = fg_cell.text.strip()     
        fga = fga_cell.text.strip()      
        fg_pct = fg_pct_cell.text.strip()
        p3 = p3_cell.text.strip()     
        pa3 = pa3_cell.text.strip()      
        p3_pct = p3_pct_cell.text.strip()         
        p2 = p2_cell.text.strip()     
        pa2 = pa2_cell.text.strip()         
        p2_pct = p2_pct_cell.text.strip()         
        efg_pct = efg_pct_cell.text.strip() 
        ft = ft_cell.text.strip()     
        fta = fta_cell.text.strip()      
        ft_pct =ft_pct_cell.text.strip()
        orb = orb_cell.text.strip()      
        drb = drb_cell.text.strip()      
        trb = trb_cell.text.strip()      
        ast = ast_cell.text.strip()      
        stl = stl_cell.text.strip()      
        blk = blk_cell.text.strip()      
        tov = tov_cell.text.strip()      
        pf = pf_cell.text.strip()     
        pts = pts_cell.text.strip()



        if name not in names_seen:
            players.append({
                "year": year,
                "player": name,
                "team": team,
                "age": age,
                "pos": pos,
                "games": g,
                "games_started": gs,
                "minutes_played": mp,
                "field_goals": fg,
                "field_goals_attempts": fga,
                "field_goals_percentage": fg_pct,
                "3p_field_goals": p3,
                "3p_field_goals_attempts": pa3,
                "3p_field_goals_percentage": p3_pct,
                "2p_field_goals": p2,
                "2p_field_goals_attempts": pa2,
                "2p_field_goals_percentage": p2_pct,
                "efg_pct": efg_pct,
                "free_throws": ft,
                "free_throws_attempts": fta,
                "free_throws_percentage": ft_pct,
                "offensive_rebounds": orb,
                "defensive_rebounds": drb,
                "total_rebounds": trb,
                "assists": ast,
                "steals": stl,
                "blocks": blk,
                "turnovers": tov,
                "personal_fouls": pf,
                "points":pts
                })
            names_seen.add(name)
        
        # return players

    # dirname = "nba_players_state"
    # if not os.path.exists(dirname):
    #     os.mkdir(dirname)

    df = pd.DataFrame(players)
    df = df.replace('', None) #把''轉換None
           
    # 針對特殊字元進行替換
    df['player'] = df['player'].replace({"à":"a",
                                "á":"a",
                                "ä":"a",
                                "č":"c",
                                "ç":"c",
                                "ć":"c",
                                "Đ":"D",
                                "đ":"d",
                                'à':'a',
                                'À':'A',
                                'á':'a',
                                'Á':'A',
                                'ä':'a',
                                'č':'c',
                                'Č': 'C',
                                'ç': 'C',
                                'ć': 'c',
                                'Đ': 'D',
                                'đ': 'd',
                                'é': 'e',
                                'ë': 'e',
                                'è': 'e',
                                'É': 'E',
                                'Ê': 'E',
                                'í': 'i',
                                'ñ': 'n',
                                'ņ': 'n',
                                'Ş': 'S',
                                'š': 's',
                                'Š': 'S',
                                'ó': 'o',
                                'ö': 'o',
                                'ú': 'u',
                                'ü': 'u',
                                'ý': 'y',
                                'ž': 'z',
                                'Ž': 'Z',
                                'ņ': 'n',
                                'Ģ': 'G',   
                                'ģ': 'g',
                                'Ł': 'L',
                                                                 
                                })
    # 縮寫隊名轉換
    df['team'] = df['team'].replace({"HOU": "Rockets",
                             "GSW": "Warriors",
                             "OKC": "Thunder",
                             "CLE": "Cavaliers",
                             "POR": "Blazers",
                             "NOP": "Pelicans",
                             "LAC": "Clippers",
                             "DAL": "Mavericks",
                             "UTA": "Jazz",
                             "CHI": "Bulls",
                             "SAC": "Kings",
                             "ORL": "Magic",
                             "MEM": "Grizzlies",
                             "WAS": "Wizards",
                             "MIN": "Timberwolves",
                             "PHO": "Suns",
                             "MIA": "Heat",
                             "2TM": "2Teams",
                             "TOR": "Raptors",
                             "BRK": "Nets",
                             "ATL": "Hawks",
                             "DEN": "Nuggets",
                             "DET": "Pistons",
                             "CHO": "Hornets",
                             "BOS": "Celtics",
                             "SAS": "Spurs",
                             "MIL": "Bucks",
                             "NYK": "Knicks",
                             "PHI": "76ers",
                             "IND": "Pacers",
                             "LAL": "Lakers",
                             "3TM": "3Teams"
                            })
 
    df.drop(df.index[-1], inplace=True) # 移除最後一列平均值
    # df.index += 1
    # fn = os.path.join(dirname, f"nba_players_state_{year}.csv")
    # df.to_csv(fn, encoding="utf-8-sig")

    data = df.to_dict(orient='records') # 將 DataFrame 轉換為字典列表
    upload_data_to_mysql_upsert(table_obj=nba_players_state_table, data=data)
    print(f"nba_players_state_{year} has been uploaded to mysql.")

   


if __name__ == '__main__':

    years = list(range(2015,2026))

    for year in years:

        nba_players_state(year)

# print(nba_players_state(2020))