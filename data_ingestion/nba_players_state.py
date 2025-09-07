import urllib.request as req
import os
from bs4 import BeautifulSoup as bs
import pandas as pd
import numpy as np


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
        e_fga_pct_cell = row.find("td", {"data-stat": "efg_pct"})
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
        g = int(g_cell.text.strip()) if g_cell.text.strip() != '' else None
        gs = int(gs_cell.text.strip()) if gs_cell.text.strip() != '' else None
        mp = int(mp_cell.text.strip()) if mp_cell.text.strip() != '' else None
        fg = int(fg_cell.text.strip()) if fg_cell.text.strip() != '' else None
        fga = int(fga_cell.text.strip()) if fga_cell.text.strip() != '' else None
        fg_pct = float(fg_pct_cell.text.strip()) if fg_pct_cell.text.strip() != '' else None
        p3 = int(p3_cell.text.strip()) if p3_cell.text.strip() != '' else None
        pa3 = int(pa3_cell.text.strip()) if pa3_cell.text.strip() != '' else None
        p3_pct = float(p3_pct_cell.text.strip()) if p3_pct_cell.text.strip() != '' else None
        p2 = int(p2_cell.text.strip()) if p2_cell.text.strip() != '' else None
        pa2 = int(pa2_cell.text.strip())    if pa2_cell.text.strip() != '' else None
        p2_pct = float(p2_pct_cell.text.strip()) if p2_pct_cell.text.strip() != '' else None
        efg_pct = float(efg_pct_cell.text.strip()) if efg_pct_cell.text.strip() != '' else None
        ft = int(ft_cell.text.strip()) if ft_cell.text.strip() != '' else None
        fta = int(fta_cell.text.strip()) if fta_cell.text.strip() != '' else None
        ft_pct =float(ft_pct_cell.text.strip()) if ft_pct_cell.text.strip() != '' else None
        orb = int(orb_cell.text.strip()) if orb_cell.text.strip() != '' else None
        drb = int(drb_cell.text.strip()) if drb_cell.text.strip() != '' else None
        trb = int(trb_cell.text.strip()) if trb_cell.text.strip() != '' else None
        ast = int(ast_cell.text.strip()) if ast_cell.text.strip() != '' else None
        stl = int(stl_cell.text.strip()) if stl_cell.text.strip() != '' else None
        blk = int(blk_cell.text.strip()) if blk_cell.text.strip() != '' else None
        tov = int(tov_cell.text.strip()) if tov_cell.text.strip() != '' else None
        pf = int(pf_cell.text.strip()) if pf_cell.text.strip() != '' else None
        pts = int(pts_cell.text.strip()) if pts_cell.text.strip() != '' else None



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

    dirname = "nba_players_merge"
    if not os.path.exists(dirname):
        os.mkdir(dirname)

    df = pd.DataFrame(players)
    df = df.replace({pd.NaT: None, np.nan: None}) #把NaN轉換回None
    df['player'] = df['player'].str.replace('Nikola Jokić', 'Nikola Jokic', regex=False)
    df['player'] = df['player'].str.replace('Nikola Jović', 'Nikola Jovic', regex=False)
    
    df['player'] = df['player'].str.replace('Nikola Vučević', 'Nikola Vucevic', regex=False)
    df['player'] = df['player'].str.replace('Bojan Bogdanović', 'Bojan Bogdanovic', regex=False)
    df['player'] = df['player'].str.replace('Luka Dončić', 'Luka Doncic', regex=False)
    df['player'] = df['player'].str.replace('Alperen Şengün', 'Alperen Sengun', regex=False)
    df['player'] = df['player'].str.replace('Goran Dragić', 'Goran Dragic', regex=False)
    df['player'] = df['player'].str.replace('Tidjane Salaün', 'Tidjane Salaun', regex=False)
    df['player'] = df['player'].str.replace('Dario Šarić', 'Dario Saric', regex=False)
    
    df['player'] = df['player'].str.replace('Anžejs Pasečņiks', 'Anzejs Pasecniks', regex=False)
    df['player'] = df['player'].str.replace('Dāvis Bertāns', 'Davis Bertans', regex=False)
    df['player'] = df['player'].str.replace('Jusuf Nurkić', 'Jusuf Nurkic', regex=False)
    df['player'] = df['player'].str.replace('Čedomir Vitkovac', 'Cedomir Vitkovac', regex=False)
    df['player'] = df['player'].str.replace('Žan Mark Šiško', 'Zan Mark Sisko', regex=False)
    df['player'] = df['player'].str.replace('Žan Mark Sisko', 'Zan Mark Sisko', regex=False)
    df['player'] = df['player'].str.replace('Šarūnas Vasilevičius', 'Sarunas Vasiljevicius', regex=False)
    df['player'] = df['player'].str.replace('Šarūnas Jasikevičius', 'Sarunas Jasikevicius', regex=False)
    df['player'] = df['player'].str.replace('Šarūnas Marčiulionis', 'Sarunas Marciulionis', regex=False)
    df['player'] = df['player'].str.replace('Čedomir Vitkovac', 'Cedomir Vitkovac', regex=False)
    df['player'] = df['player'].str.replace('Dāvis Bertāns', 'Davis Bertans', regex=False)
    df['player'] = df['player'].str.replace('Olivier-Maxence Prosper', 'O. Prosper', regex=False)
    df['player'] = df['player'].str.replace('Ante Žižić', 'Ante Zizic', regex=False)
    df['player'] = df['player'].str.replace('Žan Mark Šiško', 'Zan Mark Sisko', regex=False)
    df['player'] = df['player'].str.replace('Dario Šarić', 'Dario Saric', regex=False)  
   
    df['player'] = df['player'].str.replace('Čedomir Vitkovac', 'Cedomir Vitkovac', regex=False)
    df['player'] = df['player'].str.replace('Dāvis Bertāns', 'Davis Bertans', regex=False)
    df['player'] = df['player'].str.replace('Boban Marjanović', 'Boban Marjanovic', regex=False)




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
    fn = os.path.join(dirname, f"nba_players_state_{year}.csv")
    df.to_csv(fn, encoding="utf-8-sig", index=False)
    print(f"✅ {year}完成，處理 {len(df)} 筆記錄到{fn}")
    
       
    return df



if __name__ == '__main__':

    years = list(range(2015,2026))

    for year in years:

        nba_players_state(year)

# print(nba_players_state(2020))