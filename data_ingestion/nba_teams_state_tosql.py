
# 球隊資料 Total state
import urllib.request as req
import os
import bs4 as bs
import pandas as pd
from data_ingestion.mysql import upload_data_to_mysql, upload_data_to_mysql_upsert ,nba_teams_state_table

def nba_teams_state(year:int):

    url = f"https://www.basketball-reference.com/leagues/NBA_{year}.html"
    resp = req.urlopen(url)
    content = resp.read()
    html = bs.BeautifulSoup(content, "html.parser")

    table = html.find("div", {"id": "all_totals_team-opponent"}).find("tbody")
    rows = table.find_all("tr")

    teams = []
    team_seen = set()

    for row in rows:
        # 跳過空列或是有 'class=thead' 的分隔列
        if row.get("class") == ['thead']:
            continue

        # 抓球員名字與隊伍代碼
        team_cell = row.find("td", {"data-stat": "team"})
        g_cell = row.find("td", {"data-stat": "g"})
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


        team = team_cell.text.strip()
        g = g_cell.text.strip()
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
        ft = ft_cell.text.strip()
        fta = fta_cell.text.strip()
        ft_pct = ft_pct_cell.text.strip()
        orb = orb_cell.text.strip()
        drb = drb_cell.text.strip()
        trb = trb_cell.text.strip()
        ast = ast_cell.text.strip()
        stl = stl_cell.text.strip()
        blk = blk_cell.text.strip()
        tov = tov_cell.text.strip()
        pf = pf_cell.text.strip()
        pts = pts_cell.text.strip()



        if team not in team_seen:
            teams.append({
                "year": year,
                "team": team,
                "games": g,
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
            team_seen.add(team)

    
    dirname = "nba_teams_state"
    if not os.path.exists(dirname):
        os.mkdir(dirname)

    df = pd.DataFrame(teams)
    df['team'] = df['team'].str.replace('*', '', regex=False)
    df['team_cut'] = df["team"].str.split(' ')
    df['team'] = df['team_cut'].str[-1]
    df.drop(columns=['team_cut'], inplace=True)
    # df.index += 1
    # fn = os.path.join(dirname, f"nba_teams_state_{year}.csv")
    # df.to_csv(fn, encoding="utf-8-sig")

    data = df.to_dict(orient='records') # 將 DataFrame 轉換為字典列表
    upload_data_to_mysql_upsert(table_obj=nba_teams_state_table, data=data)
    print(f"nba_teams_state_{year} has been uploaded to mysql.")

    return df

if __name__ == '__main__':

    years = list(range(2015,2026))

    for year in years:

        nba_teams_state(year)

# print(nba_teams_state(2023))