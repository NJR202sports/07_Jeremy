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
        html = bs.BeautifulSoup(content, 'html.parser')


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
            salary_list.append(salary.text.strip()[1:].replace(',', ''))
        salary_list = salary_list[:-len(year_items)][::len(year_items)]


        # 檢查 player / salary 數量一致
        try:
            if len(player_list) != len(salary_list):
                raise ValueError(
                    f"[資料不一致] {year} 年 {team_name}：players={len(player_list)}, salaries={len(salary_list)}")

            for i in range(len(player_list)):
                all_rows.append({
                    'year': year,                    
                    'player': player_list[i],
                    'team': team_name,
                    'salary': salary_list[i]
                })

        except Exception as e:
            print(f"發生錯誤於 {team_name} {year}：{e}")

        time.sleep(random.uniform(3, 5))  # 輕微延遲，避免封鎖

    # save
    # dirname = "nba_players_salary"
    # if not os.path.exists(dirname):
    #     os.mkdir(dirname)

    df = pd.DataFrame(all_rows)
    df['team_cut'] = df["team"].str.split('-')
    df['team'] = df['team_cut'].str[-1]
    df.drop(columns=['team_cut'], inplace=True)
    df['team'] = df['team'].str.capitalize()
    
  # 轉換名字縮寫
    df['player'] = df['player'].replace({'Z. Risacher': 'Zaccharie Risacher', 
                                'P. Patterson': 'Patrick Patterson', 
                                'J. Poeltl': 'Jakob Poeltl', 
                                'D. Bertāns': 'Dāvis Bertāns', 
                                'D. Lillard': 'Damian Lillard', 
                                'A. Wiggins': 'Andrew Wiggins', 
                                'J. Valančiūnas': 'Jonas Valanciunas', 
                                'T. Sefolosha': 'Thabo Sefolosha', 
                                'C. Aldrich': 'Cole Aldrich', 
                                'M. Žižić': 'Mario Zizic', 
                                'D. Šarić': 'Dario Saric', 
                                'M. Fultz': 'Markelle Fultz', 
                                'O. Prosper': 'Olivier-Maxence Prosper', 
                                'S. Gilgeous-Alexander': 'Shai Gilgeous-Alexander', 
                                'B. Bogdanovic': 'Bogdan Bogdanovic', 
                                'F. Kaminsky': 'Frank Kaminsky', 
                                'C. Felicio': 'Cristiano Felicio',    
                                'R. Jefferson': 'Richard Jefferson', 
                                'C. Villanueva': 'Charlie Villanueva', 
                                'J. Lauvergne': 'Joffrey Lauvergne', 
                                'M. Morris': 'Marcus Morris', 
                                'K. Papanikolaou': 'Kostas Papanikolaou', 
                                'R. Bullock': 'Reggie Bullock', 
                                'S. Dinwiddie': 'Spencer Dinwiddie', 
                                'A. Ajinca': 'Alexis Ajinca', 
                                'M. World': 'Metta World Peace',
                                'M. Speights': 'Marreese Speights', 
                                'J. Michael': 'James Michael McAdoo', 
                                'G. Robinson': 'Glenn Robinson III', 
                                'S. Whittington': 'Shayne Whittington', 
                                'G. Whittington': 'Greg Whittington', 
                                'A. Stoudemire': 'Amar\'e Stoudemire', 
                                'A. Cleveland': 'Alfonzo Cleveland', 
                                'A. Schofield': 'Admiral Schofield', 
                                'A. Pokusevski': 'Aleksej Pokusevski', 
                                'A. Marciulionis': 'Aleksej Marciulionis',
                                "B. Garrett": "Billy Garrett Jr.",
                                "B. Mathurin": "Bennedict Mathurin",
                                "B. Podziemski": "Brandin Podziemski",
                                "B. Scheierman": "Baylor Scheierman",
                                'B. Dejean-Jones':'Bryce Dejean-Jones',
                                'C. Hutchison': 'Chandler Hutchison',
                                'C. Brown': 'Christian Braun',
                                'C. Johnson': 'Chris Johnson',
                                'C. Carrington': 'Chris Carrington',
                                'C. Murray-Boyles': 'Chance Murray-Boyles',
                                "D. Finney-Smith": "Dorian Finney-Smith",
                                "D. Jackson": "Damian Jones",
                                "D. Jones": "Damian Jones",
                                "D. Motiejunas": "Donatas Motiejunas",
                                "D. Walton": "Derrick Walton Jr.",
                                "D.J. Wilson": "D.J. Wilson",
                                "D. Akoon-Purcell": "Darel Akoon-Purcell",
                                "D. Melton": "De'Anthony Melton",
                                'D.J. Augustin': 'D.J. Augustin',
                                'D.J. Wilson': 'D.J. Wilson',
                                'D.J. Stephens': 'D.J. Stephens',
                                'D. Sirvydis': 'Deividas Sirvydis',
                                'D. Washington': 'Duane Washington Jr.',
                                'D. Skapintsev': 'Dmitry Skapintsev',
                                'D.J. Carton': 'D.J. Carton',
                                'D. Jones-Garcia': 'Derrick Jones-Garcia',
                                'E. Mockevicius': 'Egidijus Mockevicius',
                                'E.J. Liddell': 'E.J. Liddell',
                                'F. Gillespie': 'Freddie Gillespie',
                                'G. Yabusele': 'Guerschon Yabusele',
                                'G. Antetokounmpo': 'Giannis Antetokounmpo',
                                'G. Papagiannis': 'Georgios Papagiannis',
                                'G. Kalaitzakis': 'George Kalaitzakis',
                                'H. Highsmith': 'Haywood Highsmith',
                                'I. Hartenstein': 'Isaiah Hartenstein',
                                'I. Quickley': 'Immanuel Quickley',
                                'I. Wainright': 'Ishmail Wainright',
                                'I. Brockington': 'Izaiah Brockington',
                                'J. O\'Bryant': 'Johnny O\'Bryant',
                                'J. Valanciunas': 'Jonas Valanciunas',
                                'J.J. Barea': 'Jose Juan Barea',
                                'K. Collinsworth': 'Kyle Collinsworth',
                                'K. Porzingis': 'Kristaps Porzingis',
                                'K. Towns': 'Karl-Anthony Towns',
                                'K. Caldwell-Pope': 'Kentavious Caldwell-Pope',
                                'K. Antetokounmpo': 'Kostas Antetokounmpo',
                                'L. Richard': 'Leo Richard',
                                'L. Aldridge': 'LaMarcus Aldridge',
                                'L. Galloway': 'Langston Galloway',
                                'L. Jean-Charles': 'Livio Jean-Charles',
                                'L. Wigginton': 'Lindell Wigginton',
                                "M. Carter-Williams": "Michael Carter-Williams",
                                "M. Dellavedova": "Matthew Dellavedova",
                                "M. Kidd-Gilchrist": "Michael Kidd-Gilchrist",
                                "M. Georges-Hunt": "Marcus Georges-Hunt",
                                "M. Kuzminskas": "Mindaugas Kuzminskas",
                                "M. Richardson": "Malachi Richardson",
                                "M. Thornton": "Marcus Thornton",
                                "M. Williams": "Mo Williams",
                                "M. Bagley": "Marvin Bagley III",
                                "M. Derrickson": "Marcus Derrickson",
                                "N. Laprovittola": "Nicolas Laprovittola",
                                "N. Alexander-Walker": "Nickeil Alexander-Walker",
                                "N. Williams-Goss": "Nigel Williams-Goss",
                                "N. Hayes-Davis": "Nate Hayes-Davis",
                                'P. Baldwin': 'Patrick Baldwin Jr.',
                                'Q. Weatherspoon': 'Quinndary Weatherspoon',
                                "R. Westbrook": "Russell Westbrook",
                                "R. Hollis-Jefferson": "Rondae Hollis-Jefferson",
                                'S. Zimmerman': 'Stephen Zimmerman',
                                'S. Harrison': 'Shaquille Harrison',
                                'S. Thornwell': 'Sindarius Thornwell',
                                'S. Pointer': 'Sharmarke Pointer',
                                'S. Mamukelashvili': 'Sandro Mamukelashvili',
                                'S. Mykhailiuk': 'Sviatoslav Mykhailiuk',
                                'S. Fontecchio': 'Simone Fontecchio',
                                'S. Pippen': 'Scotty Pippen Jr.',
                                'T. Antetokounmpo': 'Thanasis Antetokounmpo', 
                                'T. Luwawu-Cabarrot': 'Timothe Luwawu-Cabarrot', 
                                'T. Ferguson': 'Terrance Ferguson', 
                                'T. McKinney-Jones': 'Terrence McKinney-Jones', 
                                'T. Horton-Tucker': 'Talen Horton-Tucker', 
                                'T. Alexander': 'Ty-Shon Alexander', 
                                'V. Wembanyama': 'Victor Wembanyama',
                                'V. Williams': 'Vince Williams',
                                'X. Rathan-Mayes': 'Xavier Rathan-Mayes',
                                'J. Blossomgame': 'Jaron Blossomgame',
                                'J. Champagnie': 'Justin Champagnie',
                                'J. Freeman-Liberty': 'Javon Freeman-Liberty',
                                'J. Hernangomez': 'Juancho Hernangómez',
                                'J. Hood-Schifino': 'Jalen Hood-Schifino',
                                'J. McLaughlin': 'Jordan McLaughlin',
                                'J. Robinson-Earl': 'Jeremiah Robinson-Earl',
                                'J. Toscano-Anderson': 'Juan Toscano-Anderson',
                                'J. Vanderbilt': 'Jarred Vanderbilt',
                                'J. Williams': 'Jaylin Williams',
                                'J. Wright-Foreman': 'Jahvon Quinerly',
                                'K. Jakucionis': 'Kasparas Jakucionis',
                                'K. Lofton': 'Kenneth Lofton Jr.',
                                'K. McCullar': 'Kevin McCullar Jr.',
                                'M. Cleveland': 'Maturin Cleveland',
                                'M. Frazier': 'Michael Frazier II',
                                'M. Kabengele': 'Mfiondu Kabengele',
                                'M. Porter': 'Michael Porter Jr.',
                                'M. Robinson': 'Mitchell Robinson',
                                'M. Wright': 'Michael Wright',
                                'T. Haliburton': 'Tyrese Haliburton',
                                'T. Jackson-Davis': 'Trayce Jackson-Davis',
                                'T. Shannon': 'Terrence Shannon Jr.',
                                'W. Carter': 'Wendell Carter Jr.',
                                'W. Cauley-Stein': 'Willie Cauley-Stein',
                                'W. Clayton': 'Wilbur Clayton',
                                'W. Hernangomez': 'Willy Hernangómez',
                                'W. Moore': 'Wendell Moore Jr.',
                                'Y. Niederhauser': 'Yves Niederhauser',

                                }) 
    df['player'] = df['player'].str.replace('.', '', regex=False) #把'.'轉換''
    # df.index += 1
    # fn = os.path.join(dirname, f"nba_players_salary_{year}.csv")
    # df.to_csv(fn, encoding="utf-8-sig")

    data = df.to_dict(orient='records') # 將 DataFrame 轉換為字典列表
    upload_data_to_mysql_upsert(table_obj=nba_players_salary_table, data=data)
    print(f"nba_players_salary_{year} has been uploaded to mysql.")

   




if __name__ == '__main__':

    years = list(range(2015,2026))

    for year in years:

        nba_players_salary(year)

# print(nba_players_salary(2018))