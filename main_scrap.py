from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup as bs
import pandas as pd
import time
import os
import logging
import sys
from functools import reduce

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def initialize_driver():
    try:
        options = Options()
        options.add_argument('--headless')
        driver = webdriver.Chrome(options=options)
        logging.info('Driver initialized successfully.')
        return driver
    except Exception as e:
        logging.error(f'Error initializing driver: {e}')
        raise

def quit_driver(driver):
    driver.quit()
    logging.info('Driver quit successfully.')

def apagar_arquivos_pasta(pasta):
    try:
        for nome_arquivo in os.listdir(pasta):
            caminho_arquivo = os.path.join(pasta, nome_arquivo)
            if os.path.isfile(caminho_arquivo):
                os.remove(caminho_arquivo)
                logging.info(f'Arquivo {nome_arquivo} apagado com sucesso.')
    except Exception as e:
        logging.error(f'Error deleting files in {pasta}: {e}')

def scrap_team_stats(driver, url, stat_category):
    driver.get(url)
    time.sleep(3)

    soup = bs(driver.page_source, 'html.parser')
    # with open('htmls_debug/debug_14_05.html', 'w', encoding='utf-8') as f:
    #     f.write(soup.prettify())
    # logging.debug('HTML saved for debugging.')
    try:
        tbodys = soup.find_all('tbody')
        tbody_teams = tbodys[0]
        tbody_teams_vs = tbodys[1]

        ### TIME ###
        rows = tbody_teams.find_all('tr')
        teams_stats = []

        for row in rows:
            team_data = {}

            team_cell = row.find('th', {'data-stat': 'team'})
            team_name = team_cell.find('a').text.strip() if team_cell.find('a') else team_cell.text.strip()
            team_data['time'] = team_name

            cols = row.find_all('td')
            for col in cols:
                key = col.get('data-stat')
                value = col.text.strip().replace(',', '.')
                try:
                    value = float(value)
                except ValueError:
                    pass
                team_data[key] = value

            teams_stats.append(team_data)
        logging.info(f'Found {len(teams_stats)} teams in {stat_category} stats.')
    except Exception as e:
        logging.error(f'Error parsing team stats: {e}')
        raise

    df = pd.DataFrame(teams_stats)
    df.to_excel(f'times/stats_{stat_category}_teams.xlsx', index=False)
    logging.info(f'Arquivo {stat_category}_times.xlsx salvo com sucesso!')

    ### VS TIME ###
    try:
        rows_vs = tbody_teams_vs.find_all('tr')
        teams_stats_vs = []

        for row in rows_vs:
            team_data_vs = {}

            team_cell_vs = row.find('th', {'data-stat': 'team'})
            team_name_vs = team_cell_vs.find('a').text.strip() if team_cell.find('a') else team_cell.text.strip()
            team_data_vs['time'] = team_name_vs

            cols = row.find_all('td')
            for col in cols:
                key = col.get('data-stat')
                value = col.text.strip().replace(',', '.')
                try:
                    value = float(value)
                except ValueError:
                    pass
                team_data_vs[key] = value

            teams_stats_vs.append(team_data_vs)
        logging.info(f'Found {len(teams_stats_vs)} teams in {stat_category} vs stats.')
    except Exception as e:
        logging.error(f'Error parsing team vs stats: {e}')
        raise

    df_vs = pd.DataFrame(teams_stats_vs)
    df_vs.to_excel(f'times/stats_{stat_category}_teams_vs.xlsx', index=False)

def scrap_player_stats(driver, url, stat_category):
    driver.get(url)
    time.sleep(3)

    soup = bs(driver.page_source, 'html.parser')
    # with open('htmls_debug/debug_08_06.html', 'w', encoding='utf-8') as f:
    #     f.write(soup.prettify())
    # logging.debug('HTML saved for debugging.')
    # sys.exit()
    tbodys = soup.find_all('tbody')
    print(f'Found {len(tbodys)} tbody elements in the page.')
    tbody_players = tbodys[1] if stat_category != 'playing_time' else tbodys[0]

    rows = tbody_players.find_all('tr', attrs={'data-row': True})
    players_stats = []

    try: 
        for row in rows:
            if row.find('td', {'data-stat': 'player'}):
                player_data = {}

                # player_cell = row.find('td', {'data-stat': 'player'})
                # player_name = player_cell.find('a').text.strip() if player_cell.find('a') else player_cell.text.strip()
                # player_data['jogador'] = player_name

                cols = row.find_all('td')
                for col in cols:
                    key = col.get('data-stat')
                    value = col.text.strip().replace(',', '.')
                    try:
                        value = float(value)
                    except ValueError:
                        pass
                    player_data[key] = value

                players_stats.append(player_data)
        logging.info(f'Found {len(players_stats)} players in {stat_category} stats.')
    except Exception as e:
        logging.error(f'Error parsing player stats: {e}')
        raise

    df = pd.DataFrame(players_stats)
    df.to_excel(f'jogadores/stats_{stat_category}_players.xlsx', index=False)
    logging.info(f'Arquivo {stat_category}_jogadores.xlsx salvo com sucesso!')

def concatenate_excel_files_per_sheet(folder_path, output_file):
    files = [f for f in os.listdir(folder_path) if f.endswith('.xlsx')]
    if not files:
        raise FileNotFoundError(f'No Excel files found in {folder_path}')
    
    if os.path.exists(output_file):
        os.remove(output_file)

    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        for f in files:
            file_path = os.path.join(folder_path, f)
            try:
                df = pd.read_excel(file_path)
                if df.empty:
                    print(f'Warning: {file_path} is empty. Skipping.')
                    continue

                sheet_name = os.path.splitext(f)[0].replace('stats_', '').replace('_', '-').title()[:31]
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                logging.info(f'Arquivo {f} concatenado com sucesso!')
            except Exception as e:
                logging.error(f'Error concatenating {f}: {e}')
                raise

def full_merge_data(folder_path, output_file, merge_keys):
    files = [f for f in os.listdir(folder_path) if f.endswith('.xlsx')]
    if not files:
        raise FileNotFoundError(f'No Excel files found in {folder_path}')
    
    df_list = []
    for f in files:
        file_path = os.path.join(folder_path, f)
        try:
            df = pd.read_excel(file_path)
            if df.empty:
                print(f'Warning: {file_path} is empty. Skipping.')
                continue
            df_list.append(df)
            print(f'Arquivo {f} lido com sucesso!')
        except Exception as e:
            print(f'Error reading {f}: {e}')
            continue

    def merge_dfs(left, right):
        merged = pd.merge(left, right, on=merge_keys, how='outer', suffixes=('', '_dup'))
        for col in merged.columns:
            if col.endswith('_dup'):
                base_col = col.replace('_dup', '')
                merged[base_col] = merged[base_col].combine_first(merged[col]) 
                merged.drop(columns=[col], inplace=True)
        return merged

    merged_df = reduce(merge_dfs, df_list)
    merged_df.to_excel(output_file, index=False)
    logging.info(f'Arquivo {f} concatenado com sucesso!')
        


driver = initialize_driver()

apagar_arquivos_pasta('jogadores')
apagar_arquivos_pasta('times')

urls = {
    'standard': 'https://fbref.com/pt/comps/Big5/stats/players/Big-5-European-Leagues-Stats',
    'keeper': 'https://fbref.com/pt/comps/Big5/keepers/players/Big-5-European-Leagues-Stats',
    'keeper_adv': 'https://fbref.com/pt/comps/Big5/keepersadv/players/Big-5-European-Leagues-Stats',
    'shooting': 'https://fbref.com/pt/comps/Big5/shooting/players/Big-5-European-Leagues-Stats',
    'passing': 'https://fbref.com/pt/comps/Big5/passing/players/Big-5-European-Leagues-Stats',
    'passing_types': 'https://fbref.com/pt/comps/Big5/passing_types/players/Big-5-European-Leagues-Stats',
    'gca': 'https://fbref.com/pt/comps/Big5/gca/players/Big-5-European-Leagues-Stats',
    'defense': 'https://fbref.com/pt/comps/Big5/defense/players/Big-5-European-Leagues-Stats',
    'possession': 'https://fbref.com/pt/comps/Big5/possession/players/Big-5-European-Leagues-Stats',
    'misc': 'https://fbref.com/pt/comps/Big5/misc/players/Big-5-European-Leagues-Stats',
    'playing_time': 'https://fbref.com/pt/comps/Big5/playingtime/players/Big-5-European-Leagues-Stats',
}

for stat_category, url in urls.items():
    # scrap_team_stats(driver, url, stat_category)
    scrap_player_stats(driver, url, stat_category)

quit_driver(driver)

full_merge_data('jogadores', 'jogadores/all_stats_jogadores.xlsx',
                 merge_keys=['player', 'nationality', 'position', 'team', 'comp_level', 'age', 'birth_year', 'minutes_90s'])
# concatenate_excel_files('times', 'times/all_stats_times.xlsx')