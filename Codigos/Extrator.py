import ftplib
import os
import io
import py7zr
import pandas as pd
import logging
import tempfile
import shutil
from datetime import datetime

# Configurar logging para melhor visibilidade
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_ftp_file_list(ftp_conn):
    """Obtém a lista de arquivos e diretórios no diretório atual do FTP."""
    try:
        return ftp_conn.nlst()
    except ftplib.error_perm as e:
        logging.warning(f"nlst() falhou. Tentando list() para depuração se necessário: {e}")
        return []

def extract_from_ftp_with_7z(ftp_host, base_ftp_path, download_dir='dados_caged_7z', processed_folders_file='processed_caged_folders.txt'):
    """
    Conecta a um servidor FTP, navega por uma estrutura de pastas YYYY/YYYYMM,
    baixa e extrai arquivos .7z, processando apenas novas pastas YYYYMM.

    Args:
        ftp_host (str): Endereço do servidor FTP (ex: 'ftp.mtps.gov.br').
        base_ftp_path (str): Caminho raiz dos microdados (ex: 'pdet/microdados/NOVO CAGED/').
        download_dir (str): Diretório local para salvar os arquivos baixados e extraídos.
        processed_folders_file (str): Arquivo para registrar as pastas YYYYMM já processadas.
    """
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
        logging.info(f"Diretório '{download_dir}' criado.")

    # Carregar pastas já processadas
    processed_folders = set()
    if os.path.exists(processed_folders_file):
        with open(processed_folders_file, 'r') as f:
            for line in f:
                processed_folders.add(line.strip())
        logging.info(f"Carregadas {len(processed_folders)} pastas já processadas.")

    all_dataframes = {}

    try:
        with ftplib.FTP(ftp_host, encoding='latin-1') as ftp:
            logging.info(f"Conectando a {ftp_host}...")
            ftp.login() # Login anônimo
            logging.info("Login FTP realizado com sucesso.")

            # Navegar para o caminho base
            try:
                ftp.cwd(base_ftp_path)
                logging.info(f"Navegou para o diretório base: {base_ftp_path}")
            except ftplib.error_perm as e:
                logging.error(f"Não foi possível navegar para o diretório base '{base_ftp_path}': {e}. Verifique o caminho.")
                return {}

            # Listar anos (ex: '2024', '2025')
            year_dirs = [d for d in get_ftp_file_list(ftp) if d.isdigit() and len(d) == 4 and d in ['2024', '2025']]
            logging.info(f"Anos encontrados (filtrados para 2024 e 2025): {year_dirs}")

            for year in sorted(year_dirs): # Processar anos em ordem
                try:
                    ftp.cwd(year) # Entra na pasta do ano
                    logging.info(f"Navegou para o ano: {year}")
                except ftplib.error_perm as e:
                    logging.warning(f"Não foi possível entrar no diretório do ano '{year}': {e}. Pulando este ano.")
                    continue

                # Listar meses (ex: '202401', '202402')
                month_dirs = [d for d in get_ftp_file_list(ftp) if d.isdigit() and len(d) == 6 and d.startswith(year)]
                logging.info(f"Meses encontrados para {year}: {month_dirs}")

                for month_folder in sorted(month_dirs): # Processar meses em ordem
                    full_month_path_id = os.path.join(year, month_folder) # Ex: '2025/202501'

                    if full_month_path_id in processed_folders:
                        logging.info(f"Pasta '{full_month_path_id}' já processada. Ignorando.")
                        continue

                    logging.info(f"Processando nova pasta: {full_month_path_id}")
                    try:
                        ftp.cwd(month_folder) # Entra na pasta do mês
                    except ftplib.error_perm as e:
                        logging.warning(f"Não foi possível entrar no diretório do mês '{month_folder}': {e}. Pulando este mês.")
                        continue

                    # Listar arquivos .7z dentro da pasta do mês
                    sevenz_files = [f for f in get_ftp_file_list(ftp) if f.lower().endswith('.7z')]
                    logging.info(f"Arquivos .7z encontrados em {full_month_path_id}: {sevenz_files}")

                    for sevenz_filename in sevenz_files:
                        local_sevenz_filepath = os.path.join(download_dir, sevenz_filename)
                        
                        logging.info(f"Baixando '{sevenz_filename}' para '{local_sevenz_filepath}'...")
                        with open(local_sevenz_filepath, 'wb') as local_file:
                            ftp.retrbinary(f"RETR {sevenz_filename}", local_file.write)
                        logging.info(f"Download de '{sevenz_filename}' concluído.")

                        # Extrair e processar o arquivo .7z
                        try:
                            # Criar diretório temporário para extração
                            with tempfile.TemporaryDirectory() as temp_extract_dir:
                                logging.info(f"Extraindo '{sevenz_filename}' para diretório temporário...")
                                
                                # --- NOVA ABORDAGEM: Usar extractall() ---
                                with py7zr.SevenZipFile(local_sevenz_filepath, mode='r') as archive:
                                    archive.extractall(path=temp_extract_dir)
                                
                                logging.info(f"Extração de '{sevenz_filename}' concluída.")
                                
                                # Listar arquivos extraídos
                                extracted_files = os.listdir(temp_extract_dir)
                                logging.info(f"Arquivos extraídos: {extracted_files}")
                                
                                # Processar arquivos CSV/TXT extraídos
                                for extracted_file in extracted_files:
                                    if extracted_file.lower().endswith('.csv') or extracted_file.lower().endswith('.txt'):
                                        extracted_file_path = os.path.join(temp_extract_dir, extracted_file)
                                        logging.info(f"Lendo '{extracted_file}' do diretório temporário.")
                                        
                                        try:
                                            # Tenta ler com ';' e 'latin1'
                                            df_temp = pd.read_csv(extracted_file_path, sep=';', encoding='latin1', on_bad_lines='skip')
                                            all_dataframes[f"{month_folder}_{extracted_file}"] = df_temp
                                            logging.info(f"DataFrame para '{extracted_file}' (de '{sevenz_filename}') criado com sucesso (latin1, sep=';').")
                                        except Exception as e:
                                            logging.warning(f"Erro ao ler CSV/TXT '{extracted_file}' com latin1 e sep=';': {e}. Tentando 'utf-8' e sep=','.")
                                            try:
                                                # Tenta ler com ',' e 'utf-8'
                                                df_temp = pd.read_csv(extracted_file_path, sep=',', encoding='utf-8', on_bad_lines='skip')
                                                all_dataframes[f"{month_folder}_{extracted_file}"] = df_temp
                                                logging.info(f"DataFrame para '{extracted_file}' (de '{sevenz_filename}') criado com sucesso (utf-8, sep=',').")
                                            except Exception as e_retry:
                                                logging.warning(f"Tentando com cp1252 e sep=';' para '{extracted_file}'...")
                                                try:
                                                    # Tenta ler com ';' e 'cp1252' (Windows-1252)
                                                    df_temp = pd.read_csv(extracted_file_path, sep=';', encoding='cp1252', on_bad_lines='skip')
                                                    all_dataframes[f"{month_folder}_{extracted_file}"] = df_temp
                                                    logging.info(f"DataFrame para '{extracted_file}' (de '{sevenz_filename}') criado com sucesso (cp1252, sep=';').")
                                                except Exception as e_final:
                                                    logging.error(f"Falha ao ler CSV/TXT '{extracted_file}' do '{sevenz_filename}' com todas as tentativas de codificação: {e_final}. Arquivo ignorado.")

                        except Exception as e:
                            logging.error(f"Erro ao extrair ou processar .7z '{sevenz_filename}': {e}", exc_info=True)
                        
                        # Opcional: Remover o arquivo .7z baixado após a extração para economizar espaço
                        # os.remove(local_sevenz_filepath)

                    # Após processar a pasta do mês, retorna ao diretório do ano
                    ftp.cwd('..')
                    # Marca a pasta como processada
                    processed_folders.add(full_month_path_id)
                    with open(processed_folders_file, 'a') as f:
                        f.write(f"{full_month_path_id}\n")
                    logging.info(f"Pasta '{full_month_path_id}' marcada como processada.")


                ftp.cwd('..') # Retorna para o diretório base 'NOVO CAGED/'
            
    except ftplib.all_errors as e:
        logging.error(f"Erro de FTP: {e}")
    except Exception as e:
        logging.error(f"Ocorreu um erro inesperado: {e}", exc_info=True)
    
    return all_dataframes

# --- Exemplo de uso ---
ftp_host = 'ftp.mtps.gov.br'
base_ftp_path = 'pdet/microdados/NOVO CAGED/'
download_directory = 'dados_caged_processados'
processed_folders_log = 'caged_folders_log.txt'

logging.info("Iniciando extração do CAGED...")
all_caged_dfs = extract_from_ftp_with_7z(ftp_host, base_ftp_path, download_directory, processed_folders_log)

if all_caged_dfs:
    logging.info(f"Extração concluída. {len(all_caged_dfs)} DataFrames foram gerados.")
    for key, df in all_caged_dfs.items():
        logging.info(f"--- DataFrame '{key}' ---")
        logging.info(f"Shape: {df.shape}")
        # print(df.head()) # Descomente para ver as primeiras linhas de cada DF
else:
    logging.warning("Nenhum DataFrame foi gerado ou ocorreu um erro significativo.")
