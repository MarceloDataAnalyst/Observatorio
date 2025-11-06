import ftplib
import os
import io
import py7zr # Nova biblioteca
import pandas as pd
import logging
from datetime import datetime # Para obter o ano/mês atual

# Configurar logging para melhor visibilidade
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_ftp_file_list(ftp_conn):
    """Obtém a lista de arquivos e diretórios no diretório atual do FTP."""
    try:
        return ftp_conn.nlst()
    except ftplib.error_perm as e:
        # Se nlst falhar (ex: diretório vazio ou permissão negada), tentar list().
        # list() retorna detalhes que precisam ser parseados para nomes de arquivos/dir.
        # Para simplificação, vamos assumir que nlst() funciona para diretórios com conteúdo ou vazios.
        # Para um ambiente de produção mais robusto, um parsing de ftp_conn.dir() seria necessário.
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
        with ftplib.FTP(ftp_host) as ftp:
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
            year_dirs = [d for d in get_ftp_file_list(ftp) if d.isdigit() and len(d) == 4]
            logging.info(f"Anos encontrados: {year_dirs}")

            for year in sorted(year_dirs): # Processar anos em ordem
                ftp.cwd(year) # Entra na pasta do ano
                logging.info(f"Navegou para o ano: {year}")

                # Listar meses (ex: '202401', '202402')
                month_dirs = [d for d in get_ftp_file_list(ftp) if d.isdigit() and len(d) == 6 and d.startswith(year)]
                logging.info(f"Meses encontrados para {year}: {month_dirs}")

                for month_folder in sorted(month_dirs): # Processar meses em ordem
                    full_month_path_id = os.path.join(year, month_folder) # Ex: '2025/202501'

                    if full_month_path_id in processed_folders:
                        logging.info(f"Pasta '{full_month_path_id}' já processada. Ignorando.")
                        continue

                    logging.info(f"Processando nova pasta: {full_month_path_id}")
                    ftp.cwd(month_folder) # Entra na pasta do mês

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
                            with py7zr.SevenZipFile(local_sevenz_filepath, mode='r') as archive:
                                # Lista os nomes dos arquivos dentro do .7z
                                inner_filenames = archive.getnames()
                                
                                for inner_file_name in inner_filenames:
                                    if inner_file_name.lower().endswith('.csv') or inner_file_name.lower().endswith('.txt'):
                                        logging.info(f"Lendo '{inner_file_name}' de dentro de '{sevenz_filename}'.")
                                        
                                        # Abre o arquivo de dentro do 7z para leitura como bytes
                                        with archive.open(inner_file_name) as member_file:
                                            # Envolve o stream de bytes com TextIOWrapper para ler como texto
                                            content = io.TextIOWrapper(member_file, encoding='utf-8')
                                            
                                            try:
                                                # Tenta ler com ';' e 'utf-8'
                                                df_temp = pd.read_csv(content, sep=';', encoding='utf-8', on_bad_lines='skip')
                                                all_dataframes[f"{month_folder}_{inner_file_name}"] = df_temp
                                                logging.info(f"DataFrame para '{inner_file_name}' (de '{sevenz_filename}') criado com sucesso (UTF-8, sep=';').")
                                            except Exception as e:
                                                logging.warning(f"Erro ao ler CSV/TXT '{inner_file_name}' com UTF-8 e sep=';': {e}. Tentando 'latin1' e sep=','. (ou outros padrões do CAGED)")
                                                try:
                                                    # Resetar o ponteiro do stream se for necessário re-tentar
                                                    content.seek(0)
                                                    # Tenta ler com ',' e 'latin1'
                                                    df_temp = pd.read_csv(content, sep=',', encoding='latin1', on_bad_lines='skip')
                                                    all_dataframes[f"{month_folder}_{inner_file_name}"] = df_temp
                                                    logging.info(f"DataFrame para '{inner_file_name}' (de '{sevenz_filename}') criado com sucesso (latin1, sep=',').")
                                                except Exception as e_retry:
                                                    logging.error(f"Falha ao ler CSV/TXT '{inner_file_name}' do '{sevenz_filename}' com UTF-8/latin1 e sep=';',sep=',' : {e_retry}. Arquivo ignorado.")

                        except Exception as e:
                            logging.error(f"Erro ao extrair ou processar .7z '{sevenz_filename}': {e}")
                        
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
        logging.error(f"Ocorreu um erro inesperado: {e}", exc_info=True) # exc_info=True para logar o traceback
    
    return all_dataframes

# --- Exemplo de uso ---
ftp_host = 'ftp.mtps.gov.br'
base_ftp_path = 'pdet/microdados/NOVO CAGED/'
download_directory = 'dados_caged_processados' # Onde os arquivos extraídos (temporariamente) e o .7z serão salvos
processed_folders_log = 'caged_folders_log.txt' # Arquivo para registrar as pastas já processadas

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
