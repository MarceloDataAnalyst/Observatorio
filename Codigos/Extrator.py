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

def save_extracted_file(file_path, file_name, month_folder, output_dir):
    """
    Salva o arquivo extraído na pasta permanente.
    
    Args:
        file_path (str): Caminho do arquivo temporário
        file_name (str): Nome do arquivo original
        month_folder (str): Pasta do mês (ex: '202407')
        output_dir (str): Diretório de saída permanente
    
    Returns:
        str: Caminho do arquivo salvo ou None se houver erro
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logging.info(f"Diretório '{output_dir}' criado.")
    
    # Criar nome do arquivo com prefixo do mês
    base_name, ext = os.path.splitext(file_name)
    permanent_filename = f"{month_folder}_{base_name}{ext}"
    permanent_filepath = os.path.join(output_dir, permanent_filename)
    
    try:
        # Copiar arquivo da pasta temporária para a permanente
        shutil.copy2(file_path, permanent_filepath)
        file_size = os.path.getsize(permanent_filepath) / (1024*1024)  # Tamanho em MB
        logging.info(f"Arquivo '{permanent_filename}' salvo ({file_size:.2f} MB)")
        return permanent_filepath
    except Exception as e:
        logging.error(f"Erro ao salvar arquivo '{permanent_filename}': {e}")
        return None

def extract_from_ftp_with_7z(ftp_host, base_ftp_path, output_dir='dados_caged_extraidos', processed_folders_file='processed_caged_folders.txt'):
    """
    Conecta a um servidor FTP, navega por uma estrutura de pastas YYYY/YYYYMM,
    baixa e extrai arquivos .7z, salvando os arquivos extraídos em pasta permanente.

    Args:
        ftp_host (str): Endereço do servidor FTP (ex: 'ftp.mtps.gov.br').
        base_ftp_path (str): Caminho raiz dos microdados (ex: 'pdet/microdados/NOVO CAGED/').
        output_dir (str): Diretório permanente para salvar os arquivos extraídos.
        processed_folders_file (str): Arquivo para registrar as pastas YYYYMM já processadas.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logging.info(f"Diretório '{output_dir}' criado.")

    # Carregar pastas já processadas
    processed_folders = set()
    if os.path.exists(processed_folders_file):
        with open(processed_folders_file, 'r') as f:
            for line in f:
                processed_folders.add(line.strip())
        logging.info(f"Carregadas {len(processed_folders)} pastas já processadas.")

    all_dataframes = {}
    saved_files = []  # Lista para rastrear arquivos salvos

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
                return {}, []

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
                        # --- NOVA ABORDAGEM: .7z em pasta temporária ---
                        with tempfile.TemporaryDirectory() as temp_download_dir:
                            temp_sevenz_filepath = os.path.join(temp_download_dir, sevenz_filename)
                            
                            logging.info(f"Baixando '{sevenz_filename}' para pasta temporária...")
                            with open(temp_sevenz_filepath, 'wb') as local_file:
                                ftp.retrbinary(f"RETR {sevenz_filename}", local_file.write)
                            logging.info(f"Download de '{sevenz_filename}' concluído na pasta temporária.")

                            # Extrair e processar o arquivo .7z
                            try:
                                # Criar diretório temporário para extração
                                with tempfile.TemporaryDirectory() as temp_extract_dir:
                                    logging.info(f"Extraindo '{sevenz_filename}' para diretório temporário...")
                                    
                                    with py7zr.SevenZipFile(temp_sevenz_filepath, mode='r') as archive:
                                        archive.extractall(path=temp_extract_dir)
                                    
                                    logging.info(f"Extração de '{sevenz_filename}' concluída.")
                                    
                                    # Listar arquivos extraídos
                                    extracted_files = os.listdir(temp_extract_dir)
                                    logging.info(f"Arquivos extraídos: {extracted_files}")
                                    
                                    # Processar arquivos CSV/TXT extraídos
                                    for extracted_file in extracted_files:
                                        if extracted_file.lower().endswith('.csv') or extracted_file.lower().endswith('.txt'):
                                            temp_extracted_path = os.path.join(temp_extract_dir, extracted_file)
                                            
                                            # --- SALVAR ARQUIVO NA PASTA PERMANENTE ---
                                            permanent_file_path = save_extracted_file(
                                                temp_extracted_path, 
                                                extracted_file, 
                                                month_folder, 
                                                output_dir
                                            )
                                            
                                            if permanent_file_path:
                                                saved_files.append(permanent_file_path)
                                                
                                                # Ler o arquivo da pasta permanente para criar DataFrame
                                                logging.info(f"Lendo '{extracted_file}' da pasta permanente.")
                                                
                                                try:
                                                    # Tenta ler com ';' e 'latin1'
                                                    df_temp = pd.read_csv(permanent_file_path, sep=';', encoding='latin1', on_bad_lines='skip')
                                                    all_dataframes[f"{month_folder}_{extracted_file}"] = df_temp
                                                    logging.info(f"DataFrame para '{extracted_file}' criado com sucesso (latin1, sep=';') - Shape: {df_temp.shape}")
                                                except Exception as e:
                                                    logging.warning(f"Erro ao ler CSV/TXT '{extracted_file}' com latin1 e sep=';': {e}. Tentando 'utf-8' e sep=','.")
                                                    try:
                                                        # Tenta ler com ',' e 'utf-8'
                                                        df_temp = pd.read_csv(permanent_file_path, sep=',', encoding='utf-8', on_bad_lines='skip')
                                                        all_dataframes[f"{month_folder}_{extracted_file}"] = df_temp
                                                        logging.info(f"DataFrame para '{extracted_file}' criado com sucesso (utf-8, sep=',') - Shape: {df_temp.shape}")
                                                    except Exception as e_retry:
                                                        logging.warning(f"Tentando com cp1252 e sep=';' para '{extracted_file}'...")
                                                        try:
                                                            # Tenta ler com ';' e 'cp1252' (Windows-1252)
                                                            df_temp = pd.read_csv(permanent_file_path, sep=';', encoding='cp1252', on_bad_lines='skip')
                                                            all_dataframes[f"{month_folder}_{extracted_file}"] = df_temp
                                                            logging.info(f"DataFrame para '{extracted_file}' criado com sucesso (cp1252, sep=';') - Shape: {df_temp.shape}")
                                                        except Exception as e_final:
                                                            logging.error(f"Falha ao ler CSV/TXT '{extracted_file}' com todas as tentativas de codificação: {e_final}. Arquivo mantido em disco para análise manual.")

                            except Exception as e:
                                logging.error(f"Erro ao extrair ou processar .7z '{sevenz_filename}': {e}", exc_info=True)
                        
                        # O arquivo .7z é automaticamente deletado quando sai do bloco with tempfile.TemporaryDirectory()

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
    
    return all_dataframes, saved_files

# --- Exemplo de uso ---
ftp_host = 'ftp.mtps.gov.br'
base_ftp_path = 'pdet/microdados/NOVO CAGED/'
output_directory = 'dados_caged_extraidos'  # Pasta permanente para CSV/TXT
processed_folders_log = 'caged_folders_log.txt'

logging.info("Iniciando extração do CAGED...")
all_caged_dfs, saved_file_list = extract_from_ftp_with_7z(ftp_host, base_ftp_path, output_directory, processed_folders_log)

# Relatório final
logging.info("\n" + "="*60)
logging.info("RELATÓRIO FINAL DA EXTRAÇÃO")
logging.info("="*60)

if all_caged_dfs:
    logging.info(f"✅ Extração concluída com sucesso!")
    logging.info(f"📊 {len(all_caged_dfs)} DataFrames foram gerados na memória")
    logging.info(f"💾 {len(saved_file_list)} arquivos foram salvos permanentemente")
    
    logging.info(f"\n📁 Arquivos salvos em '{output_directory}':")
    for saved_file in saved_file_list:
        filename = os.path.basename(saved_file)
        file_size = os.path.getsize(saved_file) / (1024*1024)  # MB
        logging.info(f"   📄 {filename} ({file_size:.2f} MB)")
    
    logging.info(f"\n📊 DataFrames disponíveis na memória:")
    for key, df in all_caged_dfs.items():
        logging.info(f"   📈 {key}: {df.shape[0]:,} linhas x {df.shape[1]} colunas")
        
else:
    logging.warning("❌ Nenhum DataFrame foi gerado ou ocorreu um erro significativo.")

logging.info("="*60)
