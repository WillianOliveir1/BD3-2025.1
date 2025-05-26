"""
Script para configuração automática do Hadoop em diferentes sistemas operacionais.
Principalmente necessário para Windows, onde bibliotecas nativas do Hadoop precisam ser configuradas.
"""

import os
import platform
import sys
import urllib.request
from pathlib import Path
from src.infrastructure.logging_config import LOG_EMOJIS, setup_logger

# Configuração do logger usando a função centralizada
logger = setup_logger(__name__)

HADOOP_VERSION = "3.2.2"
WINUTILS_BASE_URL = f"https://github.com/cdarlint/winutils/raw/master/hadoop-{HADOOP_VERSION}/bin/"
REQUIRED_FILES = ["winutils.exe", "hadoop.dll"]

# Obtém o diretório raiz do projeto (3 níveis acima deste arquivo)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

def setup_hadoop_windows():
    """
    Configura o ambiente Hadoop para Windows.
    
    Returns:
        bool: True se a configuração foi bem sucedida, False caso contrário
    """
    hadoop_home = PROJECT_ROOT / "hadoop"
    bin_dir = hadoop_home / "bin"    
    
    # Cria diretórios
    try:
        bin_dir.mkdir(parents=True, exist_ok=True)
        logger.info("%s Diretório criado: %s", LOG_EMOJIS['FILE'], bin_dir)
    except (OSError, PermissionError) as e:
        logger.error("%s Erro ao criar diretório %s: %s", LOG_EMOJIS['ERROR'], bin_dir, e)
        return False    
    
    # Download dos arquivos necessários
    for file in REQUIRED_FILES:
        url = WINUTILS_BASE_URL + file
        target_path = bin_dir / file
        
        if not target_path.exists():
            logger.info("%s Baixando %s...", LOG_EMOJIS['DOWNLOAD'], file)
            try:
                urllib.request.urlretrieve(url, str(target_path))
                logger.info("%s %s baixado com sucesso", LOG_EMOJIS['SUCCESS'], file)
            except (urllib.error.URLError, urllib.error.HTTPError, OSError) as e:
                logger.error("%s Erro ao baixar %s: %s", LOG_EMOJIS['ERROR'], file, e)
                return False
        else:
            logger.info("%s %s já existe em %s", LOG_EMOJIS['FILE'], file, target_path)    
            
    # Configura variáveis de ambiente
    os.environ['HADOOP_HOME'] = str(hadoop_home)
    path = os.environ.get('PATH', '')
    if str(bin_dir) not in path:
        os.environ['PATH'] = f"{path};{bin_dir}"
    
    logger.info("%s HADOOP_HOME configurado: %s", LOG_EMOJIS['CONFIG'], os.environ['HADOOP_HOME'])
    logger.info("%s Configuração do Hadoop concluída com sucesso", LOG_EMOJIS['SUCCESS'])
    return True

def setup_hadoop():
    """
    Configura o ambiente Hadoop de acordo com o sistema operacional.
    """
    system = platform.system().lower()
    
    if system == "windows":
        logger.info("%s Detectado sistema Windows - Iniciando configuração do Hadoop", LOG_EMOJIS['START'])
        return setup_hadoop_windows()
    else:
        logger.info("%s Detectado sistema %s - Não é necessária configuração adicional do Hadoop", LOG_EMOJIS['INFO'], system.capitalize())
        return True

if __name__ == "__main__":
    if setup_hadoop():
        logger.info("%s Configuração concluída com sucesso!", LOG_EMOJIS['SUCCESS'])
        sys.exit(0)
    else:
        logger.error("%s Falha na configuração", LOG_EMOJIS['ERROR'])
        sys.exit(1)
