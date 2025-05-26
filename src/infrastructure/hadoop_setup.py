"""
Script para configuração automática do Hadoop em diferentes sistemas operacionais.
Principalmente necessário para Windows, onde bibliotecas nativas do Hadoop precisam ser configuradas.
"""

import os
import platform
import sys
import urllib.request
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - INFO - %(message)s')
logger = logging.getLogger(__name__)

HADOOP_VERSION = "3.2.2"
WINUTILS_BASE_URL = f"https://github.com/cdarlint/winutils/raw/master/hadoop-{HADOOP_VERSION}/bin/"
REQUIRED_FILES = ["winutils.exe", "hadoop.dll"]

def setup_hadoop_windows():
    """
    Configura o ambiente Hadoop para Windows.
    
    Returns:
        bool: True se a configuração foi bem sucedida, False caso contrário
    """
    hadoop_home = Path("C:/hadoop")
    bin_dir = hadoop_home / "bin"

    # Cria diretórios
    try:
        bin_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Diretório criado: {bin_dir}")
    except Exception as e:
        logger.error(f"❌ Erro ao criar diretório {bin_dir}: {e}")
        return False

    # Download dos arquivos necessários
    for file in REQUIRED_FILES:
        url = WINUTILS_BASE_URL + file
        target_path = bin_dir / file
        
        if not target_path.exists():
            logger.info(f"Baixando {file}...")
            try:
                urllib.request.urlretrieve(url, str(target_path))
                logger.info(f"✅ {file} baixado com sucesso")
            except Exception as e:
                logger.error(f"❌ Erro ao baixar {file}: {e}")
                return False
        else:
            logger.info(f"✅ {file} já existe em {target_path}")

    # Configura variáveis de ambiente
    os.environ['HADOOP_HOME'] = str(hadoop_home)
    path = os.environ.get('PATH', '')
    if str(bin_dir) not in path:
        os.environ['PATH'] = f"{path};{bin_dir}"
    
    logger.info(f"✅ HADOOP_HOME configurado: {os.environ['HADOOP_HOME']}")
    logger.info("✅ Configuração do Hadoop concluída com sucesso")
    return True

def setup_hadoop():
    """
    Configura o ambiente Hadoop de acordo com o sistema operacional.
    """
    system = platform.system().lower()
    
    if system == "windows":
        logger.info("Detectado sistema Windows - Iniciando configuração do Hadoop")
        return setup_hadoop_windows()
    else:
        logger.info(f"Detectado sistema {system.capitalize()} - Não é necessária configuração adicional do Hadoop")
        return True

if __name__ == "__main__":
    if setup_hadoop():
        logger.info("🎉 Configuração concluída com sucesso!")
        sys.exit(0)
    else:
        logger.error("❌ Falha na configuração")
        sys.exit(1)
