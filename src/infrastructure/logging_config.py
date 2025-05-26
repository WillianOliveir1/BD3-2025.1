"""
Configuração centralizada de logging para o projeto
"""
import logging
import sys
from pathlib import Path
from typing import Optional

def setup_logger(name: str, log_file: Optional[Path] = None, level=logging.INFO) -> logging.Logger:
    """
    Configura e retorna um logger com formatação consistente.
    
    Args:
        name: Nome do logger
        log_file: Caminho opcional para arquivo de log
        level: Nível de logging (default: INFO)
    
    Returns:
        Logger configurado
    """
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        formatter = logging.Formatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Handler para console com emojis para melhor visualização
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # Se fornecido, adiciona handler para arquivo
        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        
        logger.setLevel(level)
        logger.propagate = False  # Evita duplicação de logs
    
    return logger

# Dicionário de emojis para diferentes níveis de log
LOG_EMOJIS = {
    'INFO': '📝',
    'WARNING': '⚠️',
    'ERROR': '❌',
    'CRITICAL': '🚨',
    'SUCCESS': '✅',
    'START': '🚀',
    'END': '🏁',
    'DATA': '📊',
    'FILE': '📁',
    'PROCESS': '⚙️',
    'EXTRACT': '📥',
    'TRANSFORM': '🔄',
    'LOAD': '📤',
    'CONFIG': '⚙️',
    'DOWNLOAD': '⬇️',
    'UPLOAD': '⬆️',
    'SKIP': '⏭️',
    'CLEAN': '🧹',
    'TIME': '⏱️',
    'RETRY': '🔁',
    'TEST': '🧪'
}
