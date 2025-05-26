import os
from pathlib import Path
from typing import Optional
import logging
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

class EnvironmentManager:
    """Gerencia variáveis de ambiente e configurações do projeto."""
    
    def __init__(self):
        self._load_env_file()
        self._setup_java_home()
        
    def _load_env_file(self):
        """Carrega as variáveis de ambiente do arquivo .env"""
        env_path = Path(os.getcwd()) / '.env'
        if env_path.exists():
            load_dotenv(env_path)
            logger.info("Arquivo .env carregado com sucesso")
        else:
            logger.warning("Arquivo .env não encontrado")
            
    def _setup_java_home(self):
        """Configura JAVA_HOME se necessário"""
        java_home = os.environ.get('JAVA_HOME')
        if not java_home:
            # Tenta encontrar o Java instalado
            possible_paths = [
                r'C:\Program Files\Java',
                r'C:\Program Files (x86)\Java'
            ]
            
            for base_path in possible_paths:
                if os.path.exists(base_path):
                    # Procura pelo JDK mais recente
                    jdk_folders = [f for f in os.listdir(base_path) if f.startswith('jdk')]
                    if jdk_folders:
                        newest_jdk = sorted(jdk_folders)[-1]
                        java_home = os.path.join(base_path, newest_jdk)
                        os.environ['JAVA_HOME'] = java_home
                        logger.info(f"JAVA_HOME configurado automaticamente: {java_home}")
                        break
            
            if not os.environ.get('JAVA_HOME'):
                logger.error("Não foi possível configurar JAVA_HOME automaticamente")
                
    @property
    def java_home(self) -> Optional[str]:
        """Retorna o valor atual do JAVA_HOME"""
        return os.environ.get('JAVA_HOME')
    
    def validate_environment(self) -> bool:
        """Valida se todas as variáveis de ambiente necessárias estão configuradas"""
        required_vars = ['JAVA_HOME']
        missing_vars = [var for var in required_vars if not os.environ.get(var)]
        
        if missing_vars:
            logger.error(f"Variáveis de ambiente faltando: {', '.join(missing_vars)}")
            return False
        return True
