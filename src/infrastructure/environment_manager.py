import os
from typing import Optional
from src.infrastructure.config import CONFIG, validate_config
from src.infrastructure.logging_config import LOG_EMOJIS, setup_logger
class EnvironmentManager:
    """Gerencia variáveis de ambiente e configurações do projeto."""
    
    def __init__(self):
        self.logger = setup_logger('EnvironmentManager')
        self._validate_environment()
        self._setup_java_home()
        
    def _validate_environment(self):
        """Valida se todas as variáveis de ambiente necessárias estão configuradas"""
        try:
            validate_config(CONFIG)
            self.logger.info("%s Configurações do ambiente validadas com sucesso", LOG_EMOJIS['SUCCESS'])
        except ValueError as e:
            self.logger.error("%s Erro na validação do ambiente: %s", LOG_EMOJIS['ERROR'], str(e))            
            raise          
        
    def _setup_java_home(self):
        """Configura JAVA_HOME"""
        java_home = CONFIG['JAVA_HOME']
        if java_home and os.path.exists(java_home):
            os.environ['JAVA_HOME'] = java_home
            self.logger.info("%s JAVA_HOME configurado: %s", LOG_EMOJIS['CONFIG'], java_home)
        else:
            self.logger.error("%s JAVA_HOME inválido ou não encontrado: %s", LOG_EMOJIS['ERROR'], java_home)
            raise ValueError("JAVA_HOME inválido ou não encontrado")
                
    @property
    def java_home(self) -> Optional[str]:
        """Retorna o valor atual do JAVA_HOME"""
        return CONFIG['JAVA_HOME']
        
    @property
    def data_path(self) -> str:
        """Retorna o caminho base para armazenamento de dados"""
        return CONFIG['DATA_PATH']
        
    @property
    def kaggle_dataset(self) -> str:
        """Retorna o nome do dataset do Kaggle"""
        return CONFIG['KAGGLE_DATASET']
