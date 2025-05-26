"""
Gerenciador simplificado de sessões Spark
"""
import os
import findspark
from pyspark.sql import SparkSession
from typing import Dict, Optional, Union
from src.infrastructure.hadoop_setup import setup_hadoop
from src.infrastructure.config import CONFIG
from src.infrastructure.logging_config import LOG_EMOJIS, setup_logger

class SparkConfigurationError(Exception):
    """Exception raised when Spark environment configuration fails."""
    pass

class SparkManager:
    """Gerenciador simplificado de sessões Spark"""
    
    def __init__(self, app_name: str = "SparkApp", config: Optional[Dict[str, str]] = None):
        """Inicializa o gerenciador do Spark"""
        self.logger = setup_logger(self.__class__.__name__)
        self.app_name = app_name
        self.config = config or {}
        self.spark = None
        
        # Configurar ambiente
        if not self._setup_environment():
            raise SparkConfigurationError("Falha ao configurar ambiente Spark")
    
    def _setup_environment(self) -> bool:
        """Configura o ambiente básico do Spark"""
        try:
            # Tentar obter configurações de diferentes fontes
            java_home = None
            spark_home = None
            
            # 1. Tentar variáveis de ambiente diretamente
            if os.getenv('JAVA_HOME') and os.getenv('SPARK_HOME'):
                java_home = os.getenv('JAVA_HOME')
                spark_home = os.getenv('SPARK_HOME')            # 2. Tentar CONFIG do projeto
            elif CONFIG and 'JAVA_HOME' in CONFIG and 'SPARK_HOME' in CONFIG:
                java_home = CONFIG['JAVA_HOME']
                spark_home = CONFIG['SPARK_HOME']
            # 3. Tentar caminhos padrão típicos
            else:
                # Caminhos típicos para Windows
                java_home = r"C:\Program Files\Java\jdk-11.0.12"
                user_home = os.path.expanduser("~")
                spark_home = os.path.join(user_home, "spark", "spark-3.5.5-bin-hadoop3")
            
            # Configurar Hadoop primeiro
            if not setup_hadoop():
                self.logger.error("%s Falha ao configurar Hadoop", LOG_EMOJIS['ERROR'])
                return False
            
            if not java_home or not os.path.exists(java_home):
                self.logger.error("%s JAVA_HOME inválido ou não encontrado: %s", LOG_EMOJIS['ERROR'], java_home)
                return False
                
            os.environ["JAVA_HOME"] = java_home
            os.environ["PATH"] = f"{os.path.join(java_home, 'bin')};{os.environ.get('PATH', '')}"
            
            if spark_home and os.path.exists(spark_home):                
                findspark.init(spark_home)
            else:
                self.logger.error("%s SPARK_HOME inválido ou não encontrado: %s", LOG_EMOJIS['ERROR'], spark_home)
                return False
                
            self.logger.info("%s Ambiente configurado com sucesso: JAVA_HOME=%s, SPARK_HOME=%s", LOG_EMOJIS['CONFIG'], java_home, spark_home)
            return True
            
        except Exception as e:
            self.logger.error("%s Erro ao configurar ambiente: %s", LOG_EMOJIS['ERROR'], str(e))
            return False
    
    def start_session(self) -> Union[SparkSession, None]:
        """Inicia uma nova sessão Spark"""
        if self.spark is not None:
            return self.spark
            
        try:
            builder = SparkSession.builder.appName(self.app_name)
            for key, value in self.config.items():                
                builder = builder.config(key, value)
                
            self.spark = builder.getOrCreate()
            self.logger.info("%s Sessão Spark iniciada com sucesso", LOG_EMOJIS['START'])
            return self.spark
            
        except Exception as e:
            self.logger.error("%s Erro ao iniciar sessão Spark: %s", LOG_EMOJIS['ERROR'], str(e))
            return None
    
    def stop_session(self):
        """Para a sessão atual"""
        if self.spark:
            try:
                self.spark.stop()
                self.logger.info("%s Sessão Spark encerrada", LOG_EMOJIS['END'])
            except Exception as e:
                self.logger.error("%s Erro ao encerrar sessão Spark: %s", LOG_EMOJIS['ERROR'], str(e))
            finally:
                self.spark = None
    
    def __enter__(self):
        """Suporte a context manager"""
        self.start_session()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Limpeza ao sair do contexto"""
        self.stop_session()
