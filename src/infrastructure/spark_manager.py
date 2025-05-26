"""
Gerenciador simplificado de sessões Spark
"""
import os
import logging
import findspark
from pyspark.sql import SparkSession
from typing import Dict, Optional, Union
from src.infrastructure.hadoop_setup import setup_hadoop

class SparkManager:
    """Gerenciador simplificado de sessões Spark"""
    
    def __init__(self, app_name: str = "SparkApp", config: Optional[Dict[str, str]] = None):
        """Inicializa o gerenciador do Spark"""
        self._setup_logging()
        self.app_name = app_name
        self.config = config or {}
        self.spark = None
        
        # Configurar ambiente
        if not self._setup_environment():
            raise Exception("Falha ao configurar ambiente Spark")
    
    def _setup_logging(self):
        """Configura logging"""
        self.logger = logging.getLogger(self.__class__.__name__)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
    
    def _setup_environment(self) -> bool:
        """Configura o ambiente básico do Spark"""
        try:
            # Configurar JAVA_HOME se não estiver definido
            if not os.environ.get("JAVA_HOME"):
                java_paths = [r"C:\Program Files\Java", r"C:\Program Files (x86)\Java"]
                for base_path in java_paths:
                    if os.path.exists(base_path):
                        java_versions = [d for d in os.listdir(base_path) if d.startswith("jdk")]
                        if java_versions:
                            java_home = os.path.join(base_path, sorted(java_versions)[-1])
                            os.environ["JAVA_HOME"] = java_home
                            os.environ["PATH"] = f"{os.path.join(java_home, 'bin')};{os.environ.get('PATH', '')}"
                            self.logger.info(f"JAVA_HOME configurado: {java_home}")
                            break
            
            # Configura Hadoop (multiplataforma)
            if not setup_hadoop():
                self.logger.error("Falha na configuração do Hadoop")
                return False
                
            # Inicializar Spark
            findspark.init()
            
            # Configurações base
            self.config.update({
                "spark.driver.host": "localhost",
                "spark.driver.bindAddress": "127.0.0.1",
                "spark.master": "local[*]",
                "spark.executor.memory": "2g",
                "spark.driver.memory": "2g"
            })
            
            return True
            
        except Exception as e:
            self.logger.error(f"Erro na configuração do ambiente: {e}")
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
            self.logger.info("Sessão Spark iniciada com sucesso")
            return self.spark
            
        except Exception as e:
            self.logger.error(f"Erro ao iniciar sessão Spark: {str(e)}")
            return None
    
    def stop_session(self):
        """Para a sessão atual"""
        if self.spark:
            try:
                self.spark.stop()
                self.logger.info("Sessão Spark encerrada")
            except Exception as e:
                self.logger.error(f"Erro ao encerrar sessão Spark: {str(e)}")
            finally:
                self.spark = None
    
    def __enter__(self):
        """Suporte a context manager"""
        self.start_session()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Limpeza ao sair do contexto"""
        self.stop_session()
