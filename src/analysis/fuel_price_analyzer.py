"""
Analisador simplificado de preços de combustíveis.
"""
import logging
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from src.infrastructure.spark_manager import SparkManager
from src.infrastructure.logging_config import LOG_EMOJIS

logger = logging.getLogger(__name__)

class FuelPriceAnalyzer:
    """Análise básica de preços de combustíveis"""
    
    def __init__(self):
        """Inicializa o analisador"""
        self.spark_manager = SparkManager(app_name="GasPricesAnalyzer")
        self.spark = self.spark_manager.start_session()
    
    def load_data(self):
        """Carrega apenas os dados necessários para análise."""
        try:
            silver_path = Path("data/silver/kaggle/gas_prices_in_brazil")
            
            # Carregar tabelas
            facts = self.spark.read.parquet(str(silver_path / "fact_precos.parquet"))
            dim_produto = self.spark.read.parquet(str(silver_path / "dim_produto.parquet"))
            dim_estado = self.spark.read.parquet(str(silver_path / "dim_estado.parquet"))
            
            # Join com aliases para evitar conflitos de nomes
            df = facts.join(dim_produto.alias("produto"), facts.produtoId == dim_produto.id) \
                    .join(dim_estado.alias("estado"), facts.estadoId == dim_estado.id) \
                    .select(facts["*"], 
                            col("produto.nome").alias("produto_nome"),
                            col("estado.nome").alias("estado_nome"))
            
            logger.info("%s Dados carregados com sucesso", LOG_EMOJIS['SUCCESS'])
            return df
            
        except Exception as e:
            logger.error("%s Erro ao carregar dados: %s", LOG_EMOJIS['ERROR'], str(e))
            raise
    
    def analyze_gas_prices(self, df):
        """Análise básica dos preços da gasolina comum."""
        try:
            # Filtrar apenas gasolina comum
            gas_df = df.filter(col("produto_nome") == "GASOLINA COMUM")
            
            # Estatísticas Brasil
            logger.info("%s Análise - Estatísticas da Gasolina Comum no Brasil", LOG_EMOJIS['DATA'])
            gas_df.select("pMed", "pMin", "pMax") \
                .summary("count", "mean", "min", "max") \
                .show()
            
            # Top 5 estados mais caros (média)
            logger.info("%s Análise - Top 5 Estados com Gasolina Mais Cara", LOG_EMOJIS['DATA'])
            gas_df.groupBy(col("estado_nome").alias("Estado")) \
                .agg(F.avg("pMed").alias("Preco_Medio")) \
                .orderBy("Preco_Medio", ascending=False) \
                .limit(5) \
                .show()
            
            # Contagem por produto para validação
            logger.info("%s Análise - Distribuição por Produto", LOG_EMOJIS['DATA'])
            df.groupBy("produto_nome").count().orderBy("count", ascending=False).show()
            
        except Exception as e:
            logger.error("%s Erro na análise: %s", LOG_EMOJIS['ERROR'], str(e))
            raise
    
    def analyze_all_products(self, df):
        """Análise geral de todos os produtos."""
        try:
            logger.info("%s Análise - Preços médios por produto", LOG_EMOJIS['DATA'])
            df.groupBy("produto_nome") \
                .agg(F.avg("pMed").alias("Preco_Medio"), 
                F.count("*").alias("Registros")) \
                .orderBy("Preco_Medio", ascending=False) \
                .show()
            
        except Exception as e:
            logger.error("%s Erro na análise geral: %s", LOG_EMOJIS['ERROR'], str(e))
            raise
    
    def __del__(self):
        """Cleanup"""
        if hasattr(self, 'spark_manager'):
            self.spark_manager.stop_session()
