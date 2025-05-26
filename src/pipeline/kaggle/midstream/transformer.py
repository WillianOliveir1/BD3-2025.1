"""
Transformador de dados da camada bronze para silver usando modelo dimensional
"""
import logging
import time
from pathlib import Path
from typing import Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.infrastructure.spark_manager import SparkManager

class MultidimensionalTransformer:
    """Classe responsável por transformar dados da bronze em modelo estrela"""
    
    def __init__(self, output_base_path: str = None):
        """
        Inicializa o transformador
        
        Args:
            output_base_path: Caminho base para salvar os arquivos transformados
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.output_base_path = output_base_path or "data/silver"
        self.spark = None
        self.spark_manager = None

    def _setup_spark(self) -> bool:
        """Configura a sessão Spark com as configurações necessárias"""
        try:
            self.spark_manager = SparkManager(
                app_name="GasPricesTransformer",
                config={
                    "spark.sql.legacy.timeParserPolicy": "LEGACY",
                    "spark.sql.sources.partitionOverwriteMode": "dynamic"
                }
            )
            self.spark = self.spark_manager.start_session()
            return True
        except Exception as e:
            self.logger.error(f"Erro ao configurar Spark: {str(e)}")
            return False

    def _ensure_spark(self):
        """Garante que temos uma sessão Spark ativa"""
        if self.spark is None:
            self._setup_spark()
        return self.spark

    def _cleanup_spark(self):
        """Limpa recursos do Spark"""
        if self.spark_manager:
            self.spark_manager.stop_session()
            self.spark = None
            self.spark_manager = None    
    
    def transform_to_star_schema(self, input_file: str = None) -> Dict[str, Any]:
        """
        Transforma os dados do arquivo TSV em modelo estrela
        
        Args:
            input_file: Caminho para o arquivo TSV da camada bronze. Se não fornecido, usa a pasta mais recente.
            
        Returns:
            Dict com informações sobre a transformação
        """
        start_time = time.time()
        result = {
            'success': False,
            'error': None,
            'output_path': None,
            'files_created': 0,
            'records_processed': 0,
            'processing_time_seconds': 0,
            'dimensions': {},
            'fact_records': 0
        }
        
        try:
            # Garantir que temos Spark
            spark = self._ensure_spark()
            
            # Se input_file não foi fornecido, usar a pasta mais recente
            if input_file is None:
                latest_folder = self._get_latest_bronze_folder()
                input_file = str(Path(latest_folder) / "*.tsv")
            
            # Ler arquivo bronze
            df = spark.read \
                .option("header", "true") \
                .option("delimiter", "\t") \
                .option("inferSchema", "true") \
                .csv(input_file)
            
            # Criar dimensões
            dim_regiao = self._create_dim_regiao(df)
            dim_estado = self._create_dim_estado(df, dim_regiao)
            dim_produto = self._create_dim_produto(df)
            dim_tempo = self._create_dim_tempo(df)
            
            # Criar fato
            fact_precos = self._create_fact_precos(df, dim_regiao, dim_estado, dim_produto, dim_tempo)
            
            # Preparar caminho de saída
            output_path = Path(self.output_base_path) / "kaggle" / "gas_prices_in_brazil"
            output_path.mkdir(parents=True, exist_ok=True)
            
            # Salvar arquivos
            dim_regiao.write.mode("overwrite").parquet(str(output_path / "dim_regiao.parquet"))
            dim_estado.write.mode("overwrite").parquet(str(output_path / "dim_estado.parquet"))
            dim_produto.write.mode("overwrite").parquet(str(output_path / "dim_produto.parquet"))
            dim_tempo.write.mode("overwrite").parquet(str(output_path / "dim_tempo.parquet"))
            fact_precos.write.mode("overwrite").parquet(str(output_path / "fact_precos.parquet"))
              # Atualizar resultado
            result.update({
                'success': True,
                'output_path': str(output_path),
                'bronze_file_path': input_file,
                'files_created': 5,
                'records_processed': df.count(),
                'processing_time_seconds': time.time() - start_time,
                'dimensions': {
                    'regiao': dim_regiao.count(),
                    'estado': dim_estado.count(),
                    'produto': dim_produto.count(),
                    'tempo': dim_tempo.count()
                },
                'fact_records': fact_precos.count()
            })
            
        except Exception as e:
            self.logger.error(f"Erro na transformação: {str(e)}")
            result['error'] = str(e)
        finally:
            self._cleanup_spark()
        
        return result
    
    def _create_dim_regiao(self, df: DataFrame) -> DataFrame:
        """Cria dimensão região"""
        return df.select("REGIÃO").distinct() \
            .withColumnRenamed("REGIÃO", "nome") \
            .withColumn("id", F.monotonically_increasing_id())
    
    def _create_dim_estado(self, df: DataFrame, dim_regiao: DataFrame) -> DataFrame:
        """Cria dimensão estado"""
        return df.select("ESTADO", "REGIÃO").distinct() \
            .join(dim_regiao.withColumnRenamed("nome", "REGIÃO"), "REGIÃO") \
            .withColumnRenamed("ESTADO", "nome") \
            .withColumnRenamed("id", "regiaoId") \
            .withColumn("id", F.monotonically_increasing_id()) \
            .select("id", "nome", "regiaoId")
    
    def _create_dim_produto(self, df: DataFrame) -> DataFrame:
        """Cria dimensão produto"""
        return df.select("PRODUTO", "UNIDADE DE MEDIDA").distinct() \
            .withColumnRenamed("PRODUTO", "nome") \
            .withColumnRenamed("UNIDADE DE MEDIDA", "unidade") \
            .withColumn("id", F.monotonically_increasing_id())
    
    def _create_dim_tempo(self, df: DataFrame) -> DataFrame:
        """Cria dimensão tempo"""
        # Converter colunas de data - ajustado para o formato correto (YYYY-MM-DD)
        df_dates = df.select(
            F.to_timestamp(F.col("DATA INICIAL"), "yyyy-MM-dd").alias("inicio"),
            F.to_timestamp(F.col("DATA FINAL"), "yyyy-MM-dd").alias("fim")
        ).distinct()
        
        # Extrair componentes temporais
        return df_dates \
            .withColumn("id", F.monotonically_increasing_id()) \
            .withColumn("semana", F.weekofyear("inicio")) \
            .withColumn("mes", F.month("inicio")) \
            .withColumn("ano", F.year("inicio"))
    
    def _create_fact_precos(
        self, 
        df: DataFrame, 
        dim_regiao: DataFrame,
        dim_estado: DataFrame,
        dim_produto: DataFrame,
        dim_tempo: DataFrame
    ) -> DataFrame:
        """Cria tabela fato de preços"""        # Preparar dimensões com aliases para join e renomear IDs para evitar ambiguidade
        dim_regiao_alias = dim_regiao.withColumnRenamed("nome", "REGIÃO").withColumnRenamed("id", "regiao_id")
        dim_estado_alias = dim_estado.withColumnRenamed("nome", "ESTADO").withColumnRenamed("id", "estado_id")
        dim_produto_alias = dim_produto.withColumnRenamed("nome", "PRODUTO") \
            .withColumnRenamed("unidade", "UNIDADE DE MEDIDA") \
            .withColumnRenamed("id", "produto_id")
          # Preparar datas para join com dimensão tempo - ajustado para o formato correto (YYYY-MM-DD)
        df_with_dates = df.withColumn(
            "inicio", 
            F.to_timestamp(F.col("DATA INICIAL"), "yyyy-MM-dd")
        )
        
        # Criar fato com joins
        joined_df = df_with_dates \
            .join(dim_regiao_alias, "REGIÃO") \
            .join(dim_estado_alias, "ESTADO") \
            .join(dim_produto_alias, ["PRODUTO", "UNIDADE DE MEDIDA"]) \
            .join(dim_tempo.withColumnRenamed("id", "tempo_id"), "inicio")
            
        # Criar um ID único para a tabela fato
        return joined_df.select(
                F.monotonically_increasing_id().alias("id"),
                F.col("tempo_id").alias("tempoId"),
                F.col("estado_id").alias("estadoId"),
                F.col("produto_id").alias("produtoId"),
                F.col("NÚMERO DE POSTOS PESQUISADOS").cast("integer").alias("numPostoPesq"),
                F.col("PREÇO MÉDIO REVENDA").cast("decimal(10,2)").alias("pMed"),
                F.col("DESVIO PADRÃO REVENDA").cast("decimal(10,2)").alias("desvioPadrao"),
                F.col("PREÇO MÍNIMO REVENDA").cast("decimal(10,2)").alias("pMin"),
                F.col("PREÇO MÁXIMO REVENDA").cast("decimal(10,2)").alias("pMax"),
                F.col("COEF DE VARIAÇÃO REVENDA").cast("decimal(10,2)").alias("coefVariacao"),
                F.col("MARGEM MÉDIA REVENDA").cast("decimal(10,2)").alias("margemMediaRevenda")
            )
    
    def _get_latest_bronze_folder(self, base_path: str = "data/bronze/kaggle/gas_prices_in_brazil") -> str:
        """
        Encontra a pasta mais recente na camada bronze
        
        Args:
            base_path: Caminho base para a pasta bronze
            
        Returns:
            Caminho completo para a pasta mais recente
        """
        try:
            # Listar todas as pastas de data
            data_folders = [d for d in Path(base_path).glob("*") if d.is_dir()]
            if not data_folders:
                raise ValueError(f"Nenhuma pasta de dados encontrada em {base_path}")
            
            # Encontrar a pasta mais recente
            latest_folder = max(data_folders, key=lambda x: x.name)
            
            # Retornar o caminho completo
            return str(latest_folder)
        except Exception as e:
            self.logger.error(f"Erro ao buscar pasta mais recente: {str(e)}")
            raise
