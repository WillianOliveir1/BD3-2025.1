#!/usr/bin/env python
"""
Script para depurar problemas na criação da tabela fato
"""
import os
import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_spark():
    """Configura uma sessão Spark para análise"""
    # Definir variáveis de ambiente para Hadoop no Windows
    os.environ['HADOOP_HOME'] = str(Path(__file__).resolve().parent / "hadoop")
    os.environ['HADOOP_CONF_DIR'] = str(Path(__file__).resolve().parent / "hadoop" / "etc" / "hadoop")
    
    # Iniciar sessão Spark
    spark = SparkSession.builder \
        .appName("DebugFactTable") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()
    
    return spark

def check_dimensions(spark, silver_path):
    """Verifica as dimensões e imprime informações úteis"""
    logger.info("Verificando dimensões...")
    
    # Carregar dimensões
    dim_regiao = spark.read.parquet(str(silver_path / "dim_regiao.parquet"))
    dim_estado = spark.read.parquet(str(silver_path / "dim_estado.parquet"))
    dim_produto = spark.read.parquet(str(silver_path / "dim_produto.parquet"))
    dim_tempo = spark.read.parquet(str(silver_path / "dim_tempo.parquet"))
    
    # Contar registros
    logger.info(f"Dimensão Região: {dim_regiao.count()} registros")
    logger.info(f"Dimensão Estado: {dim_estado.count()} registros")
    logger.info(f"Dimensão Produto: {dim_produto.count()} registros")
    logger.info(f"Dimensão Tempo: {dim_tempo.count()} registros")
    
    # Mostrar dados da dimensão tempo
    logger.info("Detalhes da dimensão tempo:")
    dim_tempo.show(truncate=False)
    
    return dim_regiao, dim_estado, dim_produto, dim_tempo

def check_bronze_data(spark, bronze_path):
    """Verifica os dados bronze para entender o problema de join"""
    logger.info("Verificando dados bronze...")
    
    # Encontrar o arquivo TSV mais recente
    bronze_folders = [d for d in bronze_path.glob("*") if d.is_dir()]
    latest_folder = max(bronze_folders, key=lambda x: x.name)
    tsv_files = list(latest_folder.glob("*.tsv"))
    
    if not tsv_files:
        logger.error(f"Nenhum arquivo TSV encontrado em {latest_folder}")
        return None
    
    # Ler o arquivo TSV
    df = spark.read \
        .option("header", "true") \
        .option("delimiter", "\t") \
        .option("inferSchema", "true") \
        .csv(str(tsv_files[0]))
    
    # Contar registros
    total_records = df.count()
    logger.info(f"Total de registros no bronze: {total_records}")
    
    # Verificar datas
    logger.info("Exemplo de datas no bronze:")
    df.select("DATA INICIAL", "DATA FINAL").distinct().show(5, truncate=False)
    
    # Converter datas para formato timestamp
    df_dates = df.select(
        F.to_timestamp(F.col("DATA INICIAL"), "dd/MM/yyyy").alias("inicio"),
        F.to_timestamp(F.col("DATA FINAL"), "dd/MM/yyyy").alias("fim")
    ).distinct()
    
    logger.info("Datas convertidas para timestamp:")
    df_dates.show(5, truncate=False)
    
    # Contar datas distintas
    distinct_dates = df_dates.count()
    logger.info(f"Total de datas distintas: {distinct_dates}")
    
    return df

def diagnose_join_issue(spark, bronze_df, dim_tempo):
    """Diagnostica problemas de join entre bronze e dimensão tempo"""
    logger.info("Diagnosticando problema de join...")
    
    if bronze_df is None or dim_tempo is None:
        logger.error("Dados não disponíveis para diagnóstico")
        return
    
    # Preparar dados bronze para join
    df_with_dates = bronze_df.withColumn(
        "inicio", 
        F.to_timestamp(F.col("DATA INICIAL"), "dd/MM/yyyy")
    )
    
    # Verificar exemplos de datas bronze
    logger.info("Exemplos de datas no bronze após conversão:")
    df_with_dates.select("inicio").distinct().show(5, truncate=False)
    
    # Verificar exemplos de datas dimensão
    logger.info("Exemplos de datas na dimensão tempo:")
    dim_tempo.select("inicio").show(truncate=False)
    
    # Testar join
    logger.info("Testando join entre bronze e dimensão tempo...")
    joined = df_with_dates.join(
        dim_tempo.withColumnRenamed("id", "tempo_id"), 
        "inicio"
    )
    
    join_count = joined.count()
    logger.info(f"Registros após join: {join_count}")
    
    if join_count == 0:
        # Verificar se há correspondências usando formato de string
        logger.info("Testando join usando formato de string...")
        
        bronze_dates = df_with_dates.select(
            F.date_format("inicio", "yyyy-MM-dd").alias("inicio_str")
        ).distinct()
        
        tempo_dates = dim_tempo.select(
            F.date_format("inicio", "yyyy-MM-dd").alias("inicio_str")
        ).distinct()
        
        bronze_dates.show(truncate=False)
        tempo_dates.show(truncate=False)
        
        # Fazer uma união para ver se há alguma correspondência
        bronze_dates_list = [row.inicio_str for row in bronze_dates.collect()]
        tempo_dates_list = [row.inicio_str for row in tempo_dates.collect()]
        
        common_dates = set(bronze_dates_list).intersection(set(tempo_dates_list))
        logger.info(f"Datas em comum: {common_dates}")
        
        if not common_dates:
            logger.error("Não há datas em comum entre bronze e dimensão tempo!")
            logger.info("Sugestão: verifique a criação da dimensão tempo, pode haver um problema com o formato das datas")

def main():
    """Função principal"""
    logger.info("Iniciando diagnóstico da tabela fato...")
    
    # Configurar caminhos
    base_path = Path(__file__).resolve().parent
    bronze_path = base_path / "data" / "bronze" / "kaggle" / "gas_prices_in_brazil"
    silver_path = base_path / "data" / "silver" / "kaggle" / "gas_prices_in_brazil"
    
    # Verificar se os caminhos existem
    if not bronze_path.exists():
        logger.error(f"Caminho bronze não existe: {bronze_path}")
        return
    
    if not silver_path.exists():
        logger.error(f"Caminho silver não existe: {silver_path}")
        return
    
    # Iniciar Spark
    spark = setup_spark()
    
    try:
        # Verificar dimensões
        _, _, _, dim_tempo = check_dimensions(spark, silver_path)
        
        # Verificar dados bronze
        bronze_df = check_bronze_data(spark, bronze_path)
        
        # Diagnosticar problema de join
        diagnose_join_issue(spark, bronze_df, dim_tempo)
        
        # Verificar tabela fato
        fact_path = silver_path / "fact_precos.parquet"
        if fact_path.exists():
            fact = spark.read.parquet(str(fact_path))
            fact_count = fact.count()
            logger.info(f"Tabela fato contém {fact_count} registros")
            
            if fact_count > 0:
                logger.info("Primeiros registros da tabela fato:")
                fact.show(5, truncate=False)
            else:
                logger.warning("A tabela fato está vazia!")
        else:
            logger.error(f"Arquivo da tabela fato não existe: {fact_path}")
        
    finally:
        # Encerrar sessão Spark
        spark.stop()
    
    logger.info("Diagnóstico concluído!")

if __name__ == "__main__":
    main()
