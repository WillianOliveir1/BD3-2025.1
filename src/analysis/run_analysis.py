"""
Script simplificado para análise de preços de combustíveis.
"""
import time
from src.pipeline.kaggle.etl_pipeline import FuelPriceETL
from src.analysis.fuel_price_analyzer import FuelPriceAnalyzer
from src.infrastructure.logging_config import setup_logger, LOG_EMOJIS

# Configurar logger
logger = setup_logger('Analysis')

def main():
    # Executar o pipeline ETL se necessário
    logger.info("%s Iniciando análises de preços de combustíveis", LOG_EMOJIS['START'])
    
    etl = FuelPriceETL()
    etl.run_pipeline()
    
    # Iniciar análises
    logger.info("%s Carregando dados para análise...", LOG_EMOJIS['DATA'])
    analyzer = FuelPriceAnalyzer()
    df = analyzer.load_data()
    
    # Análise da gasolina comum
    logger.info("%s Analisando preços da gasolina comum...", LOG_EMOJIS['DATA'])
    analyzer.analyze_gas_prices(df)
    
    # Análise comparativa entre combustíveis
    logger.info("%s Comparando preços entre combustíveis...", LOG_EMOJIS['DATA'])
    analyzer.analyze_all_products(df)
    
    logger.info("%s Análises concluídas com sucesso!", LOG_EMOJIS['SUCCESS'])
    
    # Pequena pausa para garantir que todos os resultados sejam exibidos
    time.sleep(1)

if __name__ == "__main__":
    main()
