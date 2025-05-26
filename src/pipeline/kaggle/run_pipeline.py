"""
Script principal para execução do pipeline ETL - Análise de Preços de Combustíveis.
Este é o ponto de entrada principal para executar o pipeline completo.
"""
import sys
from pathlib import Path

# Adiciona o diretório raiz ao PYTHONPATH
root_dir = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(root_dir))

from src.pipeline.kaggle.etl_pipeline import FuelPriceETL
from src.infrastructure.logging_config import setup_logger, LOG_EMOJIS

# Configurar logger
logger = setup_logger('Pipeline')

def main():
    """Executa o pipeline ETL completo"""
    logger.info("%s Iniciando Pipeline ETL - Análise de Preços de Combustíveis", LOG_EMOJIS['START'])
    logger.info("=" * 60)
    
    try:
        # Inicializar e executar pipeline
        etl = FuelPriceETL()
        etl.run_pipeline()
        logger.info("=" * 60)
        logger.info("%s Pipeline ETL executado com sucesso!", LOG_EMOJIS['SUCCESS'])
        logger.info("%s Dados extraídos do Kaggle", LOG_EMOJIS['EXTRACT'])
        logger.info("%s Dados transformados e salvos na camada Silver", LOG_EMOJIS['TRANSFORM'])
        logger.info("%s Verifique a pasta 'data/silver/kaggle/gas_prices_in_brazil' para os resultados", LOG_EMOJIS['FILE'])
        
    except KeyboardInterrupt:
        logger.warning("%s Pipeline interrompido pelo usuário", LOG_EMOJIS['WARNING'])
        sys.exit(1)
    except Exception as e:
        logger.error("%s Erro no pipeline: %s", LOG_EMOJIS['ERROR'], str(e), exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
