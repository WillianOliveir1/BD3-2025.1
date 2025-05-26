"""
Executor de etapas individuais do pipeline ETL.
Permite executar cada etapa do pipeline separadamente para desenvolvimento e testes.
"""
import sys
import argparse
from pathlib import Path
from datetime import datetime
import logging
from typing import Optional

# Adiciona o diretório raiz ao PYTHONPATH
root_dir = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(root_dir))

from src.pipeline.kaggle.etl_pipeline import FuelPriceETL, PipelineStage, setup_logger

def setup_args_parser():
    """Configura o parser de argumentos da linha de comando"""
    parser = argparse.ArgumentParser(
        description="Executa etapas individuais do pipeline ETL de preços de combustíveis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  python run_stages.py --stage extract                 # Apenas extração
  python run_stages.py --stage transform              # Apenas transformação
  python run_stages.py --stage load                   # Apenas carregamento
  python run_stages.py --stage all                    # Pipeline completo
  python run_stages.py --stage transform --input-file "path/to/file.tsv"  # Transformação com arquivo específico
        """
    )
    
    parser.add_argument(
        '--stage',
        choices=['extract', 'transform', 'load', 'all'],
        required=True,
        help='Etapa do pipeline para executar'
    )
    
    parser.add_argument(
        '--input-file',
        type=str,
        help='Arquivo de entrada específico (usado principalmente para transform)'
    )
    
    parser.add_argument(
        '--base-path',
        type=str,
        help='Caminho base para o data lake (padrão: data/)'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Ativar logging detalhado'
    )
    
    return parser

def configure_logging(verbose: bool = False):
    """Configura o logging com base no nível de verbosidade"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def run_extract_stage(etl: FuelPriceETL) -> Path:
    """Executa apenas a etapa de extração"""
    logger = logging.getLogger('StageRunner')
    logger.info("🔄 Executando etapa de EXTRAÇÃO...")
    
    try:
        result = etl.extract()
        logger.info(f"✅ Extração concluída com sucesso!")
        logger.info(f"📄 Arquivo extraído: {result}")
        return result
    except Exception as e:
        logger.error(f"❌ Erro na extração: {str(e)}")
        raise

def run_transform_stage(etl: FuelPriceETL, input_file: Optional[str] = None):
    """Executa apenas a etapa de transformação"""
    logger = logging.getLogger('StageRunner')
    logger.info("🔄 Executando etapa de TRANSFORMAÇÃO...")
    
    try:
        input_path = Path(input_file) if input_file else None
        result = etl.transform(input_path)
        
        logger.info(f"✅ Transformação concluída com sucesso!")
        if result.get('success'):
            logger.info(f"📊 Registros processados: {result.get('records_count', 'N/A')}")
            logger.info(f"⏱️  Tempo: {result.get('processing_time_seconds', 0):.2f}s")
            logger.info(f"📁 Pasta Silver: {result.get('silver_path', 'N/A')}")
        
        return result
    except Exception as e:
        logger.error(f"❌ Erro na transformação: {str(e)}")
        raise

def run_load_stage(etl: FuelPriceETL):
    """Executa apenas a etapa de carregamento"""
    logger = logging.getLogger('StageRunner')
    logger.info("🔄 Executando etapa de CARREGAMENTO...")
    
    # TODO: Implementar quando a etapa de load for necessária
    logger.info("⚠️  Etapa de carregamento ainda não implementada")
    logger.info("💡 Os dados já são salvos durante a transformação na camada Silver")

def run_all_stages(etl: FuelPriceETL):
    """Executa o pipeline completo"""
    logger = logging.getLogger('StageRunner')
    logger.info("🔄 Executando PIPELINE COMPLETO...")
    
    try:
        etl.run_pipeline(PipelineStage.ALL)
        logger.info("✅ Pipeline completo executado com sucesso!")
    except Exception as e:
        logger.error(f"❌ Erro no pipeline: {str(e)}")
        raise

def main():
    """Função principal"""
    parser = setup_args_parser()
    args = parser.parse_args()
    
    # Configurar logging
    configure_logging(args.verbose)
    logger = logging.getLogger('StageRunner')
    
    logger.info("🚀 Iniciando executor de etapas do pipeline ETL")
    logger.info(f"📋 Etapa selecionada: {args.stage.upper()}")
    
    try:
        # Inicializar pipeline
        base_path = Path(args.base_path) if args.base_path else None
        etl = FuelPriceETL(base_path)
        
        # Executar etapa escolhida
        if args.stage == 'extract':
            run_extract_stage(etl)
            
        elif args.stage == 'transform':
            run_transform_stage(etl, args.input_file)
            
        elif args.stage == 'load':
            run_load_stage(etl)
            
        elif args.stage == 'all':
            run_all_stages(etl)
        
        logger.info("🎉 Execução concluída com sucesso!")
        
    except KeyboardInterrupt:
        logger.warning("⚠️  Execução interrompida pelo usuário")
        sys.exit(1)
    except Exception as e:
        logger.error(f"💥 Erro fatal: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()