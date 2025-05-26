"""
Pipeline ETL principal para processamento de dados de preços de combustíveis.
"""
import sys
from enum import Enum
from pathlib import Path
from typing import Optional, Dict, Any

# Adiciona o diretório raiz ao PYTHONPATH
root_dir = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(root_dir))

from src.pipeline.kaggle.upstream.extractor import KaggleExtractor
from src.pipeline.kaggle.midstream.transformer import MultidimensionalTransformer
from src.infrastructure.data_lake_manager import DataLakeManager
from src.infrastructure.logging_config import LOG_EMOJIS, setup_logger

class PipelineStage(Enum):
    """Enumeração das etapas do pipeline"""
    EXTRACT = "extract"
    TRANSFORM = "transform"
    LOAD = "load"
    ALL = "all"

class FuelPriceETL:
    """Pipeline ETL para processamento de dados de preços de combustíveis"""
    
    def __init__(self, base_path: Optional[Path] = None):
        """
        Inicializa o pipeline ETL.
        
        Args:
            base_path: Caminho base para o data lake. Se não fornecido, usa 'data/'.        """
        # Configurar logger primeiro
        self.logger = setup_logger('ETL - Pipeline')
        self.logger.info("Inicializando pipeline ETL...")
        
        # Encontrar diretório raiz do projeto
        if base_path:
            self.base_path = Path(base_path)
        else:
            self.base_path = Path(__file__).resolve().parent.parent.parent.parent / 'data'
        
        self.logger.info("Diretório base do data lake: %s", self.base_path)
        # Inicializar gerenciador do Data Lake
        self.data_lake = DataLakeManager(str(self.base_path))
        
        # A estrutura é criada automaticamente no __init__ da DataLakeManager        
        
        # Inicializar componentes
        self.logger.info("Inicializando componentes...")
        self.extractor = KaggleExtractor(self.data_lake)
        self.transformer = MultidimensionalTransformer(output_base_path=str(self.base_path / "silver"))
        
        # Armazenar resultados das etapas
        self.results = {
            'extract': None,
            'transform': None,
            'load': None
        }
        
        self.logger.info("Pipeline ETL inicializado com sucesso!")
    
    def extract(self) -> Path:
        """
        Executa a etapa de extração.
        
        Returns:
            Path do arquivo extraído
        """
        self.logger.info("\n=== Iniciando etapa de extração ===")        
        
        try:
            input_file = self.extractor.extract_dataset()
            self.results['extract'] = input_file
            self.logger.info("%s Extração concluída com sucesso!", LOG_EMOJIS['SUCCESS'])
            self.logger.info("Arquivo salvo em: %s", input_file)
            return input_file
        except Exception as e:
            self.logger.error("Erro na extração: %s", str(e))
            raise    
    
    def transform(self, input_file: Optional[Path] = None) -> Dict[str, Any]:
        """
        Executa a etapa de transformação, convertendo os dados para modelo Star Schema.
        
        Args:
            input_file: Path do arquivo a ser transformado. Se não fornecido, usa o resultado da etapa de extração.
        
        Returns:
            Dicionário com estatísticas e metadados da transformação multidimensional
        """
        self.logger.info("\n=== Iniciando etapa de transformação ===")
        try:            
            # Usar arquivo fornecido ou resultado da extração
            input_file = input_file or self.results.get('extract')
            if not input_file:
                raise ValueError("Nenhum arquivo de entrada disponível para transformação")           
            
            # Transformar usando modelo dimensional (Star Schema)
            result = self.transformer.transform_to_star_schema(
                input_file=str(input_file)
            )
            
            self.results['transform'] = result
            
            # Log das estatísticas
            self._log_transformation_stats(result)
            
            return result          
        except Exception as e:
            self.logger.error("Erro na transformação: %s", str(e))
            raise
        
    def _log_transformation_stats(self, result: Dict[str, Any]):
        """Registra estatísticas da transformação multidimensional"""
        self.logger.info("\nEstatísticas do processamento Star Schema:")
        
        if result.get('success', False):
            self.logger.info("%s Transformação concluída com sucesso", LOG_EMOJIS['SUCCESS'])
            self.logger.info("%s Registros total: %s", LOG_EMOJIS['DATA'], result.get('records_processed', 'N/A'))
            self.logger.info("%s  Tempo de processamento: %.2fs", LOG_EMOJIS['TIME'], result.get('processing_time_seconds', 0))
            self.logger.info("%s Caminho Silver: %s", LOG_EMOJIS['FILE'], result.get('output_path', 'N/A'))
            
            # Log das dimensões
            dims = result.get('dimensions', {})
            self.logger.info("\n%s Dimensões geradas:", LOG_EMOJIS['DATA'])
            self.logger.info("  - Regiões: %s", dims.get('regiao', 'N/A'))
            self.logger.info("  - Estados: %s", dims.get('estado', 'N/A'))
            self.logger.info("  - Produtos: %s", dims.get('produto', 'N/A'))
            self.logger.info("  - Tempo: %s", dims.get('tempo', 'N/A'))
            self.logger.info("  - Fatos: %s", result.get('fact_records', 'N/A'))
            self.logger.info("\n%s Abordagem: %s", LOG_EMOJIS['PROCESS'], result.get('approach', 'N/A'))
        else:
            self.logger.error("%s Transformação falhou: %s", LOG_EMOJIS['ERROR'], result.get('error', 'Erro desconhecido'))

    def run_pipeline(self, stage: PipelineStage = PipelineStage.ALL) -> None:
        """
        Executa o pipeline ETL completo ou uma etapa específica.
        
        Args:
            stage: Etapa do pipeline a ser executada. Padrão é executar todas.
        """
        self.logger.info("\n=== Iniciando pipeline ETL - Etapa: %s ===", stage.value)
        
        try:
            if stage in [PipelineStage.EXTRACT, PipelineStage.ALL]:
                self.extract()
            
            if stage in [PipelineStage.TRANSFORM, PipelineStage.ALL]:
                self.transform()
            
            if stage in [PipelineStage.LOAD, PipelineStage.ALL]:
                # TODO: Implementar etapa de load quando necessário
                pass
            self.logger.info("\n=== Pipeline ETL concluído com sucesso - Etapa: %s ===", stage.value)
            
        except Exception as e:
            self.logger.error("\n!!! Erro no pipeline ETL: %s !!!", str(e))
            raise

if __name__ == "__main__":
    # Exemplo de uso
    etl = FuelPriceETL()
    
    # Executar pipeline completo
    etl.run_pipeline()
    
    # Ou executar etapas específicas
    # etl.run_pipeline(PipelineStage.EXTRACT)
    # etl.run_pipeline(PipelineStage.TRANSFORM)
