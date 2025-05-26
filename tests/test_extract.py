import unittest
import os
from datetime import datetime
import logging
from src.pipeline.kaggle.upstream.extractor import KaggleExtractor
from src.infrastructure.data_lake_manager import DataLakeManager

class TestKaggleExtractor(unittest.TestCase):
    def setUp(self):
        """Configura o ambiente de teste"""
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        self.data_lake = DataLakeManager()
        self.extractor = KaggleExtractor()
        
    def test_extraction(self):
        """Testa o processo completo de extração"""
        # Verificar credenciais no local padrão
        kaggle_creds_path = os.path.join(os.path.expanduser('~'), '.kaggle', 'kaggle.json')
        self.assertTrue(
            os.path.exists(kaggle_creds_path),
            f"Arquivo de credenciais do Kaggle não encontrado em {kaggle_creds_path}"
        )
        
        # Executar extração
        result = self.extractor.extract_dataset()
        self.assertIsNotNone(result, "A extração falhou")
        
        # Verificar estrutura de diretórios
        bronze_path = self.data_lake.get_bronze_path(
            source='kaggle',
            dataset='gas_prices_in_brazil',
            date=datetime.now()
        )
        self.assertTrue(
            bronze_path.exists(),
            f"Diretório bronze não encontrado: {bronze_path}"
        )
        
        # Verificar arquivo
        tsv_files = list(bronze_path.glob('**/*.tsv'))
        self.assertTrue(
            len(tsv_files) > 0,
            f"Nenhum arquivo TSV encontrado em {bronze_path}"
        )
        
        # Verificar tamanho do arquivo
        file_size = os.path.getsize(tsv_files[0])
        self.assertTrue(
            file_size > 0,
            f"Arquivo vazio: {tsv_files[0]}"
        )

if __name__ == '__main__':
    unittest.main()
