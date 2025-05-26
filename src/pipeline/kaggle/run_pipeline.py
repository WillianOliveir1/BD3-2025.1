"""
Script principal para execução do pipeline ETL - Análise de Preços de Combustíveis.
Este é o ponto de entrada principal para executar o pipeline completo.
"""
import sys
from pathlib import Path
import logging

# Adiciona o diretório raiz ao PYTHONPATH
root_dir = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(root_dir))

from src.pipeline.kaggle.etl_pipeline import FuelPriceETL

def main():
    """Executa o pipeline ETL completo"""
    print("🚀 Iniciando Pipeline ETL - Análise de Preços de Combustíveis")
    print("=" * 60)
    
    try:
        # Inicializar e executar pipeline
        etl = FuelPriceETL()
        etl.run_pipeline()
        
        print("\n" + "=" * 60)
        print("🎉 Pipeline ETL executado com sucesso!")
        print("✅ Dados extraídos do Kaggle")
        print("✅ Dados transformados e salvos na camada Silver")
        print("📁 Verifique a pasta 'data/silver/fuel_prices_hybrid/' para os resultados")
        
    except KeyboardInterrupt:
        print("\n⚠️  Pipeline interrompido pelo usuário")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Erro no pipeline: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
