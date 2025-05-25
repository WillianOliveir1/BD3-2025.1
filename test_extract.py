"""
Script para testar a extração de dados do Kaggle.
"""
from src.etl_pipeline import FuelPriceETL, PipelineStage

def main():
    # Inicializar o pipeline
    etl = FuelPriceETL()
    
    # Executar apenas a etapa de extração
    print("\nIniciando teste de extração...")
    etl.run_pipeline(PipelineStage.EXTRACT)
    print("\nExtração concluída!")

if __name__ == "__main__":
    main()
