"""
Script de teste para verificar o pipeline ETL reorganizado
Testa apenas a etapa de transformação com dados existentes
"""

import sys
from pathlib import Path
import logging

# Adicionar caminho raiz do projeto
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(name)s] %(levelname)s: %(message)s')

from src.pipeline.kaggle.etl_pipeline import FuelPriceETL, PipelineStage

def test_reorganized_pipeline():
    """Testa o pipeline ETL reorganizado"""
    
    print("="*70)
    print("🧪 TESTE DO PIPELINE ETL REORGANIZADO")
    print("="*70)
    
    try:
        # Inicializar pipeline
        print("\n📋 Inicializando pipeline...")
        etl = FuelPriceETL()
        
        # Encontrar arquivo Bronze mais recente para teste
        bronze_path = project_root / "data" / "bronze" / "kaggle" / "gas_prices_in_brazil"
        
        latest_bronze = None
        if bronze_path.exists():
            for date_dir in sorted(bronze_path.iterdir(), reverse=True):
                if date_dir.is_dir():
                    tsv_file = date_dir / "2004-2021.tsv"
                    if tsv_file.exists():
                        latest_bronze = tsv_file
                        break
          # Verificar se temos dados para testar
        assert latest_bronze is not None, "❌ Nenhum arquivo Bronze encontrado para teste"
            
        print(f"📂 Usando arquivo Bronze: {latest_bronze}")
        
        # Testar apenas transformação (dados já extraídos)
        print("\n🔄 Testando etapa de transformação...")
        result = etl.transform(input_file=latest_bronze)
        
        # Verificar resultado
        if result and result.get('success', False):
            print("\n✅ TESTE CONCLUÍDO COM SUCESSO!")
            
            # Verificar estrutura Silver criada
            silver_path = project_root / "data" / "silver" / "kaggle" / "gas_prices_in_brazil"
            
            print(f"\n📊 Verificando estrutura Silver em: {silver_path}")
            
            expected_files = [
                "dim_regiao.parquet",
                "dim_estado.parquet", 
                "dim_produto.parquet",
                "dim_tempo.parquet",
                "fact_precos.parquet"
            ]
            
            for file_name in expected_files:
                file_path = silver_path / file_name
                if file_path.exists():
                    size_mb = file_path.stat().st_size / (1024 * 1024)
                    print(f"   ✅ {file_name} ({size_mb:.2f} MB)")
                else:
                    print(f"   ❌ {file_name} - AUSENTE")
            
            return True
        else:
            print("❌ TESTE FALHOU!")
            print(f"Erro: {result.get('error', 'Erro desconhecido') if result else 'Resultado vazio'}")
            return False
            
    except Exception as e:
        print(f"❌ ERRO NO TESTE: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def verify_star_schema_structure():
    """Verifica se a estrutura star schema está correta"""
    
    print("\n" + "="*70)
    print("🔍 VERIFICAÇÃO DA ESTRUTURA STAR SCHEMA")
    print("="*70)
    
    silver_path = project_root / "data" / "silver" / "kaggle" / "gas_prices_in_brazil"
    
    if not silver_path.exists():
        print("❌ Diretório Silver não encontrado!")
        return False
    
    try:
        import pandas as pd
        
        # Verificar cada dimensão e tabela fato
        tables = {
            "dim_regiao.parquet": "Dimensão de Regiões",
            "dim_estado.parquet": "Dimensão de Estados",
            "dim_produto.parquet": "Dimensão de Produtos",
            "dim_tempo.parquet": "Dimensão de Tempo",
            "fact_precos.parquet": "Tabela Fato de Preços"
        }
        
        total_size = 0
        
        for file_name, description in tables.items():
            file_path = silver_path / file_name
            
            if file_path.exists():
                df = pd.read_parquet(file_path)
                size_mb = file_path.stat().st_size / (1024 * 1024)
                total_size += size_mb
                
                print(f"✅ {description}")
                print(f"   📁 {file_name}")
                print(f"   📊 Registros: {len(df):,}")
                print(f"   💾 Tamanho: {size_mb:.2f} MB")
                print(f"   🔧 Colunas: {list(df.columns)}")
                print()
            else:
                print(f"❌ {description} - AUSENTE")
                print(f"   📁 {file_name}")
                print()
          print(f"📈 TOTAL: {total_size:.2f} MB em estrutura star schema")
        print("🎯 Estrutura otimizada para queries Spark SQL!")
        
    except Exception as e:
        print(f"❌ Erro na verificação: {str(e)}")
        raise

if __name__ == "__main__":
    print("🚀 Iniciando testes do pipeline ETL reorganizado...")
    
    # Testar pipeline
    pipeline_ok = test_reorganized_pipeline()
    
    # Verificar estrutura
    structure_ok = verify_star_schema_structure()
    
    print("\n" + "="*70)
    print("📋 RESULTADO FINAL DOS TESTES")
    print("="*70)
    print(f"Pipeline ETL: {'✅ OK' if pipeline_ok else '❌ FALHOU'}")
    print(f"Star Schema: {'✅ OK' if structure_ok else '❌ FALHOU'}")
    
    if pipeline_ok and structure_ok:
        print("\n🎉 REORGANIZAÇÃO CONCLUÍDA COM SUCESSO!")
        print("   • Pipeline upstream/midstream/downstream funcionando")
        print("   • Estrutura Silver otimizada para Spark SQL")
        print("   • Dados em formato Parquet apenas")
        print("   • Star schema multidimensional implementado")
    else:
        print("\n⚠️ PROBLEMAS DETECTADOS - Verifique logs acima")
