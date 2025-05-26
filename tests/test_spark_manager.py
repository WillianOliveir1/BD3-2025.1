"""
Testes para o gerenciador simplificado de sessões Spark
"""
import logging
import pytest
import os
from src.infrastructure.spark_manager import SparkManager

# Verificar ambiente antes dos testes
def verify_env():
    """Verifica se o ambiente está configurado corretamente"""
    required_vars = ["JAVA_HOME"]
    missing = [var for var in required_vars if var not in os.environ]
    if missing:
        pytest.skip(f"Variáveis de ambiente faltando: {', '.join(missing)}")
    
    # Verificar se os diretórios existem
    for var in required_vars:
        path = os.environ[var]
        if not os.path.exists(path):
            pytest.skip(f"Diretório {var} não encontrado: {path}")

# Executar verificação antes dos testes
verify_env()

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@pytest.fixture
def spark_manager():
    """Fixture que fornece uma instância do gerenciador Spark"""
    manager = SparkManager(app_name="TestSparkApp")
    yield manager
    manager.stop_session()

def test_spark_session_lifecycle(spark_manager):
    """Testa o ciclo de vida completo da sessão Spark"""
    # Verificar que não há sessão inicial
    assert spark_manager.spark is None, "Spark não deveria estar iniciado"
    
    # Iniciar sessão
    spark = spark_manager.start_session()
    assert spark is not None, "Sessão Spark não foi iniciada"
    assert spark_manager.spark is not None, "Sessão Spark não foi armazenada"
    
    # Verificar se a sessão está ativa
    try:
        result = spark.sparkContext.parallelize([1, 2, 3]).sum()
        assert result == 6, "Operação Spark falhou"
    except Exception as e:
        pytest.fail(f"Erro ao executar operação Spark: {str(e)}")
    
    # Parar sessão
    spark_manager.stop_session()
    assert spark_manager.spark is None, "Sessão Spark não foi encerrada"

def test_spark_config_application(spark_manager):
    """Testa se as configurações são aplicadas corretamente"""
    test_config = {
        "spark.app.name": "TestApp",
        "spark.driver.memory": "4g",
        "spark.executor.memory": "4g"
    }
    
    spark_manager.config.update(test_config)
    spark = spark_manager.start_session()
    
    conf = dict(spark.sparkContext.getConf().getAll())
    for key, value in test_config.items():
        assert conf.get(key) == value, f"Configuração {key} não foi aplicada corretamente"

def test_spark_session_reuse(spark_manager):
    """Testa a reutilização da sessão Spark"""
    # Primeira inicialização
    spark1 = spark_manager.start_session()
    assert spark1 is not None, "Primeira sessão não foi iniciada"
    
    # Segunda chamada - deve retornar a mesma sessão
    spark2 = spark_manager.start_session()
    assert spark2 is spark1, "Nova sessão foi criada em vez de reutilizar a existente"
    
    # Verificar se a sessão ainda está utilizável
    try:
        result = spark2.sparkContext.parallelize([1, 2, 3]).sum()
        assert result == 6, "Sessão reutilizada não está funcionando"
    except Exception as e:
        pytest.fail(f"Erro ao usar sessão reutilizada: {str(e)}")

def test_spark_context_manager():
    """Testa o uso do SparkManager como context manager"""
    with SparkManager(app_name="TestContextApp") as manager:
        spark = manager.spark
        assert spark is not None, "Sessão não foi iniciada no context manager"
        
        # Testar operação
        result = spark.sparkContext.parallelize([1, 2, 3]).sum()
        assert result == 6, "Operação falhou no context manager"
    
    # Verificar limpeza após sair do context
    assert manager.spark is None, "Sessão não foi limpa após context manager"
