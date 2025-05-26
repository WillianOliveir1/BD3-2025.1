"""
Testes para o gerenciador de sessões Spark
"""
import os
import pytest
from src.infrastructure.spark_manager import SparkManager
from src.infrastructure.logging_config import setup_logger, LOG_EMOJIS

# Configurar logger para testes
logger = setup_logger('TestSpark')

def verify_env():
    """Verifica se o ambiente está configurado corretamente"""
    required_vars = ["JAVA_HOME"]
    missing = [var for var in required_vars if var not in os.environ]
    if missing:
        logger.error("%s Variáveis de ambiente faltando: %s", LOG_EMOJIS['ERROR'], ', '.join(missing))
        pytest.skip("Variáveis de ambiente faltando: %s" % ', '.join(missing))
    
    # Verificar se os diretórios existem
    for var in required_vars:
        path = os.environ[var]
        if not os.path.exists(path):
            logger.error("%s Diretório %s não encontrado: %s", LOG_EMOJIS['ERROR'], var, path)
            pytest.skip("Diretório %s não encontrado: %s" % (var, path))
            
    logger.info("%s Ambiente validado com sucesso", LOG_EMOJIS['SUCCESS'])

# Executar verificação antes dos testes
verify_env()

@pytest.fixture
def spark_manager():
    """Fixture que fornece uma instância do gerenciador Spark"""
    logger.info("%s Iniciando gerenciador Spark para teste", LOG_EMOJIS['START'])
    manager = SparkManager(app_name="TestSparkApp")
    yield manager
    manager.stop_session()
    logger.info("%s Gerenciador Spark finalizado", LOG_EMOJIS['END'])

def test_spark_session_lifecycle(spark_manager):
    """Testa o ciclo de vida completo da sessão Spark"""
    logger.info("%s Testando ciclo de vida da sessão Spark", LOG_EMOJIS['START'])
    
    # Verificar que não há sessão inicial
    assert spark_manager.spark is None, "Spark não deveria estar iniciado"
    logger.info("%s Estado inicial verificado", LOG_EMOJIS['SUCCESS'])
    
    # Iniciar sessão
    logger.info("%s Iniciando sessão Spark", LOG_EMOJIS['PROCESS'])
    spark = spark_manager.start_session()
    assert spark is not None, "Sessão Spark não foi iniciada"
    assert spark_manager.spark is not None, "Sessão Spark não foi armazenada"
    logger.info("%s Sessão Spark iniciada com sucesso", LOG_EMOJIS['SUCCESS'])
    
    # Verificar se a sessão está ativa
    try:
        logger.info("%s Testando operação Spark", LOG_EMOJIS['PROCESS'])
        result = spark.sparkContext.parallelize([1, 2, 3]).sum()
        assert result == 6, "Operação Spark falhou"
        logger.info("%s Operação Spark executada com sucesso", LOG_EMOJIS['SUCCESS'])
    except Exception as e:
        pytest.fail("Erro ao executar operação Spark: %s" % str(e))
    
    # Parar sessão
    spark_manager.stop_session()
    assert spark_manager.spark is None, "Sessão Spark não foi encerrada"
    logger.info("%s Sessão Spark encerrada com sucesso", LOG_EMOJIS['SUCCESS'])

def test_spark_config_application(spark_manager):
    """Testa se as configurações são aplicadas corretamente"""
    logger.info("%s Testando aplicação de configurações na sessão Spark", LOG_EMOJIS['START'])
    test_config = {
        "spark.app.name": "TestApp",
        "spark.driver.memory": "4g",
        "spark.executor.memory": "4g"
    }
    
    spark_manager.config.update(test_config)
    spark = spark_manager.start_session()
    
    conf = dict(spark.sparkContext.getConf().getAll())
    for key, value in test_config.items():
        assert conf.get(key) == value, "Configuração %s não foi aplicada corretamente" % key
        logger.info("%s Configuração %s verificada com sucesso", LOG_EMOJIS['SUCCESS'], key)

def test_spark_session_reuse(spark_manager):
    """Testa a reutilização da sessão Spark"""
    logger.info("%s Testando reutilização da sessão Spark", LOG_EMOJIS['START'])
    # Primeira inicialização
    spark1 = spark_manager.start_session()
    assert spark1 is not None, "Primeira sessão não foi iniciada"
    logger.info("%s Primeira sessão iniciada com sucesso", LOG_EMOJIS['SUCCESS'])
    
    # Segunda chamada - deve retornar a mesma sessão
    spark2 = spark_manager.start_session()
    assert spark2 is spark1, "Nova sessão foi criada em vez de reutilizar a existente"
    logger.info("%s Sessão reutilizada com sucesso", LOG_EMOJIS['SUCCESS'])
    
    # Verificar se a sessão ainda está utilizável
    try:
        result = spark2.sparkContext.parallelize([1, 2, 3]).sum()
        assert result == 6, "Sessão reutilizada não está funcionando"
        logger.info("%s Verificação de funcionalidade da sessão reutilizada bem-sucedida", LOG_EMOJIS['SUCCESS'])
    except Exception as e:
        pytest.fail("Erro ao usar sessão reutilizada: %s" % str(e))

def test_spark_context_manager():
    """Testa o uso do SparkManager como context manager"""
    logger.info("%s Testando SparkManager como context manager", LOG_EMOJIS['START'])
    with SparkManager(app_name="TestContextApp") as manager:
        spark = manager.spark
        assert spark is not None, "Sessão não foi iniciada no context manager"
        
        # Testar operação
        result = spark.sparkContext.parallelize([1, 2, 3]).sum()
        assert result == 6, "Operação falhou no context manager"
        logger.info("%s Operação no context manager executada com sucesso", LOG_EMOJIS['SUCCESS'])
    
    # Verificar limpeza após sair do context
    assert manager.spark is None, "Sessão não foi limpa após context manager"
