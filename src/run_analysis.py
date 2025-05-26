from src.etl_pipeline import FuelPriceETL
from src.analysis.fuel_price_analyzer import FuelPriceAnalyzer
from pathlib import Path
import matplotlib.pyplot as plt

def main():
    # Executar o pipeline ETL se necessário
    etl = FuelPriceETL()
    etl.run_pipeline()
    
    # Iniciar análises
    analyzer = FuelPriceAnalyzer()
    df = analyzer.load_data()
    
    print("\n=== Análise de Preços de Combustíveis ===")
    
    # 1. Análise de valores extremos
    print("\n1. Análise de valores máximos e mínimos:")
    price_extremes = analyzer.analyze_price_extremes(df)
    
    print("\nGasolina Comum no Brasil:")
    price_extremes["brasil_gas"].show()
    
    print("\nPor Estado:")
    price_extremes["estado_gas"].show()
    
    print("\nPor Estado e Ano:")
    price_extremes["estado_ano_gas"].show()
    
    # 2. Análise do combustível mais barato
    print("\n2. Análise do combustível mais barato:")
    cheapest_fuel = analyzer.analyze_cheapest_fuel(df)
    
    print("\nNo Brasil:")
    cheapest_fuel["brasil_cheap"].show()
    
    print("\nPor Estado e Ano (Top 2):")
    cheapest_fuel["top2_estado_ano"].show()
    
    # 3. Análise Etanol vs Gasolina
    print("\n3. Análise comparativa Etanol vs Gasolina:")
    ethanol_analysis = analyzer.analyze_ethanol_vs_gasoline(df)
    ethanol_analysis.show()
    
    # 4. Análise de receita
    print("\n4. Análise de ticket médio e receita:")
    revenue_analysis = analyzer.calculate_revenue(df)
    
    print("\nTicket Médio por Estado, Mês e Ano:")
    revenue_analysis["ticket_medio"].show()
    
    print("\nReceita Anual por Combustível:")
    revenue_analysis["receita_anual"].show()
    
    # 5. Box-plots comparativos (ponto extra)
    print("\n5. Gerando box-plots comparativos...")
    plots = analyzer.plot_price_evolution(df)
    plots_path = Path("output/plots")
    plots_path.mkdir(parents=True, exist_ok=True)
    plots.savefig(plots_path / "price_evolution.png")
    plt.close()
    
    print(f"\nGráficos salvos em: {plots_path}/price_evolution.png")

if __name__ == "__main__":
    main()
