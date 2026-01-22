import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import glob

# 1. Configurações de Caminhos
PATH_ANALYSIS = os.path.join("bases", "rede social", "reddit", "analysis")
ARQUIVOS_PATTERN = os.path.join(PATH_ANALYSIS, "*_vader.csv")
COLUNA_SCORE = "vader_compound"
OUTPUT_IMG = os.path.join(PATH_ANALYSIS, "grafico_densidade_vader.png")

def main():
    arquivos = glob.glob(ARQUIVOS_PATTERN)
    
    if not arquivos:
        print(f"❌ Nenhum arquivo encontrado em: {PATH_ANALYSIS}")
        return

    print(f"📂 Arquivos encontrados ({len(arquivos)}):")
    for f in arquivos:
        print(f"  - {os.path.basename(f)}")

    # 3. Carregar e Consolidar
    lista_dfs = []
    for f in arquivos:
        df_temp = pd.read_csv(f, usecols=[COLUNA_SCORE])
        lista_dfs.append(df_temp)
    
    df_total = pd.concat(lista_dfs, ignore_index=True)

    # 4. Conversão e limpeza
    df_total[COLUNA_SCORE] = pd.to_numeric(df_total[COLUNA_SCORE], errors='coerce')
    df_total = df_total.dropna(subset=[COLUNA_SCORE])
    print(f"📊 Total de linhas para o gráfico: {len(df_total)}")

    # 5. Criar gráfico
    plt.figure(figsize=(12, 6))
    sns.set_style("whitegrid")
    sns.kdeplot(df_total[COLUNA_SCORE], fill=True, color="blue", alpha=0.4, bw_adjust=0.5)

    v_min = df_total[COLUNA_SCORE].min()
    v_max = df_total[COLUNA_SCORE].max()
    
    plt.axvline(v_min, color="red", linestyle="--", label=f"Mín: {v_min:.4f}")
    plt.axvline(v_max, color="green", linestyle="--", label=f"Máx: {v_max:.4f}")

    # Linha em zero (neutro)
    plt.axvline(0.0, color="black", linestyle=":", linewidth=1, label="Neutro (0.0)")

    # Personalização
    plt.title("Densidade de Distribuição: Sentimento (VADER)", fontsize=14)
    plt.xlabel("Compound Score (de -1 = Negativo até +1 = Positivo)")
    plt.ylabel("Densidade (Concentração de Comentários)")
    plt.xlim(-1, 1)
    plt.legend()

    plt.tight_layout()
    plt.savefig(OUTPUT_IMG, dpi=300)
    print(f"✅ Gráfico salvo com sucesso em: {OUTPUT_IMG}")

if __name__ == "__main__":
    main()
