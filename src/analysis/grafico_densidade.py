import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import glob

# 1. Configurações de Caminhos
# Usamos o glob para pegar TODOS os arquivos que terminam com _tybyria.csv
PATH_ANALYSIS = os.path.join("bases", "rede social", "reddit", "analysis")
ARQUIVOS_PATTERN = os.path.join(PATH_ANALYSIS, "*_tybyria.csv")
COLUNA_SCORE = "tybyria_score"
OUTPUT_IMG = os.path.join(PATH_ANALYSIS, "grafico_densidade_consolidado.png")

def main():
    # 2. Localizar os arquivos
    arquivos = glob.glob(ARQUIVOS_PATTERN)
    
    if len(arquivos) == 0:
        print(f"❌ Nenhum arquivo encontrado em: {PATH_ANALYSIS}")
        return

    print(f"📂 Arquivos encontrados ({len(arquivos)}):")
    for f in arquivos:
        print(f"  - {os.path.basename(f)}")

    # 3. Carregar e Consolidar (Unir os dois arquivos)
    lista_dfs = []
    for f in arquivos:
        # Lemos apenas a coluna de score para ser mais rápido
        df_temp = pd.read_csv(f, usecols=[COLUNA_SCORE])
        lista_dfs.append(df_temp)
    
    df_total = pd.concat(lista_dfs, ignore_index=True)

    # 4. Limpeza e Conversão
    # Garante que o score seja numérico (o pandas resolve o problema do ponto/vírgula do Excel)
    df_total[COLUNA_SCORE] = pd.to_numeric(df_total[COLUNA_SCORE], errors='coerce')
    df_total = df_total.dropna(subset=[COLUNA_SCORE])

    print(f"📊 Total de linhas para o gráfico: {len(df_total)}")

    # 5. Criar Gráfico de Densidade (KDE)
    plt.figure(figsize=(12, 6))
    sns.set_style("whitegrid")

    # KDE Plot (Densidade)
    sns.kdeplot(df_total[COLUNA_SCORE], fill=True, color="purple", alpha=0.5, bw_adjust=0.5)

    # 6. Marcar os Extremos (Mínimo e Máximo)
    v_min = df_total[COLUNA_SCORE].min()
    v_max = df_total[COLUNA_SCORE].max()
    
    plt.axvline(v_min, color="red", linestyle="--", alpha=0.7, label=f"Mín: {v_min:.4f}")
    plt.axvline(v_max, color="green", linestyle="--", alpha=0.7, label=f"Máx: {v_max:.4f}")
    
    # Linha do seu Threshold (opcional, mas bom para o mestrado)
    plt.axvline(0.30, color="orange", linestyle="-", linewidth=1, label="Threshold (0.30)")

    # Customização
    plt.title("Densidade de Distribuição: Scores de Hostilidade (TybyrIA)", fontsize=14)
    plt.xlabel("Score (0 = Neutro | 1 = Hostil)")
    plt.ylabel("Densidade (Concentração de Comentários)")
    plt.xlim(0, 1) # Garante a escala de 0 a 1
    plt.legend()

    # 7. Salvar e Finalizar
    plt.tight_layout()
    plt.savefig(OUTPUT_IMG, dpi=300)
    print(f"✅ Gráfico salvo com sucesso em: {OUTPUT_IMG}")

if __name__ == "__main__":
    main()