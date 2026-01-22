import pandas as pd
import glob
import os
from src.utils.limpeza import limpar_dataframe_resultados

# Configurações de caminhos
ANALYSIS_DIR = 'bases/rede social/reddit/analysis/'
EXPORT_DIR = 'bases/rede social/reddit/export/'
os.makedirs(EXPORT_DIR, exist_ok=True)

def consolidar_e_exportar():
    # 1. Busca todos os arquivos processados pelo TybyrIA
    arquivos = glob.glob(os.path.join(ANALYSIS_DIR, "*_tybyria.csv"))
    
    if not arquivos:
        print("❌ Nenhum arquivo de análise encontrado.")
        return

    lista_df = []
    for f in arquivos:
        df_temp = pd.read_csv(f)
        lista_df.append(df_temp)
    
    # 2. Une todos os meses/arquivos em um só
    df_total = pd.concat(lista_df, ignore_index=True)
    print(f"📊 Total original: {len(df_total)} linhas.")

    # 3. Limpeza (usando sua utils)
    df_total = limpar_dataframe_resultados(df_total)
    print(f"🧹 Total após remover brancos/nulos: {len(df_total)} linhas.")

    # 4. Salvar em Excel
    output_xlsx = os.path.join(EXPORT_DIR, "resultados_completos_lgbt_minas.xlsx")
    
    # Se o arquivo for muito grande (>1 milhão de linhas), o Excel não abre.
    # Nesse caso, salvamos apenas uma amostra ou mantemos em CSV.
    if len(df_total) < 1000000:
        df_total.to_excel(output_xlsx, index=False, engine='openpyxl')
        print(f"✅ Exportado com sucesso para: {output_xlsx}")
    else:
        print("⚠️ Base muito grande para Excel. Salvando CSV consolidado.")
        df_total.to_csv(output_xlsx.replace(".xlsx", ".csv"), index=False)

if __name__ == "__main__":
    consolidar_e_exportar()