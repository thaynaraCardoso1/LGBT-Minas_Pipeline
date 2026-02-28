import os
import re
from io import BytesIO
from datetime import datetime

import pandas as pd
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from google.cloud import storage


# =========================
# Config (via env ou padrão)
# =========================
BUCKET = os.getenv("TYBYRIA_BUCKET", "lgbtminas-dados")
PREFIX_BASE = os.getenv("TYBYRIA_PREFIX_BASE", "rede social")  # ex: "rede social"
PREFIX_TYBYRIA = os.getenv("ANALYSIS_TYBYRIA_PREFIX", f"{PREFIX_BASE}/analysis/")  # onde estão outputs tybyria
PREFIX_VADER = os.getenv("ANALYSIS_VADER_PREFIX", f"{PREFIX_BASE}/analysis/vader/")  # onde estão outputs vader

OUT_DIR = os.getenv("LOCAL_ANALYSIS_OUTDIR", "analysis_local/out")
TOP_N_SUBREDDITS = int(os.getenv("TOP_N_SUBREDDITS", "20"))
WORDCLOUD_TOP_N = int(os.getenv("WORDCLOUD_TOP_N", "100"))

# Colunas esperadas (ajuste se seu CSV tiver nomes diferentes)
COL_ID = os.getenv("COL_ID", "id")
COL_SUBREDDIT = os.getenv("COL_SUBREDDIT", "subreddit")
COL_TEXT_CLEAN = os.getenv("COL_TEXT_CLEAN", "text_clean")
COL_HAS_LGBT = os.getenv("COL_HAS_LGBT", "has_lgbt_term")

COL_TYBYRIA_SCORE = os.getenv("COL_TYBYRIA_SCORE", "score_tybyria")  # se seu arquivo usa outro nome, troque aqui
COL_VADER_COMPOUND = os.getenv("COL_VADER_COMPOUND", "vader_compound")

# Stopwords básicas PT (bem simples). Pode ampliar depois.
STOPWORDS_PT = set("""
a o os as um uma uns umas de do da dos das em no na nos nas por para com sem que e ou
ao aos à às se não sim já mais menos muito pouca pouco também ainda ela ele eles elas
isso essa esse esses essas aqui aí ali lá eu tu você voces vc vcs nós nos vós
""".split())


# =========================
# Helpers
# =========================
def ensure_outdir():
    os.makedirs(OUT_DIR, exist_ok=True)


def now_tag():
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def sanitize_text(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = s.lower()
    s = re.sub(r"http\S+", " ", s)
    s = re.sub(r"[^a-záàâãéèêíïóôõöúçñ\s]", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def build_wordcloud(texts, title, out_png):
    joined = " ".join([sanitize_text(t) for t in texts if isinstance(t, str) and t.strip()])
    # remove stopwords na marra
    tokens = [w for w in joined.split() if w not in STOPWORDS_PT and len(w) > 2]
    joined2 = " ".join(tokens)

    if not joined2.strip():
        print(f"[wordcloud] Sem conteúdo para: {title}")
        return

    wc = WordCloud(
        width=1600,
        height=900,
        background_color="white",
        collocations=False
    ).generate(joined2)

    plt.figure(figsize=(14, 8))
    plt.imshow(wc, interpolation="bilinear")
    plt.axis("off")
    plt.title(title)
    plt.tight_layout()
    plt.savefig(out_png, dpi=150)
    plt.close()
    print(f"[wordcloud] Salvo: {out_png}")


def gcs_list_csv(bucket, prefix: str):
    blobs = list(bucket.list_blobs(prefix=prefix))
    csvs = [b for b in blobs if b.name.endswith(".csv")]
    return csvs


def gcs_load_csvs(client, bucket_name: str, prefix: str) -> pd.DataFrame:
    bucket = client.bucket(bucket_name)
    csvs = gcs_list_csv(bucket, prefix)

    if not csvs:
        raise RuntimeError(f"Nenhum CSV encontrado em gs://{bucket_name}/{prefix}")

    dfs = []
    for b in csvs:
        print(f"[GCS] Baixando: gs://{bucket_name}/{b.name}")
        data = b.download_as_bytes()
        df = pd.read_csv(BytesIO(data))
        df["source_file"] = b.name.split("/")[-1]
        dfs.append(df)

    out = pd.concat(dfs, ignore_index=True)
    return out


def save_df(df: pd.DataFrame, filename: str):
    path = os.path.join(OUT_DIR, filename)
    df.to_csv(path, index=False)
    print(f"[CSV] Salvo: {path}")


def plot_bar(series: pd.Series, title: str, out_png: str, ascending=False):
    plt.figure(figsize=(12, 6))
    series.sort_values(ascending=ascending).plot(kind="bar")
    plt.title(title)
    plt.ylabel("valor")
    plt.tight_layout()
    plt.savefig(out_png, dpi=150)
    plt.close()
    print(f"[PLOT] Salvo: {out_png}")


# =========================
# Main analysis
# =========================
def main():
    ensure_outdir()
    tag = now_tag()

    client = storage.Client()

    # 1) Carregar dataframes
    df_ty = gcs_load_csvs(client, BUCKET, PREFIX_TYBYRIA)
    df_vd = gcs_load_csvs(client, BUCKET, PREFIX_VADER)

    print("[INFO] Tybyria shape:", df_ty.shape)
    print("[INFO] Vader shape:", df_vd.shape)

    # 2) Checagens mínimas de colunas
    for col in [COL_ID, COL_SUBREDDIT]:
        if col not in df_ty.columns:
            raise RuntimeError(f"Tybyria sem coluna obrigatória: {col}")
        if col not in df_vd.columns:
            raise RuntimeError(f"Vader sem coluna obrigatória: {col}")

    if COL_TYBYRIA_SCORE not in df_ty.columns:
        raise RuntimeError(f"Tybyria sem coluna de score: {COL_TYBYRIA_SCORE}")

    if COL_VADER_COMPOUND not in df_vd.columns:
        raise RuntimeError(f"Vader sem coluna compound: {COL_VADER_COMPOUND}")

    # 3) Agregados: média por subreddit
    ty_sub = (df_ty.groupby(COL_SUBREDDIT)[COL_TYBYRIA_SCORE]
              .mean()
              .sort_values(ascending=False)
              .head(TOP_N_SUBREDDITS))

    vd_sub = (df_vd.groupby(COL_SUBREDDIT)[COL_VADER_COMPOUND]
              .mean()
              .sort_values(ascending=True)  # compound: negativos primeiro
              .head(TOP_N_SUBREDDITS))

    save_df(ty_sub.reset_index().rename(columns={COL_TYBYRIA_SCORE: "mean_tybyria"}),
            f"agg_tybyria_subreddit_top{TOP_N_SUBREDDITS}_{tag}.csv")

    save_df(vd_sub.reset_index().rename(columns={COL_VADER_COMPOUND: "mean_vader_compound"}),
            f"agg_vader_subreddit_top{TOP_N_SUBREDDITS}_{tag}.csv")

    plot_bar(
        ty_sub,
        f"Média Tybyria por Subreddit (Top {TOP_N_SUBREDDITS})",
        os.path.join(OUT_DIR, f"plot_tybyria_by_subreddit_top{TOP_N_SUBREDDITS}_{tag}.png"),
        ascending=False
    )

    plot_bar(
        vd_sub,
        f"Média VADER compound por Subreddit (Top {TOP_N_SUBREDDITS} mais negativos)",
        os.path.join(OUT_DIR, f"plot_vader_by_subreddit_top{TOP_N_SUBREDDITS}_{tag}.png"),
        ascending=True
    )

    # 4) Merge Tybyria x Vader (por id)
    common_cols_ty = {COL_ID, COL_SUBREDDIT, COL_TEXT_CLEAN, COL_HAS_LGBT, COL_TYBYRIA_SCORE}
    common_cols_vd = {COL_ID, COL_VADER_COMPOUND}

    keep_ty = [c for c in df_ty.columns if c in common_cols_ty]
    keep_vd = [c for c in df_vd.columns if c in common_cols_vd]

    df_ty_small = df_ty[keep_ty].copy()
    df_vd_small = df_vd[keep_vd].copy()

    df_merge = df_ty_small.merge(df_vd_small, on=COL_ID, how="inner")

    print("[INFO] Merge shape (inner on id):", df_merge.shape)
    save_df(df_merge, f"merge_tybyria_vader_{tag}.csv")

    # 5) Scatter Tybyria x Vader
    plt.figure(figsize=(8, 6))
    plt.scatter(df_merge[COL_TYBYRIA_SCORE], df_merge[COL_VADER_COMPOUND], alpha=0.2, s=10)
    plt.xlabel("Tybyria score")
    plt.ylabel("VADER compound")
    plt.title("Tybyria vs VADER (por ID)")
    plt.tight_layout()
    out_scatter = os.path.join(OUT_DIR, f"scatter_tybyria_vs_vader_{tag}.png")
    plt.savefig(out_scatter, dpi=150)
    plt.close()
    print(f"[PLOT] Salvo: {out_scatter}")

    # 6) Diferenças: distribuições (hist simples)
    plt.figure(figsize=(8, 6))
    df_merge[COL_TYBYRIA_SCORE].dropna().plot(kind="hist", bins=50)
    plt.title("Distribuição Tybyria score")
    plt.tight_layout()
    out_hist_ty = os.path.join(OUT_DIR, f"hist_tybyria_{tag}.png")
    plt.savefig(out_hist_ty, dpi=150)
    plt.close()
    print(f"[PLOT] Salvo: {out_hist_ty}")

    plt.figure(figsize=(8, 6))
    df_merge[COL_VADER_COMPOUND].dropna().plot(kind="hist", bins=50)
    plt.title("Distribuição VADER compound")
    plt.tight_layout()
    out_hist_vd = os.path.join(OUT_DIR, f"hist_vader_{tag}.png")
    plt.savefig(out_hist_vd, dpi=150)
    plt.close()
    print(f"[PLOT] Salvo: {out_hist_vd}")

    # 7) Wordcloud: Top N maiores scores Tybyria
    if COL_TEXT_CLEAN in df_merge.columns:
        top_ty = df_merge.sort_values(COL_TYBYRIA_SCORE, ascending=False).head(WORDCLOUD_TOP_N)
        build_wordcloud(
            top_ty[COL_TEXT_CLEAN].tolist(),
            f"Nuvem de palavras — Top {WORDCLOUD_TOP_N} Tybyria",
            os.path.join(OUT_DIR, f"wordcloud_top{WORDCLOUD_TOP_N}_tybyria_{tag}.png")
        )

        # 8) Wordcloud separado: só registros com termo LGBT
        if COL_HAS_LGBT in df_merge.columns:
            lgbt_df = df_merge[df_merge[COL_HAS_LGBT] == 1]
            top_lgbt = lgbt_df.sort_values(COL_TYBYRIA_SCORE, ascending=False).head(WORDCLOUD_TOP_N)

            build_wordcloud(
                top_lgbt[COL_TEXT_CLEAN].tolist(),
                f"Nuvem de palavras — Top {WORDCLOUD_TOP_N} Tybyria (has_lgbt_term=1)",
                os.path.join(OUT_DIR, f"wordcloud_top{WORDCLOUD_TOP_N}_tybyria_lgbt_{tag}.png")
            )
    else:
        print(f"[WARN] Coluna {COL_TEXT_CLEAN} não existe no merge; pulando wordcloud.")

    # 9) Comparação por flag LGBT (médias)
    if COL_HAS_LGBT in df_merge.columns:
        comp_lgbt = df_merge.groupby(COL_HAS_LGBT)[[COL_TYBYRIA_SCORE, COL_VADER_COMPOUND]].mean().reset_index()
        save_df(comp_lgbt, f"compare_means_by_has_lgbt_{tag}.csv")

        # Plot simples
        plt.figure(figsize=(8, 6))
        plt.bar(comp_lgbt[COL_HAS_LGBT].astype(str), comp_lgbt[COL_TYBYRIA_SCORE])
        plt.title("Média Tybyria por has_lgbt_term (0/1)")
        plt.tight_layout()
        out_lgbt_ty = os.path.join(OUT_DIR, f"bar_mean_tybyria_by_has_lgbt_{tag}.png")
        plt.savefig(out_lgbt_ty, dpi=150)
        plt.close()
        print(f"[PLOT] Salvo: {out_lgbt_ty}")

        plt.figure(figsize=(8, 6))
        plt.bar(comp_lgbt[COL_HAS_LGBT].astype(str), comp_lgbt[COL_VADER_COMPOUND])
        plt.title("Média VADER compound por has_lgbt_term (0/1)")
        plt.tight_layout()
        out_lgbt_vd = os.path.join(OUT_DIR, f"bar_mean_vader_by_has_lgbt_{tag}.png")
        plt.savefig(out_lgbt_vd, dpi=150)
        plt.close()
        print(f"[PLOT] Salvo: {out_lgbt_vd}")

    print("\n✅ Finalizado. Resultados em:", OUT_DIR)


if __name__ == "__main__":
    main()
