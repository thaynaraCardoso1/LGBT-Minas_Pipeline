cat > README.md << 'EOF'
# 🏳️‍🌈 LGBT+ Minas — Coleta e Análise de Discurso de Ódio em Redes Sociais

Este projeto implementa um pipeline de **coleta, filtragem, limpeza e análise** de dados provenientes de redes sociais (Mastodon e Reddit) para identificar e caracterizar **discurso de ódio direcionado à comunidade LGBT+** em conteúdos relacionados ao estado de **Minas Gerais (Brasil)**.

A pesquisa integra dados de:
- 🌐 Redes sociais públicas (Mastodon, Reddit)
- 📍 Filtros geográficos (cidades de Minas Gerais)
- 🏳️‍🌈 Termos LGBT+
- ⚠️ Termos de discurso de ódio
- 🧠 Identificação automática de idioma (português)
- 💾 Processamento de dumps massivos Reddit (`.zst`, dezenas de GB)

O projeto faz parte da dissertação de mestrado da autora.

---

## ✨ Objetivos

- Coletar e processar grandes volumes de dados textuais.
- Detectar menções LGBT+ associadas a discurso de ódio.
- Restringir análise a conteúdos potencialmente localizáveis em MG.
- Criar dataset filtrado para análise linguística e modelos de NLP.
- Estabelecer pipeline reprodutível e documentado.

---

## 📂 Estrutura do Projeto
LGBT+Minas/
│
├── bases/ # Dados locais (não versionados)
│ └── rede social/
│ └── reddit/
│ ├── raw/ # Dumps (.zst)
│ ├── processed/ # CSVs gerados
│ └── tmp/
│
├── configs/ # Termos, cidades, parâmetros
│ ├── filtros/
│ │ ├── cidades_mg.txt
│ │ ├── termos_lgbt.txt
│ │ └── termos_odio.txt
│ └── global.json
│
├── src/
│ ├── reddit/
│ │ ├── process_dump.py # Pipeline Reddit
│ │ ├── filters.py # Filtros MG + LGBT + Ódio
│ │ └── config.py
│ ├── mastodon/ # Scripts de coleta Mastodon
│ └── utils/
│ ├── lang/ # Detectores de idioma
│ ├── logger.py
│ └── load_config.py
│
├── logs/ # Logs de processamento
└── README.md # (este arquivo)


---

## ⚙️ Dependências

Instale com:

```bash
pip install -r requirements.txt
```

## Principais bibliotecas:
```bash
pandas
zstandard
requests
beautifulsoup4
langdetect
```

🧵 Como rodar o pipeline Reddit
1. Coloque os dumps .zst em:
```bash
bases/rede social/reddit/raw/
```

2. Rode:
```bash
python3 -m src.reddit.process_dump
```

O script:
descompacta o .zst em streaming
filtra idioma (pt)
filtra termos LGBT+, ódio e cidades de MG
salva incrementalmente no CSV
gera logs detalhados

🚫 Dados não versionados

Importante: Nenhum dump, CSV processado ou modelo é enviado ao Git.

Veja .gitignore para mais detalhes.

📜 Licença

Uso acadêmico e educacional.

✍️ Autora

Tata (Thaynara Alexandre Cardoso)
Mestrado em Informática – UNIRIO
Arquiteta de Aplicações • Pesquisadora em NLP
EOF


---

# 🚀 **2) Subir tudo para o GitHub (passo a passo)**

### 1. Confirme que está no branch correto

```bash
git branch -M main

2. Adicione tudo
git add README.md
git commit -m "Adiciona README.md do projeto"

3. Crie o repositório no GitHub

Vá para:

👉 https://github.com/new

Repository name: LGBT-Minas-Pipeline

Description:
Pipeline de coleta e análise de discurso de ódio LGBT+ em redes sociais com filtros geográficos para Minas Gerais

Public (recomendado)

NÃO crie README pelo GitHub (você já tem um local)

Clique em Create Repository

4. Adicionar o remoto

(Use sua URL real do GitHub)

git remote add origin https://github.com/SEU_USUARIO/LGBT-Minas-Pipeline.git

5. Subir pro GitHub
git push -u origin main
