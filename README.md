# 🏳️‍🌈 LGBT+ Minas — Hate Speech Detection in Social Media

This project implements a complete data pipeline for the **collection, filtering, processing, and analysis** of social media content, aiming to identify and characterize **hate speech targeting the LGBT+ community** in the context of **Minas Gerais (Brazil)**.

The research integrates multiple data sources and processing steps, including:

- 🌐 Public social media platforms (Redit)
- 📍 Geographic filtering based on cities in Minas Gerais
- 🏳️‍🌈 LGBT-related terminology detection
- ⚠️ Hate speech keyword filtering
- 🧠 Automatic language identification (Portuguese)
- 💾 Processing of large-scale Reddit dumps (`.zst`, tens of GB)

This project is part of a Master's dissertation in Computer Science.

---

## ✨ Objectives

- Collect and process large-scale textual data from social media.
- Identify LGBT-related content associated with hate speech.
- Restrict analysis to geographically relevant content (Minas Gerais).
- Build a structured dataset for linguistic and NLP-based analysis.
- Provide a reproducible and well-documented data pipeline.

---

## 📂 Project Structure
LGBT-Minas-Pipeline/
│
├── data/ # Data (not versioned)
│ ├── social_media/
│ │ ├── raw/ # Raw dumps (.zst)
│ │ ├── processed/ # Filtered CSV files
│ │ └── analysis/
│ │ ├── vader/ # Sentiment analysis outputs
│ │ └── tybyria/ # Hate speech detection outputs
│ │
│ └── criminal_data/ # Official crime datasets
│
├── configs/ # Filters and parameters
│ ├── filtros/
│ │ ├── cidades_mg.txt
│ │ ├── termos_lgbt.txt
│ │ └── termos_odio.txt
│ └── global.json
│
├── src/
│ ├── reddit/
│ │ ├── process_dump.py # Reddit processing pipeline
│ │ ├── filters.py # MG + LGBT + hate filters
│ │ └── config.py
│ │
│ ├── mastodon/ # Mastodon data collection scripts
│ │
│ └── utils/
│ ├── lang/ # Language detection utilities
│ ├── logger.py
│ └── load_config.py
│
├── logs/ # Processing logs
└── README.md


---

## ⚙️ Requirements

Install dependencies using:
pip install -r requirements.txt

Main libraries:
- pandas
- zstandard
- requests
- beautifulsoup4
- langdetect

## 🧵 Running the Reddit Pipeline
1. Place .zst files in: data/social_media/raw/
2. Run: python -m src.reddit.process_dump

The pipeline performs:
Streaming decompression of .zst files
Language filtering (Portuguese)
Detection of LGBT terms, hate speech, and MG locations
Incremental CSV generation
Detailed logging

## 📊 Data and Code Availability

The datasets generated and analyzed during this study are available at:

The source code is publicly available at:
👉 https://github.com/thaynaraCardoso1/LGBT-Minas_Pipeline


## 🚫 Data Versioning Policy

Large datasets (raw dumps, processed CSVs, and model outputs) are not versioned in this repository.

Please refer to the .gitignore file and external storage links for data access.

## 📜 License

This project is intended for academic and educational use.

## ✍️ Author

Thaynara Alexandre Cardoso
M.Sc. Candidate in Computer Science – UNIRIO
Application Architect • NLP Researcher
