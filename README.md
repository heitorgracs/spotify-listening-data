# spotify-data-analysis

# 🎧 Spotify Listening Data — Personal Analytics Dashboard

Análise completa do histórico de escuta pessoal do Spotify, com pipeline ETL orquestrado pelo **Prefect** e visualização interativa em **Power BI**. O projeto consome os dados brutos exportados diretamente pelo Spotify, enriquece as informações via **Spotify Web API** e entrega um dashboard com tendências reais de consumo musical.

---

## 📌 Visão Geral

O projeto parte dos arquivos JSON do histórico de streaming exportados pelo Spotify (disponíveis via *Privacy Settings* da conta) e os transforma em um dataset limpo e enriquecido, pronto para análise. O pipeline é composto por duas etapas principais orquestradas pelo Prefect:

1. **Transform** — leitura, limpeza e padronização dos dados brutos
2. **Data Enrichment** — enriquecimento via Spotify API com gênero musical, imagens de artistas e capas de álbuns

O resultado é consumido diretamente pelo Power BI para geração do dashboard.

---

## 🗂️ Estrutura do Projeto

```
spotify-listening-data/
├── data/
│   ├── raw/                  # JSONs exportados do Spotify          
│   ├── clean/                # Parquet files processados
├── pbi_backgrounds/          # Assets visuais do Power BI
├── scripts/
│   ├── constants/
│   │   └── spotipy_config.py # Configuração do cliente Spotify API
│   ├── utils/
│   │   └── aux_funcs.py      # Funções auxiliares de transformação
│   ├── transform.py          # Step 1: ETL e limpeza dos dados brutos
│   └── data_enrichment.py    # Step 2: Enriquecimento via Spotify API
├── main.py                   # Entrypoint do pipeline Prefect
├── requirements.txt
└── .env                      # Credenciais da Spotify API (não versionado)
```

---

## ⚙️ Pipeline ETL (Prefect)

O pipeline é orquestrado pelo Prefect e executado via `main.py`. As tasks são executadas sequencialmente:

```
etl_pipeline
├── transform_task       → Lê os JSONs brutos, limpa e salva em Parquet
└── data_enrichment_task → Enriquece com Spotify API e exporta dataset final
```

### Transform Task (`transform.py`)
- Lê e concatena todos os arquivos `.json` do diretório `data/raw`
- Renomeia colunas para nomenclatura padronizada
- Remove colunas irrelevantes (`spotify_episode_uri`, `ip_addr`, `reason_start`)
- Aplica conversão de tipos: `datetime`, `boolean`, `int`, `float`
- Separa o campo `ts` em colunas `date` e `time`
- Remove registros com `track_name`, `album_name` ou `album_artist_name` nulos
- Gera dois arquivos de saída:
  - `streaming_data.parquet` — histórico completo de streaming
  - `aux_tracksdata.parquet` — tabela auxiliar com URIs únicas para enriquecimento

### Data Enrichment Task (`data_enrichment.py`)
- Autentica na **Spotify Web API** via `Client Credentials Flow`
- Busca metadados em lote (batch) para tracks, artists e albums:
  - Tracks → `spotify_track_id`, `spotify_artist_id`, `spotify_album_id`, `album_cover_image`
  - Artists → `main_genre`, `artist_image`
  - Albums → `album_genre` (fallback quando o gênero do artista não está disponível)
- Aplica fallback de gênero: se o artista não possui gênero cadastrado, utiliza o gênero do álbum
- Exporta `tracksdata.parquet` — dataset final enriquecido, compatível com Power BI

---

## 📊 Dashboard Power BI

O dashboard consome os arquivos Parquet gerados pelo pipeline e apresenta análises do histórico pessoal de escuta, incluindo:

- **Top artistas e músicas** mais ouvidos no período
- **Distribuição por gênero musical** (enriquecido via Spotify API)
- **Padrões temporais** — escuta por hora do dia, dia da semana e período histórico
- **Tendências de consumo** — evolução dos hábitos musicais ao longo do tempo
- **Plataformas utilizadas** — Android, iOS, Desktop, Web Player e outras (padronizadas no ETL)
- **Comportamento de skips e shuffle** — análise de engajamento por faixa
- **Capas de álbuns e imagens de artistas** integradas ao visual (via URLs enriquecidas pela API)

---

## 🔐 Configuração da Spotify API

Crie um app em [developer.spotify.com](https://developer.spotify.com/dashboard) e adicione as credenciais em um arquivo `.env` na raiz do projeto:

```env
CLIENT_ID=seu_client_id
CLIENT_SECRET=seu_client_secret
```

---

## 🐍 Configuração do Ambiente Virtual

```bash
# Criar o ambiente virtual
python -m venv .venv

# Ativar o ambiente (Linux/macOS)
source .venv/bin/activate

# Ativar o ambiente (Windows)
.venv\Scripts\activate

# Instalar dependências
pip install -r requirements.txt
```

---

## 🌀 Configuração do Prefect

O projeto utiliza o servidor local do Prefect para orquestração do pipeline.

```bash
# Iniciar o servidor Prefect localmente
prefect server start
```

Após o servidor estar ativo, execute o pipeline em outro terminal (com o `.venv` ativado):

```bash
python main.py
```

A UI do Prefect estará disponível em `http://localhost:4200`, onde é possível acompanhar as execuções das tasks e o status do flow em tempo real.

---

## 📦 Dependências Principais

| Biblioteca   | Uso                                              |
|--------------|--------------------------------------------------|
| `prefect`    | Orquestração do pipeline ETL                     |
| `spotipy`    | Integração com a Spotify Web API                 |
| `pandas`     | Manipulação e transformação dos dados            |
| `pyarrow`    | Leitura e escrita de arquivos Parquet            |
| `tqdm`       | Barra de progresso nas requisições em batch      |

---

## 📁 Como Obter Seus Dados do Spotify

1. Acesse [spotify.com/account/privacy](https://www.spotify.com/account/privacy/)
2. Role até **"Download your data"** e solicite o **Extended Streaming History**
3. O Spotify enviará os arquivos por e-mail em até 30 dias
4. Extraia os arquivos `.json` para `data/raw`
5. Execute o pipeline com `python main.py`

---

## 🛠️ Tecnologias Utilizadas

- **Python 3.11+**
- **Prefect** — orquestração de pipeline
- **Spotipy** — Spotify Web API client
- **Pandas** — ETL e transformação de dados
- **Power BI** — visualização e dashboard
- **Parquet** — formato de armazenamento intermediário

---

> Projeto desenvolvido para análise pessoal de hábitos musicais com dados reais do Spotify.