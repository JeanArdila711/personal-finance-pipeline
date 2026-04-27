# Personal Finance Pipeline

ETL pipeline pa' mis finanzas personales. Extrae data de [Wallet by BudgetBakers](https://budgetbakers.com/) vía REST API, la transforma, la carga en un data warehouse, y genera analytics + alertas.

## 🏗️ Arquitectura

\```
Wallet API → Extract → Transform → Load (Postgres/SQLite) → Analytics (Telegram, Claude AI, Dashboards)
\```

## 🛠️ Stack

- **Lenguaje:** Python 3.13
- **Orquestación:** GitHub Actions
- **Data Warehouse:** TBD (Postgres en Supabase o SQLite)
- **Transformaciones:** dbt-core
- **Notificaciones:** Telegram Bot
- **AI Insights:** Anthropic Claude API

## 🚀 Setup local

\```bash
# Clonar
git clone https://github.com/JeanArdila711/personal-finance-pipeline.git
cd personal-finance-pipeline

# Virtualenv
python -m venv .venv
.\.venv\Scripts\Activate.ps1   # Windows
# source .venv/bin/activate    # Linux/Mac

# Dependencias
pip install -r requirements.txt

# Configurar variables
cp .env.example .env
# Editá .env con tus credenciales reales
\```

## 🧪 Tests

\```bash
pytest
\```

## 📜 Licencia

MIT — uso personal.