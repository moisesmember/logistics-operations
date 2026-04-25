# Logistics Operations Lakehouse Bootstrap

Estrutura base para projetos de aprendizado de máquina e análise de dados com foco em arquitetura limpa, código reutilizável e ingestão idempotente de dados do Kaggle para o MinIO.

O projeto foi preparado para trabalhar com o dataset `yogape/logistics-operations-database`:

- Kaggle: https://www.kaggle.com/datasets/yogape/logistics-operations-database
- Fonte oficial da biblioteca `kagglehub`: https://github.com/Kaggle/kagglehub

## Objetivos

- Baixar o dataset do Kaggle usando `kagglehub`.
- Organizar os arquivos brutos em um bucket do MinIO.
- Não reenviar objetos que já existirem no bucket.
- Disponibilizar uma camada reutilizável para leitura dos arquivos em notebooks Jupyter e em outros códigos Python.
- Manter a solução organizada em camadas inspiradas em Clean Architecture e princípios SOLID.

## Estrutura do projeto

```text
.
|-- .env.example
|-- docker-compose.yml
|-- pyproject.toml
|-- requirements.txt
|-- notebooks/
|-- scripts/
|   `-- ingest_dataset.py
`-- src/
    `-- logistics_ops/
        |-- __init__.py
        |-- bootstrap.py
        |-- application/
        |   |-- dto/
        |   `-- use_cases/
        |-- domain/
        |   |-- entities/
        |   `-- ports/
        `-- infrastructure/
            |-- config/
            |-- readers/
            |-- sources/
            `-- storage/
```

## Arquitetura

### `domain`

Contém contratos e entidades centrais do negócio:

- `DatasetAsset`: representa um arquivo do dataset.
- `DatasetSource`: abstrai qualquer fonte externa de dados.
- `ObjectStorage`: abstrai qualquer armazenamento compatível com objetos.

### `application`

Contém os casos de uso:

- `SyncDatasetToObjectStorage`: coordena download, descoberta de arquivos e envio idempotente para o MinIO.

### `infrastructure`

Implementa detalhes concretos:

- `KaggleHubDatasetSource`: usa `kagglehub` para baixar o dataset.
- `MinioObjectStorage`: encapsula operações no MinIO.
- `MinioTabularReader`: lê CSV e texto diretamente do MinIO para uso em notebooks e scripts.

## Estrutura de dados no MinIO

Os arquivos serão enviados para o bucket configurado em `MINIO_BUCKET`, usando o prefixo:

```text
raw/kaggle/yogape/logistics-operations-database/
```

Exemplo:

```text
raw/kaggle/yogape/logistics-operations-database/drivers.csv
raw/kaggle/yogape/logistics-operations-database/trips.csv
raw/kaggle/yogape/logistics-operations-database/DATABASE_SCHEMA.txt
```

Essa convenção facilita:

- reutilização em notebooks;
- separação entre origem e camada `raw`;
- futura expansão para `silver/` e `gold/`.

## Pré-requisitos

- Docker Desktop
- Python 3.11+ (o ambiente local foi preparado para Python 3.14)
- Credenciais de API do Kaggle

## Subindo o MinIO com Docker Compose

O arquivo [`docker-compose.yml`](./docker-compose.yml) sobe:

- MinIO API em `http://localhost:9000`
- MinIO Console em `http://localhost:9001`

Comando:

```powershell
docker compose up -d
```

Credenciais padrão definidas no `.env.example`:

- usuário: `minioadmin`
- senha: `minioadmin123`

## Configuração de ambiente

1. Copie o arquivo de exemplo:

```powershell
Copy-Item .env.example .env
```

2. Edite o `.env` com seus valores.

Exemplo de variáveis:

```env
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin123
MINIO_SECURE=false
MINIO_BUCKET=logistics-lake
MINIO_DATASET_PREFIX=raw/kaggle/yogape/logistics-operations-database

KAGGLE_DATASET_HANDLE=yogape/logistics-operations-database
KAGGLEHUB_CACHE=.cache/kagglehub
```

## Autenticação no Kaggle

Segundo a documentação oficial do `kagglehub`, fora do ambiente do Kaggle você precisa autenticar para acessar datasets que exigem consentimento ou credenciais.

As opções mais práticas são:

1. Gerar o token em `https://www.kaggle.com/settings`.
2. Salvar o arquivo `kaggle.json` em `C:\Users\SEU_USUARIO\.kaggle\kaggle.json`.

Alternativamente, você pode autenticar por código:

```python
import kagglehub

kagglehub.login()
```

## Criação da virtualenv

No Windows PowerShell:

```powershell
C:\Users\User\AppData\Local\Programs\Python\Python314\python.exe -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
pip install -e .
```

Se quiser registrar a `venv` como kernel do Jupyter:

```powershell
python -m ipykernel install --user --name logistics-operations --display-name "Python (logistics-operations)"
```

## Dependências

O arquivo [`requirements.txt`](./requirements.txt) inclui o necessário para:

- ingestão com `kagglehub`;
- acesso ao MinIO;
- leitura tabular com `pandas`;
- uso em notebooks com `jupyterlab` e `ipykernel`.

## Como executar a ingestão Kaggle -> MinIO

Com o MinIO ativo, `.env` configurado e a `venv` ativada:

```powershell
python scripts/ingest_dataset.py
```

O fluxo executado é:

1. baixa o dataset do Kaggle para o cache local;
2. identifica os arquivos do dataset;
3. cria o bucket no MinIO se ele não existir;
4. verifica objeto por objeto;
5. envia apenas os arquivos ausentes;
6. retorna um resumo com enviados, ignorados e total.

## Estrutura esperada do dataset

De acordo com a página do dataset no Kaggle, os arquivos publicados incluem:

- `drivers.csv`
- `trucks.csv`
- `trailers.csv`
- `customers.csv`
- `facilities.csv`
- `routes.csv`
- `loads.csv`
- `trips.csv`
- `fuel_purchases.csv`
- `maintenance_records.csv`
- `delivery_events.csv`
- `safety_incidents.csv`
- `driver_monthly_metrics.csv`
- `truck_utilization_metrics.csv`
- `DATABASE_SCHEMA.txt`

## Uso em notebooks Jupyter

Depois de instalar o projeto com `pip install -e .`, você pode importar o leitor reutilizável em qualquer notebook:

```python
from logistics_ops.bootstrap import build_tabular_reader

reader = build_tabular_reader()

drivers = reader.read_csv_from_dataset("drivers.csv")
trips = reader.read_csv_from_dataset("trips.csv")
schema_text = reader.read_text_from_dataset("DATABASE_SCHEMA.txt")

print(drivers.head())
print(trips.head())
print(schema_text[:500])
```

Também é possível listar os arquivos disponíveis:

```python
from logistics_ops.bootstrap import build_tabular_reader

reader = build_tabular_reader()
print(reader.list_dataset_objects())
```

## Exemplo usando diretamente a ideia sugerida pelo Kaggle

O Kaggle sugere o uso de `load_dataset` para carregar um arquivo específico em um `DataFrame`. Exemplo:

```python
import kagglehub
from kagglehub import KaggleDatasetAdapter

df = kagglehub.dataset_load(
    KaggleDatasetAdapter.PANDAS,
    "yogape/logistics-operations-database",
    "drivers.csv",
)
```

Neste projeto, a abordagem principal para ingestão é diferente:

- usamos `kagglehub.dataset_download(...)` para baixar o conjunto completo;
- preservamos os arquivos brutos no MinIO;
- centralizamos a leitura posterior via `MinioTabularReader`.

Essa abordagem é melhor para reuso entre notebooks, pipelines e outros serviços.

## Exemplo de notebook ponta a ponta

```python
from logistics_ops.bootstrap import build_sync_use_case, build_tabular_reader

# Garante que o bucket tenha os arquivos do dataset
sync = build_sync_use_case()
result = sync.execute()
print(result)

# Lê os dados do MinIO
reader = build_tabular_reader()
loads = reader.read_csv_from_dataset("loads.csv")
print(loads.head())
```

## Desenvolvimento

Validação rápida da base:

```powershell
python -m compileall src scripts
```

## Observações importantes

- O projeto não versiona `.env` nem `.venv`.
- O envio para o MinIO é idempotente: se o objeto já existe, ele não é reenviado.
- O bucket é criado automaticamente quando necessário.
- A camada de leitura foi pensada para reuso em um ou muitos notebooks.

## Referências

- Kaggle dataset: https://www.kaggle.com/datasets/yogape/logistics-operations-database
- KaggleHub README: https://github.com/Kaggle/kagglehub/blob/main/README.md
