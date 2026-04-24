# Projeto Final - Banco de Dados Relacional de Vendas

## Ferramentas:
- PostgreSQL
- DBeaver

## 📋 Contexto do Negócio

A diretoria de uma empresa precisava transformar um arquivo CSV de vendas em um banco de dados relacional estruturado, capaz de responder a perguntas estratégicas sobre clientes, produtos, filiais e regiões.

O projeto cobre o ciclo completo de engenharia de dados:

> **Ingestão de dados brutos (staging) → Modelagem relacional → Carga nas tabelas normalizadas → Queries analíticas → View consolidada**

## 🗂️ Estrutura do Dataset (CSV)

O arquivo de entrada `dataset_PA.csv` contém as seguintes colunas:

| Campo | Tipo | Descrição |
|---|---|---|
| `nr_pedido` | INT | Número do pedido |
| `dt_momento` | TIMESTAMP | Data e hora do registro |
| `codigo_filial` | INT | Código da filial responsável |
| `nome_filial` | VARCHAR | Nome da filial |
| `codigo_cliente` | INT | Código do cliente |
| `nome_cliente` | VARCHAR | Nome do cliente |
| `uf` | CHAR(2) | Estado de origem do cliente |
| `codigo_produto` | INT | Código do produto |
| `descricao_produto` | VARCHAR | Descrição comercial |
| `marca` | VARCHAR | Marca do produto |
| `preco_unitario` | DECIMAL | Preço unitário |
| `quantidade` | INT | Quantidade vendida |
| `avaliacao` | INT | Avaliação de 1 a 5 |

## 🏗️ Modelagem Relacional

O banco foi normalizado em **4 tabelas**, saindo de uma estrutura flat (staging) para o modelo relacional abaixo:

```
tbl_clientes (codigo_cliente PK, nome_cliente, uf)
       ↑
tbl_pedidos (nr_pedido + codigo_produto PK composta,
             dt_momento, codigo_filial FK, codigo_cliente FK, codigo_produto FK,
             quantidade, avaliacao)
       ↓                    ↓
tbl_filiais            tbl_produtos
(codigo_filial PK,     (codigo_produto PK,
 nome)                  nome_produto, marca, preco_unitario)
```

## Passo a Passo da Implementação
### ✅ Passo 1 — Criação da tabela de staging

A tabela `staging_vendas` recebe os dados brutos do CSV sem qualquer transformação. Ela serve como zona de aterrizagem para posterior normalização.

```sql
CREATE TABLE staging_vendas (
    nr_pedido INT,
    dt_momento TIMESTAMP,
    codigo_filial INT,
    nome_filial VARCHAR(255),
    codigo_cliente INT,
    nome_cliente VARCHAR(255),
    uf CHAR(2),
    codigo_produto INT,
    descricao_produto VARCHAR(255),
    marca VARCHAR(100),
    preco_unitario DECIMAL(10,2),
    quantidade INT,
    avaliacao INT
);
```
**Estrutura criada no DBeaver:**

![Estrutura staging_vendas](img/DB1.png)

### ✅ Passo 2 — Importação dos dados com COPY

```sql
COPY staging_vendas FROM 'C:/dados/dataset_PA.csv'
WITH (FORMAT CSV, HEADER, DELIMITER ',', ENCODING 'UTF8');

SELECT * FROM staging_vendas LIMIT 10;
```

📸 **Preview dos primeiros 10 registros importados:**

![Preview dos dados](img/DB2.png)

> A tabela staging recebeu **18.710 registros** provenientes do CSV.

### ✅ Passo 3 — Análise dos tamanhos dos campos (LENGTH)

Antes de criar as tabelas dimensionais, foi executada uma consulta para identificar o tamanho máximo de cada campo de texto, garantindo que os `VARCHAR` fossem definidos com precisão.

```sql
SELECT
    MAX(LENGTH(nome_filial))  AS tam_filial,
    MAX(LENGTH(nome_cliente)) AS tam_cliente,
    MAX(LENGTH(uf))           AS tam_uf,
    MAX(LENGTH(descricao_produto)) AS tam_produto,
    MAX(LENGTH(marca))        AS tam_marca
FROM staging_vendas;
```

📸 **Tabelas criadas após análise de tamanhos:**

![Tabelas criadas](img/DB3.png)

### ✅ Passo 4 e 5 — Criação das tabelas com constraints

As tabelas foram criadas conforme o diagrama de modelagem, com as regras de negócio aplicadas diretamente como constraints:

- `tbl_pedidos.quantidade` → aceita **somente valores positivos** (`CHECK > 0`)
- `tbl_produtos.preco_unitario` → aceita **somente valores > 0** e **NOT NULL**
- `tbl_pedidos.avaliacao` → aceita **somente valores entre 1 e 5**

```sql
-- Tabela de Clientes
CREATE TABLE tbl_clientes (
    codigo_cliente INT PRIMARY KEY,
    nome_cliente VARCHAR(255),
    uf CHAR(2)
);

-- Tabela de Filiais
CREATE TABLE tbl_filiais (
    codigo_filial INT PRIMARY KEY,
    nome VARCHAR(255)
);

-- Tabela de Produtos (com constraint de preço)
CREATE TABLE tbl_produtos (
    codigo_produto INT PRIMARY KEY,
    nome_produto VARCHAR(255),
    marca VARCHAR(100),
    preco_unitario DECIMAL(10,2) NOT NULL CHECK (preco_unitario > 0)
);

-- Tabela de Pedidos (tabela fato com chave composta e constraints)
CREATE TABLE tbl_pedidos (
    nr_pedido INT,
    dt_momento TIMESTAMP,
    codigo_filial INT REFERENCES tbl_filiais(codigo_filial),
    codigo_cliente INT REFERENCES tbl_clientes(codigo_cliente),
    codigo_produto INT REFERENCES tbl_produtos(codigo_produto),
    quantidade INT CHECK (quantidade > 0),
    avaliacao INT CHECK (avaliacao BETWEEN 1 AND 5),
    PRIMARY KEY (nr_pedido, codigo_produto)
);
```
**Constraints aplicadas na tbl_pedidos:**

![Constraints na tbl_pedidos](img/DB4-5.png)

---

### ✅ Passo 6 — Carga de dados nas tabelas normalizadas

Os dados foram persistidos nas tabelas a partir da staging, usando `SELECT DISTINCT` para garantir unicidade nas dimensões.

```sql
-- Popular Clientes
INSERT INTO tbl_clientes (codigo_cliente, nome_cliente, uf)
SELECT DISTINCT codigo_cliente, nome_cliente, uf FROM staging_vendas;

-- Popular Filiais
INSERT INTO tbl_filiais (codigo_filial, nome)
SELECT DISTINCT codigo_filial, nome_filial FROM staging_vendas;

-- Popular Produtos
INSERT INTO tbl_produtos (codigo_produto, nome_produto, marca, preco_unitario)
SELECT DISTINCT codigo_produto, descricao_produto, marca, preco_unitario FROM staging_vendas;

-- Popular Pedidos
INSERT INTO tbl_pedidos (nr_pedido, dt_momento, codigo_filial, codigo_cliente, codigo_produto, quantidade, avaliacao)
SELECT nr_pedido, dt_momento, codigo_filial, codigo_cliente, codigo_produto, quantidade, avaliacao
FROM staging_vendas;
```

📸 **Contagem de registros por tabela após carga:**

![Contagem de registros](screenshots/DB11_1.png)

| Tabela | Registros |
|---|---|
| tbl_clientes | 100 |
| tbl_filiais | 10 |
| tbl_produtos | 19 |
| tbl_pedidos | 18.710 |

---

### ✅ Passo 7 — Integridade referencial (Chaves Estrangeiras)

Todas as tabelas foram relacionadas via Foreign Keys, garantindo integridade referencial. A `tbl_pedidos` referencia todas as dimensões:

📸 **Chaves estrangeiras configuradas:**

![Chaves estrangeiras](screenshots/DB6.png)

```
tbl_pedidos_codigo_cliente_fkey → tbl_clientes
tbl_pedidos_codigo_filial_fkey  → tbl_filiais
tbl_pedidos_codigo_produto_fkey → tbl_produtos
```

---

### ✅ Passo 8 — Top 5 clientes por volume de compras

```sql
SELECT
    c.nome_cliente,
    SUM(p.quantidade) AS total_volumes
FROM tbl_pedidos p
JOIN tbl_clientes c ON p.codigo_cliente = c.codigo_cliente
GROUP BY c.nome_cliente
ORDER BY total_volumes DESC
LIMIT 5;
```

📸 **Resultado:**

![Top 5 clientes](screenshots/DB7.png)

| # | Cliente | Total Volumes |
|---|---|---|
| 1 | Cliente 41 | 993 |
| 2 | Cliente 39 | 976 |
| 3 | Cliente 95 | 973 |
| 4 | Cliente 24 | 946 |
| 5 | Cliente 31 | 941 |

---

### ✅ Passo 9 — Top 3 produtos por volume de compras

```sql
SELECT
    pr.nome_produto,
    SUM(p.quantidade) AS total_volumes
FROM tbl_pedidos p
JOIN tbl_produtos pr ON p.codigo_produto = pr.codigo_produto
GROUP BY pr.nome_produto
ORDER BY total_volumes DESC
LIMIT 3;
```

📸 **Resultado:**

![Top 3 produtos](screenshots/DB8.png)

| # | Produto | Total Volumes |
|---|---|---|
| 1 | Produto 11 | 4.686 |
| 2 | Produto 9 | 4.645 |
| 3 | Produto 18 | 4.603 |

---

### ✅ Passo 10 — Produto mais vendido por marca (Jul–Dez 2023)

Utilizou-se `DISTINCT ON` do PostgreSQL para retornar apenas o produto mais vendido de cada marca no período solicitado, sem necessidade de subquery.

```sql
SELECT DISTINCT ON (pr.marca)
    pr.marca,
    pr.nome_produto,
    SUM(p.quantidade) AS total_vendido
FROM tbl_pedidos p
JOIN tbl_produtos pr ON p.codigo_produto = pr.codigo_produto
WHERE p.dt_momento BETWEEN '2023-07-01' AND '2023-12-31'
GROUP BY pr.marca, pr.nome_produto
ORDER BY pr.marca, total_vendido DESC;
```

📸 **Resultado:**

![Produto mais vendido por marca](screenshots/DB9.png)

| Marca | Produto Mais Vendido | Total Vendido |
|---|---|---|
| Marca 1 | Produto 7 | 556 |
| Marca 2 | Produto 9 | 548 |
| Marca 3 | Produto 14 | 481 |

---

### ✅ Passo 11 — VIEW Pivot: Vendas por Filial e Região

Foi criada uma VIEW com técnica de **pivot manual** usando `CASE WHEN`, agrupando os estados brasileiros nas 5 macrorregiões do IBGE.

```sql
CREATE OR REPLACE VIEW vendas_por_filial_regiao AS
SELECT
    f.nome AS "Filial",
    ROUND(SUM(CASE WHEN c.uf IN ('AM','RR','AP','PA','TO','RO','AC')
              THEN (p.quantidade * pr.preco_unitario) ELSE 0 END)::numeric, 2) AS "Norte",
    ROUND(SUM(CASE WHEN c.uf IN ('MA','PI','CE','RN','PE','PB','SE','AL','BA')
              THEN (p.quantidade * pr.preco_unitario) ELSE 0 END)::numeric, 2) AS "Nordeste",
    ROUND(SUM(CASE WHEN c.uf IN ('MT','MS','GO','DF')
              THEN (p.quantidade * pr.preco_unitario) ELSE 0 END)::numeric, 2) AS "Centro-Oeste",
    ROUND(SUM(CASE WHEN c.uf IN ('SP','RJ','ES','MG')
              THEN (p.quantidade * pr.preco_unitario) ELSE 0 END)::numeric, 2) AS "Sudeste",
    ROUND(SUM(CASE WHEN c.uf IN ('PR','SC','RS')
              THEN (p.quantidade * pr.preco_unitario) ELSE 0 END)::numeric, 2) AS "Sul",
    ROUND(SUM(p.quantidade * pr.preco_unitario)::numeric, 2) AS "Total de vendas"
FROM tbl_pedidos p
JOIN tbl_filiais f ON p.codigo_filial = f.codigo_filial
JOIN tbl_clientes c ON p.codigo_cliente = c.codigo_cliente
JOIN tbl_produtos pr ON p.codigo_produto = pr.codigo_produto
GROUP BY f.nome
ORDER BY f.nome;
```

📸 **View criada no banco:**

![View no banco](screenshots/DB10.png)

📸 **Resultado da view:**

![Resultado da view pivot](screenshots/DB11.png)

> Cada filial apresenta vendas distribuídas pelas 5 regiões do Brasil, com o total consolidado na última coluna.

---

### ✅ Passo 12 — Acumulado de Vendas por Filial e UF (Window Function)

Consulta com **Window Function** (`SUM OVER PARTITION`) para calcular o acumulado de vendas dentro de cada filial, ordenado por UF — permitindo visualizar a contribuição progressiva de cada estado.

```sql
SELECT
    f.codigo_filial AS "Filial",
    f.nome          AS "nome",
    c.uf            AS "UF",
    ROUND(SUM(SUM(p.quantidade * pr.preco_unitario)) OVER (
        PARTITION BY f.codigo_filial
        ORDER BY c.uf
    )::numeric, 2) AS "Total de vendas"
FROM tbl_pedidos p
JOIN tbl_filiais f  ON p.codigo_filial  = f.codigo_filial
JOIN tbl_clientes c ON p.codigo_cliente = c.codigo_cliente
JOIN tbl_produtos pr ON p.codigo_produto = pr.codigo_produto
GROUP BY f.codigo_filial, f.nome, c.uf
ORDER BY f.codigo_filial, c.uf;
```

📸 **Resultado — Acumulado da Filial 1 por UF:**

![Acumulado por filial e UF](screenshots/DB12.png)

> O valor em "Total de vendas" é cumulativo: cada linha soma o valor da UF atual com todas as UFs anteriores dentro da mesma filial.

---

## 🧠 Técnicas e Conceitos Aplicados

| Conceito | Onde foi usado |
|---|---|
| `COPY` para ingestão em massa | Passo 2 |
| `MAX(LENGTH())` para sizing de campos | Passo 3 |
| Chave primária composta | `tbl_pedidos (nr_pedido, codigo_produto)` |
| Constraints (`CHECK`, `NOT NULL`) | Passos 4 e 5 |
| `SELECT DISTINCT` na carga | Passo 6 |
| Integridade referencial com FK | Passo 7 |
| `GROUP BY` + `ORDER BY` + `LIMIT` | Passos 8 e 9 |
| `DISTINCT ON` (PostgreSQL) | Passo 10 |
| `CREATE VIEW` + Pivot com `CASE WHEN` | Passo 11 |
| Window Function `SUM OVER PARTITION BY` | Passo 12 |

---

## ⚙️ Como Reproduzir

1. Clone o repositório
2. Tenha o PostgreSQL instalado e um banco criado
3. Coloque o arquivo `dataset_PA.csv` em `C:/dados/` (ou ajuste o caminho no Passo 2)
4. Execute o `script-banco-de-dados.sql` completo em ordem no DBeaver ou psql

```bash
psql -U postgres -d nome_do_banco -f script-banco-de-dados.sql
```
