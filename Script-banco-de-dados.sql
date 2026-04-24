
-- PASSO 1
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

-- PASSO 2
COPY staging_vendas FROM 'C:/dados/dataset_PA.csv' 
WITH (FORMAT CSV, HEADER, DELIMITER ',', ENCODING 'UTF8');

SELECT * FROM staging_vendas LIMIT 10;

-- drop table tbl_clientes 
-- drop table tbl_clientes_telefone 

-- PASSO 3 
SELECT 
    MAX(LENGTH(nome_filial)) AS tam_filial,
    MAX(LENGTH(nome_cliente)) AS tam_cliente,
    MAX(LENGTH(uf)) AS tam_uf,
    MAX(LENGTH(descricao_produto)) AS tam_produto,
    MAX(LENGTH(marca)) AS tam_marca
FROM staging_vendas;


-- 1. Tabela de Clientes
CREATE TABLE tbl_clientes (
    codigo_cliente INT PRIMARY KEY,
    nome_cliente VARCHAR(255),
    uf CHAR(2)
);

-- 2. Tabela de Filiais
CREATE TABLE tbl_filiais (
    codigo_filial INT PRIMARY KEY,
    nome VARCHAR(255)
);

-- PASSO 5 
-- 3. Tabela de Produtos
CREATE TABLE tbl_produtos (
    codigo_produto INT PRIMARY KEY,
    nome_produto VARCHAR(255),
    marca VARCHAR(100),
    preco_unitario DECIMAL(10,2) NOT NULL CHECK (preco_unitario > 0) -- Validação solicitada 
);

--PASSO 4 e PASSO 7
-- 4. Tabela de Pedidos (Fato)
CREATE TABLE tbl_pedidos (
    nr_pedido INT,
    dt_momento TIMESTAMP,
    codigo_filial INT REFERENCES tbl_filiais(codigo_filial),
    codigo_cliente INT REFERENCES tbl_clientes(codigo_cliente),
    codigo_produto INT REFERENCES tbl_produtos(codigo_produto),
    quantidade INT CHECK (quantidade > 0), -- Validação solicitada 
    avaliacao INT CHECK (avaliacao BETWEEN 1 AND 5),
    PRIMARY KEY (nr_pedido, codigo_produto) -- Chave composta se um pedido tiver vários itens
);

-- PASSO 6
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
SELECT nr_pedido, dt_momento, codigo_filial, codigo_cliente, codigo_produto, quantidade, avaliacao FROM staging_vendas;

-- CONSULTANDO TOTAL DOS REGISTROS
SELECT 'tbl_clientes'AS tabela, COUNT(*) AS total FROM tbl_clientes
UNION ALL
SELECT 'tbl_filiais', COUNT(*) FROM tbl_filiais
UNION ALL
SELECT 'tbl_produtos', COUNT(*) FROM tbl_produtos
UNION ALL
SELECT 'tbl_pedidos', COUNT(*) FROM tbl_pedidos;

-- Consultas de Análise (Queries)
-- PASSO 8. Localize o top 5 de cliente que mais compraram produtos (em quantidade - Volumes)
SELECT 
    c.nome_cliente, 
    SUM(p.quantidade) AS total_volumes
FROM tbl_pedidos p
JOIN tbl_clientes c ON p.codigo_cliente = c.codigo_cliente
GROUP BY c.nome_cliente
ORDER BY total_volumes DESC
LIMIT 5;

-- PASSO 9. Localize o top 3 de produtos que mais foram comprados (em quantidade - Volumes)
SELECT 
    pr.nome_produto, 
    SUM(p.quantidade) AS total_volumes
FROM tbl_pedidos p
JOIN tbl_produtos pr ON p.codigo_produto = pr.codigo_produto
GROUP BY pr.nome_produto
ORDER BY total_volumes DESC
LIMIT 3;

-- PASSO 10. Localize o produto mais vendido por marca no período de Julho/2023 até Dezembro/2023
-- Nota: Aqui usei uma técnica para pegar apenas o primeiro (mais vendido) de cada grupo de marca.
SELECT DISTINCT ON (pr.marca)
    pr.marca,
    pr.nome_produto,
    SUM(p.quantidade) AS total_vendido
FROM tbl_pedidos p
JOIN tbl_produtos pr ON p.codigo_produto = pr.codigo_produto
WHERE p.dt_momento BETWEEN '2023-07-01' AND '2023-12-31'
GROUP BY pr.marca, pr.nome_produto
ORDER BY pr.marca, total_vendido DESC;

DROP VIEW IF EXISTS view_vendas_consolidado;

-- PASSO 11 . Crie uma VIEW pivot de vendas e região por filial, totalizando todos os pedidos
CREATE OR REPLACE VIEW vendas_por_filial_regiao AS
SELECT 
    f.nome AS "Filial",
    ROUND(SUM(CASE WHEN c.uf IN ('AM', 'RR', 'AP', 'PA', 'TO', 'RO', 'AC') THEN (p.quantidade * pr.preco_unitario) ELSE 0 END)::numeric, 2) AS "Norte",
    ROUND(SUM(CASE WHEN c.uf IN ('MA', 'PI', 'CE', 'RN', 'PE', 'PB', 'SE', 'AL', 'BA') THEN (p.quantidade * pr.preco_unitario) ELSE 0 END)::numeric, 2) AS "Nordeste",
    ROUND(SUM(CASE WHEN c.uf IN ('MT', 'MS', 'GO', 'DF') THEN (p.quantidade * pr.preco_unitario) ELSE 0 END)::numeric, 2) AS "Centro-Oeste",
    ROUND(SUM(CASE WHEN c.uf IN ('SP', 'RJ', 'ES', 'MG') THEN (p.quantidade * pr.preco_unitario) ELSE 0 END)::numeric, 2) AS "Sudeste",
    ROUND(SUM(CASE WHEN c.uf IN ('PR', 'SC', 'RS') THEN (p.quantidade * pr.preco_unitario) ELSE 0 END)::numeric, 2) AS "Sul",
    ROUND(SUM(p.quantidade * pr.preco_unitario)::numeric, 2) AS "Total de vendas"
FROM tbl_pedidos p
JOIN tbl_filiais f ON p.codigo_filial = f.codigo_filial
JOIN tbl_clientes c ON p.codigo_cliente = c.codigo_cliente
JOIN tbl_produtos pr ON p.codigo_produto = pr.codigo_produto
GROUP BY f.nome
ORDER BY f.nome;

-- Para testar a View:
SELECT * FROM vendas_por_filial_regiao;

-- PASSO 12: Consulta de Acumulado Corrigida (Agrupada por Filial e UF)
SELECT 
    f.codigo_filial AS "Filial",
    f.nome AS "nome",
    c.uf AS "UF",
    ROUND(SUM(SUM(p.quantidade * pr.preco_unitario)) OVER (
        PARTITION BY f.codigo_filial 
        ORDER BY c.uf
    )::numeric, 2) AS "Total de vendas"
FROM tbl_pedidos p
JOIN tbl_filiais f ON p.codigo_filial = f.codigo_filial
JOIN tbl_clientes c ON p.codigo_cliente = c.codigo_cliente
JOIN tbl_produtos pr ON p.codigo_produto = pr.codigo_produto
GROUP BY f.codigo_filial, f.nome, c.uf
ORDER BY f.codigo_filial, c.uf;







