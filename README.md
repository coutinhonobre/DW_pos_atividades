# Data Warehouse Organizacional e Data Marts

Atividade da disciplina Tópicos Avançados em Inteligência Artificial I (Especialização em Inteligência Artificial Aplicada, IFG): integração das bases Northwind e Mercearia em um Data Warehouse Organizacional, com três Data Marts derivados dele (Vendas, Compras e Logística), carga via Airflow e análise no Metabase.

## Estrutura

- `application/` — implementação: DDL do DW e dos Data Marts, DAGs de ETL, ambiente Docker e provisionamento do BI. Ver o [README](application/README.md) dessa pasta para detalhes e como subir o ambiente.
- `modelo-ifg.tex`, `tex/`, `apendices/`, `bib/`, `pre/`, `formatacao/`, `fig/` — relatório técnico da atividade, em LaTeX (classe [classe-ifg](https://github.com/raphaeldeaquino/classe-ifg) do IFG).
- `screenshoots/` — capturas de tela dos diagramas e painéis usadas no relatório.

## Compilando o relatório

Requer uma distribuição LaTeX (TeX Live) com `-shell-escape` habilitado:

```bash
pdflatex -shell-escape modelo-ifg.tex
bibtex modelo-ifg
pdflatex -shell-escape modelo-ifg.tex
pdflatex -shell-escape modelo-ifg.tex
```

O PDF gerado é `modelo-ifg.pdf`.
