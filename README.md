# Data Warehouse Organizacional e Data Marts

Atividade da disciplina Tópicos Avançados em Inteligência Artificial I (Especialização em Inteligência Artificial Aplicada, IFG): integração das bases Northwind e Mercearia em um Data Warehouse Organizacional, com três Data Marts derivados dele (Vendas, Compras e Logística), carga via Airflow e análise no Metabase.

## Estrutura

- `application/` — implementação: DDL do DW e dos Data Marts, DAGs de ETL, ambiente Docker e provisionamento do BI. Ver o [README](application/README.md) dessa pasta para detalhes e como subir o ambiente.
- `relatorio/` — relatório técnico da atividade, em LaTeX (classe [classe-ifg](https://github.com/raphaeldeaquino/classe-ifg) do IFG): `modelo-ifg.tex`, `tex/`, `apendices/`, `anexos/`, `bib/`, `pre/`, `formatacao/` e `fig/`, além das capturas de tela usadas no relatório (`screenshoots/`).

## Compilando o relatório

Requer uma distribuição LaTeX (TeX Live) com `-shell-escape` habilitado:

```bash
cd relatorio
pdflatex -shell-escape modelo-ifg.tex
bibtex modelo-ifg
pdflatex -shell-escape modelo-ifg.tex
pdflatex -shell-escape modelo-ifg.tex
```

O PDF gerado é `relatorio/modelo-ifg.pdf`.

O relatório usa o template oficial de trabalhos do IFG, [classe-ifg](https://github.com/raphaeldeaquino/classe-ifg), do Prof. Dr. Raphael de Aquino.
