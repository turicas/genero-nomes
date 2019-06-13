# Gênero dos Nomes Brasileiros

Script que baixa dados de gênero do [IBGE
Nomes](https://censo2010.ibge.gov.br/nomes/) (Censo 2010) e cria um banco de
dados, que pode ser utilizado para classificar nomes por gênero em bases que
não possuem essa informação.


## Licença

A licença do código é [LGPL3](https://www.gnu.org/licenses/lgpl-3.0.en.html) e
dos dados convertidos [Creative Commons Attribution
ShareAlike](https://creativecommons.org/licenses/by-sa/4.0/). Caso utilize os
dados, **cite a fonte original e quem tratou os dados**, como: **Fonte:
IBGE/Censo 2010, dados tratados por Álvaro
Justen/[Brasil.IO](https://brasil.io/)**. Caso compartilhe os dados, **utilize
a mesma licença**.


## Dados

Caso você não queira/possa rodar o script, **[acesse diretamente os dados
convertidos no Brasil.IO](https://brasil.io/dataset/genero-nomes)**.

Se esse programa e/ou os dados resultantes foram úteis a você ou à sua empresa,
considere [fazer uma doação ao projeto Brasil.IO](https://brasil.io/doe), que é
mantido voluntariamente.


## Rodando

### Instalando as Dependências

Esse script depende de Python 3.7 e de algumas bibliotecas. Depois de instalar
o Python 3.7 instale as bibliotecas executando:

```bash
pip install -r requirements.txt
```

### Executando

Como o IBGE não divulga um índice de todos os nomes, é necessário que
você possua um arquivo que tenha uma lista de nomes para que o script possa
fazer a consulta. Por padrão o script utiliza como base um arquivo chamado
`data/input/documentos-brasil.csv.xz`, que deve possuir uma coluna `name` com o
nome e uma coluna `document-type` com o valor `CPF` ([o *dataset*
documentos-brasil do Brasil.IO possui esses
dados](https://brasil.io/dataset/documentos-brasil/documents)).

Depois de conseguir esse arquivo, execute o script:

```bash
./run.sh
```
