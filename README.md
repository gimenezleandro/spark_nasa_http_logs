# ANÁLISE DE LOGS HTTP

## Análise de logs de requisições HTTP para o servidor WWW NASA Kennedy Space Center com Spark.
Fonte oficial do dateset : http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html
    
    Dados:
    ● Jul 01 to Jul 31, ASCII format, 20.7 MB gzip compressed , 205.2 MB.
    ● Aug 04 to Aug 31, ASCII format, 21.8 MB gzip compressed , 167.8 MB.
  
Os logs estão em arquivos ASCII com uma linha por requisição com as seguintes colunas:

    ● Host fazendo a requisição. Um hostname quando possível, caso contrário o endereço de internet se o nome
    não puder ser identificado.
    ● Timestamp no formato "DIA/MÊS/ANO:HH:MM:SS TIMEZONE"
    ● Requisição (entre aspas)
    ● Código do retorno HTTP
    ● Total de bytes retornados


## Executando a aplicação
Premissa: A variável de ambiente $SPARK_HOME deve estar configurada

A aplicação log_nasa.py será executada através do script spark_submit.sh, para que a ação seja bem sucedida é preciso informar 3 parâmetros:

        1. Número de workers (cores)
        2. Caminho e nome do script log_nada.py
        3. Caminho de onde estão os arquivos de log a serem analisados

Ex: ./spark_submit.sh 2 /pyspark/log_nasa.py /pyspark/filelogs/


## Pontos a serem analisados
1. Número de hosts únicos.
2. O total de erros 404.
3. Os 5 URLs que mais causaram erro 404.
4. Quantidade de erros 404 por dia.
5. O total de bytes retornados.
