#!/bin/bash

if [ -z $1 ]
	then
		echo "Primeiro argumento: indique a quantidade de threads"
		exit 1
fi
if [ -z $2 ]
	then
		echo "Segundo argumento: indique o caminho e nome do script .py"
		exit 1
fi
if [ -z $3 ]
	then
		echo "Terceiro argumento: indique o caminho dos arquivos de log"
		exit 1
fi

$SPARK_HOME/bin/spark-submit --master local[$1] $2 $3
