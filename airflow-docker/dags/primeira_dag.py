# Imports
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash  import BashOperator
import pandas as pd
import requests
import json

# Função para coletar dados via api
def captura_conta_dados():
    url = "https://data.cityofnewyork.us/resource/ic3t-wcy2.json"
    response = requests.get(url)
    df = pd.DataFrame(json.loads(response.content))
    qtd = len(df.index)
    return qtd

# Função que verifica se a quantidade de row é true ou não 
def e_valida(ti):
    #coletando o dado da variavel qtd
    qtd = ti.xcom_pull(task_ids = "captura_conta_dados")
    if (qtd > 10000):
        return 'valido' #task_ids
    return 'nao_valido' #task_ids


# Criando instancia da DAG
with DAG('primeira_dag', start_date = datetime(2022,7,27),
        schedule_interval = '30 * * * *', catchup = False ) as dag:

# Criando Task
        captura_conta_dados = PythonOperator(
            task_id = 'captura_conta_dados',
            python_callable = captura_conta_dados
        )
# Task que verifica se a quantidade é valida ou não dentro de uma função         
        e_valida = BranchPythonOperator(
            task_id = 'e_valida',
            python_callable = e_valida
        )

# Task condicional usando operator bash
        valido = BashOperator(
            task_id = 'valido',
            bash_command = "echo 'Quantidade ok' "
        )
        nao_valido = BashOperator(
            task_id = 'nao_valido',
            bash_command = "echo 'Quantidade nao ok' "
        )

        captura_conta_dados >> e_valida >> [valido, nao_valido]