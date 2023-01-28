# Imports
from pathlib import Path
import pipe1
import pipe2
import os
from datetime import datetime as dt

data_hoje = dt.today().strftime(format="%Y-%m-%d")
diretorio_postgres = Path.cwd() / 'local' / 'postgres'
diretorio_csv = Path.cwd() / 'local' / 'csv'

# Função com texto do menu
def texto_menu():
        print('\n'+'#'*25)
        print('LIGHTHOUSE - PIPELINES')
        print('#'*25+'\n')
        print('Opção 1 - PIPELINE 1: GRAVAR DADOS EM DISCO LOCAL')
        print('Opção 2 - PIPELINE 2: CARREGAR DADOS LOCAIS PARA BANCO DE DADOS POSTGRES')
        print('Opção 5 - Sair do programa\n')

# Função para limpar o console.
def limpar_console():
        os.system('cls' if os.name == 'nt' else 'clear')

while True:
    texto_menu()
    opcao = input("Selecione uma opção: ")
    if opcao == "1":
        limpar_console()
        print('PASSO 1')
        pipe1.exec_pipe1()

    elif opcao == "2":
        limpar_console()
        print('PASSO 2')
        dia = input("Escolha uma data para executar o pipeline 2: (Ano-Mês-Dia)").strip()

        #Loop para validar se existe a data fornecida pelo usuário.
        for lista_dias in os.listdir(diretorio_csv):
            if dia in lista_dias:
                pipe2.exec_pipe2(dia)
            else:
                print('Data não encontrada. Digite uma data válida ou execute novamente o Pipeline 1.')

    elif opcao == "5":
        limpar_console()
        print("LIGHTHOUSE - PIPELINES ENCERRADO! Muito obrigado.")
        break
    else:
        limpar_console()
        print("Opção inválida.")
    
    
        