from datetime import datetime
import pandas as pd
import polars as pl 
import numpy as np
import matplotlib.pyplot as plt


ENDERECO_DADOS = './bronze/'

try:
    print('iniciando a leitura do arquivo Parquet')
    inicio = datetime.now()
    # pandas
    # df_bolsa_familia = pd.read_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet')

    # plano de execução
    df_bolsa_familia = (
        pl.scan_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet')
        .select(pl.col('VALOR PARCELA'))
        .filter(pl.col('VALOR PARCELA') > 1518)
    )

    df_bolsa_familia = df_bolsa_familia.collect()

    final = datetime.now()
    print(f'Tempo de Execução: {final - inicio}')
    print('Arquivo Parquet lido com sucesso!')

except Exception as e:
    print(f'Erro ao carregar os dados: {e}')


# Processando

try:
    array_valor_parcela = np.array(df_bolsa_familia['VALOR PARCELA'])

except Exception as e:
    print(f'Erro ao processar os dados: {e}')


# visualizando a distribuição

try:
    print('Visualizando a distribuição')
    plt.boxplot(array_valor_parcela, showmeans=True, vert=False)

    plt.title('Distribuição das Parcelas')
    plt.show()

except Exception as e:
    print(f'Erro ao processar os dados: {e}')