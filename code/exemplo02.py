from datetime import datetime
import pandas as pd
import polars as pl 
import numpy as np


ENDERECO_DADOS = './bronze/'

try:
    print('iniciando a leitura do arquivo Parquet')
    inicio = datetime.now()
    # pandas
    # df_bolsa_familia = pd.read_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet')

    # polars
    df_bolsa_familia = pl.scan_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet')

    df_bolsa_familia = df_bolsa_familia.collect()

    print(df_bolsa_familia.head())

    print(df_bolsa_familia.columns)
    print(df_bolsa_familia.dtypes)

    final = datetime.now()
    print(f'Tempo de Execução: {final - inicio}')
    print('Arquivo Parquet lido com sucesso!')

except Exception as e:
    print(f'Erro ao carregar os dados: {e}')


# Processando as informações

try:
    array_valor_parcela = np.array(df_bolsa_familia['VALOR PARCELA'])
    media = np.mean(array_valor_parcela)
    mediana = np.median(array_valor_parcela)
    distancia_media_mediana = abs(media - mediana) / mediana

    print(f'Média: {media}')
    print(f'Mediana: {mediana}')
    print(f'Distância entre média e mediana: {distancia_media_mediana:.2%}')


except Exception as e:
    print(f'Erro ao processar os dados: {e}')