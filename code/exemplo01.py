from datetime import datetime
import pandas as pd
import polars as pl 


ENDERECO_DADOS = './bronze/'

try:
    print('iniciando a leitura do arquivo Parquet')
    inicio = datetime.now()
    # pandas
    # df_bolsa_familia = pd.read_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet')

    # polars
    df_bolsa_familia = pl.read_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet')
    print(df_bolsa_familia.head())

    print(df_bolsa_familia.columns)
    print(df_bolsa_familia.dtypes)

    final = datetime.now()
    print(f'Tempo de Execução: {final - inicio}')
    print('Arquivo Parquet lido com sucesso!')

except Exception as e:
    print(f'Erro ao carregar os dados: {e}')