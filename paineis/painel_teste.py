import streamlit as st
import pandas as pd
import snowflake.connector

from authentication.login import login_user  # Manteve-se pois é do projeto

def show_teste_snowflake():
    st.title("Painel Teste Snowflake")

    # Conexão com o Snowflake
    conn = snowflake.connector.connect(
        user="JULIANO.CALDAS@AUTOGLASS.COM.BR", 
        account="ZGVSIFS-CBB87909",
        database = 'REPORT',
        role='GL_SNOWFLAKE_ACESSO_MAX_PRICING',    
        warehouse="WH_USE_FATURAMENTO",
        authenticator="externalbrowser"
    )
    cursor = conn.cursor()

    # Query SQL
    query = '''
        WITH BASE_FATURAMENTO AS (
            SELECT *, 'HISTORICO' AS FONTE 
            FROM REPORT.MAX_FATURAMENTO.REP_FATURAMENTO_HISTORICO_DELTA
            UNION ALL
            SELECT *, 'ATUAL' AS FONTE 
            FROM REPORT.MAX_FATURAMENTO.REP_FATURAMENTO_ATUAL_DELTA
            UNION ALL
            SELECT *, 'DETALHADO' AS FONTE 
            FROM REPORT.MAX_FATURAMENTO.REP_FATURAMENTO_DETALHADO_APPINT    
        )
        SELECT 
            REP_FAT.DSC_CODIGO_FIPE || '-' || TO_CHAR(CAST(REP_FAT.NUM_ANO_MODELO AS INT)) AS CHAVE_APOIO,
            COUNT(DISTINCT CASE 
                WHEN DAT_REFERENCIA = DAT_FECHAMENTO THEN CONCAT(NUM_APOLICE, COD_ITEM) 
                ELSE NULL 
            END) AS ITENS
        FROM 
            BASE_FATURAMENTO REP_FAT
        WHERE
            REP_FAT.DAT_REFERENCIA BETWEEN '2025-04-01' AND '2025-04-30'
            AND REP_FAT.DAT_REFERENCIA IS NOT NULL
            AND REP_FAT.VAL_COBRADO IS NOT NULL
        GROUP BY 
            DSC_CODIGO_FIPE,
            NUM_ANO_MODELO
    '''

    # Execução da query
    cursor.execute(query)
    dados = cursor.fetchall()
    colunas = [desc[0] for desc in cursor.description]
    df_itens = pd.DataFrame(dados, columns=colunas)

    # Exibição no Streamlit
    st.dataframe(df_itens)