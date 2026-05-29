#TODO testar se é mais performatico já retornar todas as linhas com a chave de internacao
def group_by_days(partition_data):
    internation_flag = False
    last_hospitalization_date = ''
    current_hospitalization_key = ''
    start_date = ''
    for row in partition_data:
        if internation_flag is False:
            if row.CHAVE_INTERNACAO_AON is None:
                continue

            internation_flag = True
            current_hospitalization_key = row.CHAVE_INTERNACAO_AON
            start_date = row.DATA_PROCEDIMENTO

        if row.CHAVE_INTERNACAO_AON is not None:
            last_hospitalization_date = row.DATA_PROCEDIMENTO

        #TODO check how to transform 10 in a parameter using UDFs
        if row.NEXT_DAY is None or abs((last_hospitalization_date - row.DATA_PROCEDIMENTO).days) > 10:
            internation_flag = False
            end_date = last_hospitalization_date
            yield [
                row.CODIGO_CONTRATO,
                row.CODIGO_GRUPO_ECONOMICO_OPERADORA,
                row.CODIGO_BENEFICIARIO,
                current_hospitalization_key,
                start_date,
                end_date
            ]