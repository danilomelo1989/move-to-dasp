create or replace view GOLD.AUX_EV as 

/*
VIEW: AUX_EV
OBJETIVO: JUNÇÃO DAS TABELAS  A EV_PREM E  EV_UTLZ
AUTOR: DANILO MELO
ALTERAÇOES:
26-03-2024 - VERSÃO INICIAL
 */

/*TABELA QUE JUNTA A EV_PREM E A EV_UTLZ */
WITH
PREMIO_UTILIZACAO AS (
    SELECT DISTINCT 
        CHAVE_JOIN
        ,PRE_CHAVE
        , DT_REFR
        , CD_OPRD
        ,CD_EMPR
        , CD_PLNO
        , CD_APLC
        , CD_EMPR_GRPO 
        , FL_SEXO
        ,NR_IDDE
        , NM_FAIX_ETRA
        , NM_CATG_USRO
        ,  CD_DIVISAO -- SOMENTE PARA JBS E FEMSA
        ,PLANO_AON_2 as PLANO_AON2
        ,0 as ARH_COD -- somente para a empresa FEMSA KOF
        ,'EV_PREM' AS ORIGEM
    FROM GOLD.EV_PREM  A 
    UNION ALL
    SELECT DISTINCT 
        CHAVE_JOIN
        ,PRE_CHAVE
        , DT_REFR
        , CD_OPRD
        ,CD_EMPR
        , CD_PLNO
        , CD_APLC
        , CD_EMPR_GRPO 
        , FL_SEXO
        ,NR_IDDE
        , NM_FAIX_ETRA
        , NM_CATG_USRO
        ,  CD_DIVISAO -- SOMENTE PARA JBS E FEMSA
        ,PLANO_AON_2 as PLANO_AON2
        ,0 as ARH_COD -- somente para a empresa FEMSA KOF
        ,'EV_UTLZ' AS ORIGEM
    FROM GOLD.EV_UTLZ  
    WHERE 1=1
        AND CHAVE_JOIN NOT IN (select distinct CHAVE_JOIN from GOLD.EV_PREM )
)
/*SELECT FINAL COM A MONTAGEM DA CHAVE JOIN */
SELECT DISTINCT
    CHAVE_JOIN
    ,PRE_CHAVE   
    ,DT_REFR
    ,CD_OPRD
    ,CD_EMPR
    ,CD_PLNO
    ,CD_APLC
    ,CASE 
        WHEN SUBSTR(CD_EMPR_GRPO, 0,1) = '0' THEN SUBSTR(CD_EMPR_GRPO, 2,5)
        ELSE CD_EMPR_GRPO
    END AS CD_EMPR_GRPO
    ,FL_SEXO
    ,NR_IDDE
    ,NM_FAIX_ETRA
    ,NM_CATG_USRO
    ,CD_DIVISAO
    ,PLANO_AON2
    ,ARH_COD
    ,ORIGEM
FROM PREMIO_UTILIZACAO
;
