------- Antes de correr la consulta correr la tabla auxiliar usuarios---
--------------------------------------------------------------Argentina--------------------------------------------------------------------
SELECT A.FECHA,
      -- ELEMENT_CLASS,
       ELEMENT_ID,
       TRAFICO_DL "Tráfico DL",
       TRAFICO_UL "Tráfico UL",
       B.CONN_USR,
       B.IDLE_USR,
       B.REGISTERED_USR,
       A."Avg PDCP cell thp DL",
       A."Avg PDCP cell thp UL",
       C.MENOR_Q_5,
       C.ENTRE_5_Y_10,
       C.ENTRE_10_Y_20,
       C.MAYOR_Q_20,  
       A."E-RAB Drop",
       A."Radio Network Layer Drop",
       A."Other Reasons",
       A."Radio Link Failure",
       A."Transport Layer Drop",
       A."Other Causes (eNB init rel)",
       A."E-RAB Drop Den",
       A."Acc RRC Denom",
       A."Acc RAB Denom",
       A."Availability"
FROM    
      
(       
SELECT A.FECHA,
       A.ELEMENT_CLASS,
       A.ELEMENT_ID,
       TRAFICO_DL / 1000 TRAFICO_DL,
       TRAFICO_UL / 1000 TRAFICO_UL,
       (AVG_PDCP_CELL_THP_DL_NUM * 8 / AVG_PDCP_CELL_THP_DL_DEN) / 1000    "Avg PDCP cell thp DL",
       (AVG_PDCP_CELL_THP_UL_NUM * 8 / AVG_PDCP_CELL_THP_UL_DEN) / 1000    "Avg PDCP cell thp UL",
       (RAB_DROP_USR_NUM / RAB_DROP_USR_DEN) * 100 "E-RAB Drop",
       EPC_EPS_BEARER_REL_REQ_RNL "Radio Network Layer Drop" ,
       EPC_EPS_BEARER_REL_REQ_OTH "Other Reasons",
       ENB_EPS_BEARER_REL_REQ_RNL "Radio Link Failure",
       ENB_EPS_BEARER_REL_REQ_TNL "Transport Layer Drop",
       ENB_EPS_BEARER_REL_REQ_OTH "Other Causes (eNB init rel)",
       RAB_DROP_ATTEMPTS "E-RAB Drop Den",
       DECODE(RRC_CONN_STP_SUCCESS_NUM,
              0,
              0,
              DECODE(RAB_SETUP_SUCCESS_DEN,
                     0,
                     0,
                     ((RRC_CONN_STP_SUCCESS_NUM /
                     RRC_CONN_STP_SUCCESS_NUM) *
                     (RAB_SETUP_SUCCESS_NUM / RAB_SETUP_SUCCESS_DEN)) * 100)) "Accesibility",
      SIGN_EST_F_RRCCOMPL_MISSING "Completions Missing", 
      SIGN_EST_F_RRCCOMPL_ERROR "Completions Error", 
      SIGN_CONN_ESTAB_FAIL_RRMRAC "Rejection by RRM RAC", 
      SIGN_CONN_ESTAB_FAIL_RB_EMG "Missing RB Resources (emg)",
      EPS_BEARER_SETUP_FAIL_RNL"Radio Network Layer", 
      EPS_BEARER_SETUP_FAIL_RESOUR "Radio Resource", 
      EPS_BEARER_SETUP_FAIL_TRPORT "Transport Layer", 
      EPS_BEARER_SETUP_FAIL_OTH "Other", 
      RRC_CONN_STP_ATTEMPTS "Acc RRC Denom",
      EPS_BEARER_SETUP_ATTEMPTS "Acc RAB Denom",
      (AVAILABILITY_NUM / AVAILABILITY_DEN) * 100 "Availability"
      --FROM lte_nsn_service_ne_dayw A, LTE_NSN_AVAIL_NE_DAYW B
      FROM lte_nsn_service_ne_ibhw A, LTE_NSN_AVAIL_NE_IBHW B
WHERE A.ELEMENT_CLASS in ( 'PAIS')
AND A.FECHA BETWEEN TO_DATE ('&FECHA_INICIO','DD.MM.YYYY')
AND TO_DATE ('&FECHA_FIN','DD.MM.YYYY') + 83999/84000
--AND A.FECHA > ADD_MONTHS(SYSDATE, -12) + F_CALCULO_BISIESTO_WO_FECHA
AND A.ELEMENT_ID not in ('No Especificado')
AND A.FECHA = B.FECHA (+)
AND A.ELEMENT_ID = B.ELEMENT_ID (+)
ORDER BY A.element_class desc, A.element_id, A.fecha
) A,

(SELECT TRUNC(FECHA, 'DAY') FECHA,
       PAIS,
       CONN_USR,
       IDLE_USR,
       REGISTERED_USR

FROM(
      SELECT FECHA,
             'Argentina' PAIS,
             CONN_USR/1000 CONN_USR,
             IDLE_USR/1000 IDLE_USR,
             REGISTERED_USR/1000 REGISTERED_USR,
             ROW_NUMBER() OVER(PARTITION BY 'Argentina'      ---- AGRUPA EL ORDEN POR CELL
                                         ORDER BY REGISTERED_USR DESC    ---- VARIABLE POR LA QUE ME INTERESA ORDENAR
                                         )RANGO
      FROM(
            SELECT FECHA,
                   SUM (CONN_USR) CONN_USR,
                   SUM (IDLE_USR) IDLE_USR,
                   SUM(REG_MAX) REGISTERED_USR 
            FROM(       
                  SELECT TRUNC(PERIOD_START_TIME, 'HH24') FECHA,
                         FINS_ID,
                         decode (AVG(EPS_ECM_CONN_DENOM), 0, 0, SUM(EPS_ECM_CONN_SUM)/AVG(EPS_ECM_CONN_DENOM))  CONN_USR,
                         decode (AVG(EPS_ECM_IDLE_DENOM), 0, 0, SUM(EPS_ECM_IDLE_SUM)/AVG(EPS_ECM_IDLE_DENOM))  IDLE_USR,
                         --decode (AVG(EPS_ECM_CONN_DENOM), 0, 0, SUM(EPS_ECM_CONN_SUM)/AVG(EPS_ECM_CONN_DENOM)) + decode (AVG(EPS_ECM_IDLE_DENOM), 0, 0, SUM(EPS_ECM_IDLE_SUM)/AVG(EPS_ECM_IDLE_DENOM)) SUMUSR
                         MAX(EPS_EMM_REG_MAX)REG_MAX
                  FROM pcofns_ps_umlm_flexins_raw    
                  WHERE PERIOD_START_TIME BETWEEN TO_DATE ('&FECHA_INICIO','DD.MM.YYYY') AND TO_DATE ('&FECHA_FIN','DD.MM.YYYY') + 83999/84000  
                  --WHERE PERIOD_START_TIME > ADD_MONTHS(SYSDATE, -12) + F_CALCULO_BISIESTO_WO_FECHA
                  AND FINS_ID IN (942096, 315163, 315162,1000004977813,1000004982797)
                  GROUP BY TRUNC(PERIOD_START_TIME, 'HH24'),FINS_ID
                  )
                  GROUP BY FECHA
            )
     )
WHERE RANGO = 1

) B,

(      SELECT FECHA,
       PAIS,
       SUM(MENOR_Q_05) MENOR_Q_5,
       SUM(ENTRE_05_Y_10) ENTRE_5_Y_10,
       SUM(ENTRE_10_Y_20) ENTRE_10_Y_20,
       SUM(MAYOR_Q_20) MAYOR_Q_20
  FROM (
SELECT FECHA,
       PAIS,
       CELL_THP_DL,
       CASE WHEN CELL_THP_DL < 5                        THEN 1 ELSE 0 END MENOR_Q_05,
       CASE WHEN CELL_THP_DL >= 5  AND CELL_THP_DL < 10 THEN 1 ELSE 0 END ENTRE_05_Y_10,
       CASE WHEN CELL_THP_DL >= 10 AND CELL_THP_DL < 20 THEN 1 ELSE 0 END ENTRE_10_Y_20,
       CASE WHEN CELL_THP_DL >= 20                      THEN 1 ELSE 0 END MAYOR_Q_20
  FROM (
SELECT FECHA,
       O.PAIS,
       O.LNCEL_NAME,
       DECODE(AVG_PDCP_CELL_THP_DL_DEN, 0, 0, 
       ((AVG_PDCP_CELL_THP_DL_NUM * 8) / AVG_PDCP_CELL_THP_DL_DEN)/1000) CELL_THP_DL, -----NOKLTE_PS_LCELLT_MNC1_RAW
       DECODE(AVG_PDCP_CELL_THP_UL_DEN, 0, 0,
       ((AVG_PDCP_CELL_THP_UL_NUM * 8)   / AVG_PDCP_CELL_THP_UL_DEN)/1000) CELL_THP_UL  -----NOKLTE_PS_LCELLT_MNC1_RAW

  FROM OBJECTS_SP_LTE            O,
       --Lte_Nsn_Service_Lcel_Dayw A
       Lte_Nsn_Service_Lcel_Ibhw A
WHERE A.LNCEL_ID = O.LNCEL_ID
   AND FECHA BETWEEN TO_DATE('&FECHA_INICIO', 'DD.MM.YYYY')
                 AND TO_DATE('&FECHA_FIN', 'DD.MM.YYYY') + 86399 / 86400
   --AND FECHA > ADD_MONTHS(SYSDATE, -12) + F_CALCULO_BISIESTO_WO_FECHA
   AND O.PAIS = 'Argentina'
       )
       )
GROUP BY FECHA,
          PAIS
       ) C
WHERE A.FECHA = DECODE (A.ELEMENT_CLASS, 'PAIS', B.FECHA (+))
   AND A.FECHA = DECODE (A.ELEMENT_CLASS, 'PAIS', C.FECHA (+))
/* AND A.ELEMENT_ID = B.PAIS (+)
  AND A.ELEMENT_ID = C.PAIS (+)*/
  AND A.ELEMENT_ID = DECODE (A.ELEMENT_CLASS, 'PAIS', B.PAIS (+))
  AND A.ELEMENT_ID = DECODE (A.ELEMENT_CLASS, 'PAIS', C.PAIS (+))
  AND B.PAIS NOT IN ('Paraguay')
;
--------------------------------------------------------------Paraguay--------------------------------------------------------------------
SELECT A.FECHA,
      -- ELEMENT_CLASS,
       ELEMENT_ID,
       TRAFICO_DL "Tráfico DL",
       TRAFICO_UL "Tráfico UL",
       B.CONN_USR,
       B.IDLE_USR,
       B.REGISTERED_USR,
       A."Avg PDCP cell thp DL",
       A."Avg PDCP cell thp UL",
       C.MENOR_Q_5,
       C.ENTRE_5_Y_10,
       C.ENTRE_10_Y_20,
       C.MAYOR_Q_20,  
       A."E-RAB Drop",
       A."Radio Network Layer Drop",
       A."Other Reasons",
       A."Radio Link Failure",
       A."Transport Layer Drop",
       A."Other Causes (eNB init rel)",
       A."E-RAB Drop Den",
       A."Acc RRC Denom",
       A."Acc RAB Denom",
       A."Availability"
FROM    
      
(       
SELECT A.FECHA,
       A.ELEMENT_CLASS,
       A.ELEMENT_ID,
       TRAFICO_DL / 1000 TRAFICO_DL,
       TRAFICO_UL / 1000 TRAFICO_UL,
       (AVG_PDCP_CELL_THP_DL_NUM * 8 / AVG_PDCP_CELL_THP_DL_DEN) / 1000    "Avg PDCP cell thp DL",
       (AVG_PDCP_CELL_THP_UL_NUM * 8 / AVG_PDCP_CELL_THP_UL_DEN) / 1000    "Avg PDCP cell thp UL",
       (RAB_DROP_USR_NUM / RAB_DROP_USR_DEN) * 100 "E-RAB Drop",
       EPC_EPS_BEARER_REL_REQ_RNL "Radio Network Layer Drop" ,
       EPC_EPS_BEARER_REL_REQ_OTH "Other Reasons",
       ENB_EPS_BEARER_REL_REQ_RNL "Radio Link Failure",
       ENB_EPS_BEARER_REL_REQ_TNL "Transport Layer Drop",
       ENB_EPS_BEARER_REL_REQ_OTH "Other Causes (eNB init rel)",
       RAB_DROP_ATTEMPTS "E-RAB Drop Den",
       DECODE(RRC_CONN_STP_SUCCESS_NUM,
              0,
              0,
              DECODE(RAB_SETUP_SUCCESS_DEN,
                     0,
                     0,
                     ((RRC_CONN_STP_SUCCESS_NUM /
                     RRC_CONN_STP_SUCCESS_NUM) *
                     (RAB_SETUP_SUCCESS_NUM / RAB_SETUP_SUCCESS_DEN)) * 100)) "Accesibility",
      SIGN_EST_F_RRCCOMPL_MISSING "Completions Missing", 
      SIGN_EST_F_RRCCOMPL_ERROR "Completions Error", 
      SIGN_CONN_ESTAB_FAIL_RRMRAC "Rejection by RRM RAC", 
      SIGN_CONN_ESTAB_FAIL_RB_EMG "Missing RB Resources (emg)",
      EPS_BEARER_SETUP_FAIL_RNL"Radio Network Layer", 
      EPS_BEARER_SETUP_FAIL_RESOUR "Radio Resource", 
      EPS_BEARER_SETUP_FAIL_TRPORT "Transport Layer", 
      EPS_BEARER_SETUP_FAIL_OTH "Other", 
      RRC_CONN_STP_ATTEMPTS "Acc RRC Denom",
      EPS_BEARER_SETUP_ATTEMPTS "Acc RAB Denom",
      (AVAILABILITY_NUM / AVAILABILITY_DEN) * 100 "Availability"
      
      --FROM lte_nsn_service_ne_dayw A, LTE_NSN_AVAIL_NE_DAYW B
      FROM lte_nsn_service_ne_ibhw A, LTE_NSN_AVAIL_NE_IBHW B
WHERE A.ELEMENT_CLASS in ( 'PAIS')
AND A.FECHA BETWEEN TO_DATE ('&FECHA_INICIO','DD.MM.YYYY')
AND TO_DATE ('&FECHA_FIN','DD.MM.YYYY') + 83999/84000
AND A.ELEMENT_ID not in ('No Especificado')
AND A.ELEMENT_ID = 'Paraguay'
AND A.FECHA = B.FECHA (+)
AND A.ELEMENT_ID = B.ELEMENT_ID (+)
AND A.ELEMENT_CLASS = B.ELEMENT_CLASS (+)
ORDER BY A.element_class desc, A.element_id, A.fecha
) A,

(SELECT TRUNC(FECHA, 'DAY') FECHA,
       PAIS,
       CONN_USR,
       IDLE_USR,
       REGISTERED_USR

FROM(
      SELECT FECHA,
             'Paraguay' PAIS,
             CONN_USR/1000 CONN_USR,
             IDLE_USR/1000 IDLE_USR,
             REGISTERED_USR/1000 REGISTERED_USR,
             ROW_NUMBER() OVER(PARTITION BY 'Paraguay'      ---- AGRUPA EL ORDEN POR CELL
                                         ORDER BY REGISTERED_USR DESC    ---- VARIABLE POR LA QUE ME INTERESA ORDENAR
                                         )RANGO
      FROM(
            SELECT FECHA,
                   SUM (CONN_USR) CONN_USR,
                   SUM (IDLE_USR) IDLE_USR,
                   SUM(REG_MAX) REGISTERED_USR 
            FROM(       
                  SELECT TRUNC(PERIOD_START_TIME, 'HH24') FECHA,
                         FINS_ID,
                         decode (AVG(EPS_ECM_CONN_DENOM), 0, 0, SUM(EPS_ECM_CONN_SUM)/AVG(EPS_ECM_CONN_DENOM))  CONN_USR,
                         decode (AVG(EPS_ECM_IDLE_DENOM), 0, 0, SUM(EPS_ECM_IDLE_SUM)/AVG(EPS_ECM_IDLE_DENOM))  IDLE_USR,
                         --decode (AVG(EPS_ECM_CONN_DENOM), 0, 0, SUM(EPS_ECM_CONN_SUM)/AVG(EPS_ECM_CONN_DENOM)) + decode (AVG(EPS_ECM_IDLE_DENOM), 0, 0, SUM(EPS_ECM_IDLE_SUM)/AVG(EPS_ECM_IDLE_DENOM)) SUMUSR
                         MAX(EPS_EMM_REG_MAX)REG_MAX
                  FROM pcofns_ps_umlm_flexins_raw    
                  WHERE PERIOD_START_TIME BETWEEN TO_DATE ('&FECHA_INICIO','DD.MM.YYYY') AND TO_DATE ('&FECHA_FIN','DD.MM.YYYY') + 83999/84000  
                  AND FINS_ID IN (917369, 1558981)
                  GROUP BY TRUNC(PERIOD_START_TIME, 'HH24'),FINS_ID
                  )
                  GROUP BY FECHA
            )
     )
WHERE RANGO = 1
 
) B,

(      SELECT FECHA,
       PAIS,
       SUM(MENOR_Q_05) MENOR_Q_5,
       SUM(ENTRE_05_Y_10) ENTRE_5_Y_10,
       SUM(ENTRE_10_Y_20) ENTRE_10_Y_20,
       SUM(MAYOR_Q_20) MAYOR_Q_20
  FROM (
SELECT FECHA,
       PAIS,
       CELL_THP_DL,
       CASE WHEN CELL_THP_DL < 5                        THEN 1 ELSE 0 END MENOR_Q_05,
       CASE WHEN CELL_THP_DL >= 5  AND CELL_THP_DL < 10 THEN 1 ELSE 0 END ENTRE_05_Y_10,
       CASE WHEN CELL_THP_DL >= 10 AND CELL_THP_DL < 20 THEN 1 ELSE 0 END ENTRE_10_Y_20,
       CASE WHEN CELL_THP_DL >= 20                      THEN 1 ELSE 0 END MAYOR_Q_20
  FROM (
SELECT FECHA,
       O.PAIS,
       O.LNCEL_NAME,
       DECODE(AVG_PDCP_CELL_THP_DL_DEN, 0, 0, 
       ((AVG_PDCP_CELL_THP_DL_NUM * 8) / AVG_PDCP_CELL_THP_DL_DEN)/1000) CELL_THP_DL, -----NOKLTE_PS_LCELLT_MNC1_RAW
       DECODE(AVG_PDCP_CELL_THP_UL_DEN, 0, 0,
       ((AVG_PDCP_CELL_THP_UL_NUM * 8)   / AVG_PDCP_CELL_THP_UL_DEN)/1000) CELL_THP_UL  -----NOKLTE_PS_LCELLT_MNC1_RAW

  FROM OBJECTS_SP_LTE            O,
       --Lte_Nsn_Service_Lcel_Dayw A
       Lte_Nsn_Service_Lcel_Ibhw A
WHERE A.LNCEL_ID = O.LNCEL_ID
   AND FECHA BETWEEN TO_DATE('&FECHA_INICIO', 'DD.MM.YYYY')
                 AND TO_DATE('&FECHA_FIN', 'DD.MM.YYYY') + 86399 / 86400
   AND O.PAIS = 'Paraguay'
       )
       )
GROUP BY FECHA,
          PAIS
       ) C
WHERE A.FECHA = DECODE (A.ELEMENT_CLASS, 'PAIS', B.FECHA (+))
   AND A.FECHA = DECODE (A.ELEMENT_CLASS, 'PAIS', C.FECHA (+))
/* AND A.ELEMENT_ID = B.PAIS (+)
  AND A.ELEMENT_ID = C.PAIS (+)*/
  AND A.ELEMENT_ID = DECODE (A.ELEMENT_CLASS, 'PAIS', B.PAIS (+))
  AND A.ELEMENT_ID = DECODE (A.ELEMENT_CLASS, 'PAIS', C.PAIS (+))
;
--------------------------------------------------------------Mercado--------------------------------------------------------------------
SELECT A.FECHA,
      -- ELEMENT_CLASS,
       ELEMENT_ID,
       TRAFICO_DL "Tráfico DL",
       TRAFICO_UL "Tráfico UL",
       CONNECTED_USERS,
       null,
       null,
       A."Avg PDCP cell thp DL",
       A."Avg PDCP cell thp UL",
       C.MENOR_Q_5,
       C.ENTRE_5_Y_10,
       C.ENTRE_10_Y_20,
       C.MAYOR_Q_20,  
       A."E-RAB Drop",
       A."Radio Network Layer Drop",
       A."Other Reasons",
       A."Radio Link Failure",
       A."Transport Layer Drop",
       A."Other Causes (eNB init rel)",
       A."E-RAB Drop Den",
       A."Acc RRC Denom",
       A."Acc RAB Denom",
       A."Availability"
FROM    
      
(       
SELECT A.FECHA,
       A.ELEMENT_CLASS,
       A.ELEMENT_ID,
       TRAFICO_DL / 1000 TRAFICO_DL,
       TRAFICO_UL / 1000 TRAFICO_UL,
       (AVG_PDCP_CELL_THP_DL_NUM * 8 / AVG_PDCP_CELL_THP_DL_DEN) / 1000    "Avg PDCP cell thp DL",
       (AVG_PDCP_CELL_THP_UL_NUM * 8 / AVG_PDCP_CELL_THP_UL_DEN) / 1000    "Avg PDCP cell thp UL",
       (RAB_DROP_USR_NUM / RAB_DROP_USR_DEN) * 100 "E-RAB Drop",
       EPC_EPS_BEARER_REL_REQ_RNL "Radio Network Layer Drop" ,
       EPC_EPS_BEARER_REL_REQ_OTH "Other Reasons",
       ENB_EPS_BEARER_REL_REQ_RNL "Radio Link Failure",
       ENB_EPS_BEARER_REL_REQ_TNL "Transport Layer Drop",
       ENB_EPS_BEARER_REL_REQ_OTH "Other Causes (eNB init rel)",
       RAB_DROP_ATTEMPTS "E-RAB Drop Den",
       DECODE(RRC_CONN_STP_SUCCESS_NUM,
              0,
              0,
              DECODE(RAB_SETUP_SUCCESS_DEN,
                     0,
                     0,
                     ((RRC_CONN_STP_SUCCESS_NUM /
                     RRC_CONN_STP_SUCCESS_NUM) *
                     (RAB_SETUP_SUCCESS_NUM / RAB_SETUP_SUCCESS_DEN)) * 100)) "Accesibility",
      SIGN_EST_F_RRCCOMPL_MISSING "Completions Missing", 
      SIGN_EST_F_RRCCOMPL_ERROR "Completions Error", 
      SIGN_CONN_ESTAB_FAIL_RRMRAC "Rejection by RRM RAC", 
      SIGN_CONN_ESTAB_FAIL_RB_EMG "Missing RB Resources (emg)",
      EPS_BEARER_SETUP_FAIL_RNL"Radio Network Layer", 
      EPS_BEARER_SETUP_FAIL_RESOUR "Radio Resource", 
      EPS_BEARER_SETUP_FAIL_TRPORT "Transport Layer", 
      EPS_BEARER_SETUP_FAIL_OTH "Other", 
      RRC_CONN_STP_ATTEMPTS "Acc RRC Denom",
      EPS_BEARER_SETUP_ATTEMPTS "Acc RAB Denom",
      (AVAILABILITY_NUM / AVAILABILITY_DEN) * 100 "Availability"

      

      FROM lte_nsn_service_ne_ibhw A,  LTE_NSN_AVAIL_NE_IBHW B
WHERE A.ELEMENT_CLASS in ( 'MERCADO')
AND A.FECHA BETWEEN TO_DATE ('&FECHA_INICIO','DD.MM.YYYY')
AND TO_DATE ('&FECHA_FIN','DD.MM.YYYY') + 83999/84000
AND A.ELEMENT_ID not in ('No Especificado')
AND A.FECHA = B.FECHA (+)
AND A.ELEMENT_ID = B.ELEMENT_ID (+)
ORDER BY A.element_class desc, A.element_id, A.fecha
) A,

     (
SELECT TRUNC(B.FECHA, 'DAY') FECHA,
       A.PAIS,
       MERCADO,
       (USUARIOS/1000) CONNECTED_USERS
FROM (
      SELECT E.PERIOD_START_TIME Fecha,
                     o.pais,
                     O.MERCADO           MERCADO,
                     --  O.LNCEL_NAME,
                     sum(E.SUM_RRC_CONN_UE) / avg(E.DENOM_RRC_CONN_UE) Usuarios
                FROM OBJECTS_SP_LTE O, NOKLTE_PS_LCELLD_MNC1_RAW E
               WHERE E.PERIOD_START_TIME BETWEEN TO_DATE('&Fecha_Inicio', 'DD.MM.YYYY') AND TO_DATE('&Fecha_Fin', 'DD.MM.YYYY') + 86399 / 86400
                 AND E.LNCEL_ID = O.LNCEL_ID
                 and mercado is not null
                 AND O.PAIS = 'Argentina'
               GROUP BY PERIOD_START_TIME, o.pais, O.MERCADO
            ) A, AUX_USUARIOS_LTE B
WHERE a.fecha = b.fecha
       ) B,
       


(      SELECT FECHA,
       MERCADO,
       PAIS,
       SUM(MENOR_Q_05) MENOR_Q_5,
       SUM(ENTRE_05_Y_10) ENTRE_5_Y_10,
       SUM(ENTRE_10_Y_20) ENTRE_10_Y_20,
       SUM(MAYOR_Q_20) MAYOR_Q_20
  FROM (
SELECT FECHA,
       MERCADO,
       PAIS,
       CELL_THP_DL,
       CASE WHEN CELL_THP_DL < 5                        THEN 1 ELSE 0 END MENOR_Q_05,
       CASE WHEN CELL_THP_DL >= 5  AND CELL_THP_DL < 10 THEN 1 ELSE 0 END ENTRE_05_Y_10,
       CASE WHEN CELL_THP_DL >= 10 AND CELL_THP_DL < 20 THEN 1 ELSE 0 END ENTRE_10_Y_20,
       CASE WHEN CELL_THP_DL >= 20                      THEN 1 ELSE 0 END MAYOR_Q_20
  FROM (
SELECT FECHA,
       O.PAIS,
       O.MERCADO,
       O.LNCEL_NAME,
       DECODE(AVG_PDCP_CELL_THP_DL_DEN, 0, 0, 
       ((AVG_PDCP_CELL_THP_DL_NUM * 8) / AVG_PDCP_CELL_THP_DL_DEN)/1000) CELL_THP_DL, -----NOKLTE_PS_LCELLT_MNC1_RAW
       DECODE(AVG_PDCP_CELL_THP_UL_DEN, 0, 0,
       ((AVG_PDCP_CELL_THP_UL_NUM * 8)   / AVG_PDCP_CELL_THP_UL_DEN)/1000) CELL_THP_UL  -----NOKLTE_PS_LCELLT_MNC1_RAW

  FROM OBJECTS_SP_LTE            O,
       Lte_Nsn_Service_Lcel_Ibhw A
WHERE A.LNCEL_ID = O.LNCEL_ID
   AND FECHA BETWEEN TO_DATE('&FECHA_INICIO', 'DD.MM.YYYY')
                 AND TO_DATE('&FECHA_FIN', 'DD.MM.YYYY') + 86399 / 86400
   AND O.PAIS = 'Argentina'
       )
       )
GROUP BY FECHA,
          MERCADO,
          PAIS
       ) C
WHERE A.FECHA = DECODE (A.ELEMENT_CLASS, 'MERCADO', B.FECHA (+))
   AND A.FECHA = DECODE (A.ELEMENT_CLASS, 'MERCADO', C.FECHA (+))
/* AND A.ELEMENT_ID = B.PAIS (+)
  AND A.ELEMENT_ID = C.PAIS (+)*/
  AND A.ELEMENT_ID = DECODE (A.ELEMENT_CLASS, 'MERCADO', B.MERCADO (+))
  AND A.ELEMENT_ID = DECODE (A.ELEMENT_CLASS, 'MERCADO', C.MERCADO (+))
