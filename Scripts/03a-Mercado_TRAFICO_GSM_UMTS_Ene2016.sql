set head off
set feedback off
set lines 360
set pages 5000
set verify off
set echo off

spool C:\Demos\InformesMarcela\03a-Mercado_TRAFICO_GSM_UMTS_Ene2016.csv

WITH  MERCADO3G AS  (SELECT /*+ MATERIALIZE*/distinct
                             o1.mercado,
                             t1.fecha,
                             (cs_erl) cs,
                             round(((HS_DSCH_DATA_VOL) / (1024 * 1024 * 1024)), 2) hsdpaMacdMb,
                             round(decode((HSDPA_ACT_USR_DEN), 0, 0,
                                          ((HSDPA_ACT_USR_NUM)) /
                                          ((HSDPA_ACT_USR_DEN))), 2) hsdpaActUserAvg
                      FROM alm_mercado                  o1,
                           umts_nsn_service_mkt_ISABHWC t1,
                           umtsc_nsn_macd_mkt_ISABHW    t2,
                           umts_nsn_hsdpa_mkt_ISABHW    t3
                      WHERE t2.period_start_time(+) = t1.fecha
                      AND t2.mercado(+) = t1.mercado
                      AND t3.mercado(+) = t1.mercado
                      AND t3.fecha(+) = t1.fecha
                      AND T1.FECHA > ADD_MONTHS(SYSDATE, -12) + F_CALCULO_BISIESTO_WO_FECHA
                      AND o1.mercado = t1.mercado),
      MERCADOX2G  AS  (
                      --2G x Mercado
                      SELECT /*+ MATERIALIZE*/ distinct
                             o1.mercado,
                             t1.fecha,
                             ROUND(NVL((t1.TCH_ERLANG), 0), 2) tchErlang,
                             NVL(ROUND(((EDGE_UL_TRAFFIC) + (t2.EDGE_DL_TRAFFIC)) /
                                       (1024 * 1024), 2), 0) trafficEDGE
                      FROM alm_mercado                 o1,
                           multivendor_mkt_isabhw      t1,
                           multivendor_gprs_mkt_isabhw t2
                       WHERE 1 = 1
                       AND t2.mercado = t1.mercado
                       AND t2.fecha = t1.fecha
                       AND T1.FECHA > ADD_MONTHS(SYSDATE, -12) + F_CALCULO_BISIESTO_WO_FECHA
                       AND o1.mercado = t1.mercado),
      MINUTOS AS  (
                  -- Minutos
                  SELECT /*+ MATERIALIZE*/
                        fecha, 
                        mercado, 
                        tot_subs_lac_gsm, 
                        tot_subs_lac_umts
                  FROM tablero_mous_dayw
                  WHERE fecha > add_months(SYSDATE, -12) + f_calculo_bisiesto_wo_fecha
                  AND region = 'MERCADO'
                  AND semana = 'D-S'),
      DISPOGSMMERCADO AS  (
                            -- Dispo GSM MERcado
                            SELECT /*+ MATERIALIZE*/ distinct
                                   o1.mercado,
                                   t1.fecha,
                                   round(100 *
                                         ((((cell_wo_state_usr_NUM_full)) +
                                         ((min_err_administrativo) +
                                         (min_exc_operativo) + (min_exc_implantacion) +
                                         (min_exc_acceso) + (min_exc_clausura) +
                                         (min_exc_contractual) + (min_exc_energia) +
                                         (min_exc_vandalismo) + (min_exc_operaciones) +
                                         (min_exc_proceso_alta) +
                                         (min_exc_baja_comercial))) /
                                         ((CELL_WO_STATE_USR_DEN_FULL))),
                                         2) disponibilidadOpe2g,
                                   CASE
                                     WHEN (CELL_WO_STATE_USR_DEN_FULL) = 0 THEN  0
                                     ELSE  ROUND(100 * ((CELL_WO_STATE_USR_NUM_FULL) /
                                            (CELL_WO_STATE_USR_DEN_FULL)),2)
                                   END disponibilidadTotal2g
                             FROM alm_mercado o1, noc_umts_avail_mkt_daywo t1
                             WHERE 1 = 1 -- t1.mercado = 'AMBA' AND 
                             AND T1.FECHA > ADD_MONTHS(SYSDATE, -12) + F_CALCULO_BISIESTO_WO_FECHA
                             AND t1.semana = 'D-S'
                             AND o1.mercado = t1.mercado
                             AND t1.ESTADO = 'Comercial'),
      DISPONIBILIDADUMTSMERCADO  AS  (SELECT  /*+ MATERIALIZE*/
                                              distinct o1.mercado,
                                              t1.fecha,
                                              ROUND(100 -
                                                    100 *
                                                    ((BCCH_DOWNTIME_FULL) -
                                                    ((MIN_ERR_ADMINISTRATIVO) +
                                                    (MIN_EXC_OPERATIVO) + (MIN_EXC_IMPLANTACION) +
                                                    (MIN_EXC_ACCESO) + (MIN_EXC_CLAUSURA) +
                                                    (MIN_EXC_CONTRACTUAL) + (MIN_EXC_ENERGIA) +
                                                    (MIN_EXC_VANDALISMO) + (MIN_EXC_OPERACIONES) +
                                                    (MIN_EXC_PROCESO_ALTA) +
                                                    (MIN_EXC_BAJA_COMERCIAL))) /
                                                    ((BCCH_DOWNTIME_FULL) + (BCCH_UPTIME_FULL)),
                                                    2) disponibilidadOpe3g,
                                              ROUND(100 -
                                                    100 * ((BCCH_DOWNTIME_FULL)) /
                                                    ((BCCH_DOWNTIME_FULL) + (BCCH_UPTIME_FULL)),
                                                    2) disponibilidadTotal3g                                      
                                       FROM alm_mercado o1, noc_gsm_avail_mkt_daywo t1
                                       WHERE 1 = 1
                                       AND T1.FECHA > ADD_MONTHS(SYSDATE, -12) + F_CALCULO_BISIESTO_WO_FECHA
                                       AND t1.semana = 'D-S'
                                       AND o1.mercado = t1.mercado
                                       AND t1.ESTADO = 'Comercial'),
      TABLAR  AS  (SELECT FECHA,
                          ELEMENT_NAME MERCADO,
                          MAX(DECODE(INDICADOR_NOMBRE, 'TOT_SUBS_LAC_GSM'          , INDICADOR_VALOR)) TOT_SUBS_LAC_GSM,
                          MAX(DECODE(INDICADOR_NOMBRE, 'TOT_SUBS_LAC_UMTS'         , INDICADOR_VALOR)) TOT_SUBS_LAC_UMTS,
                          MAX(DECODE(INDICADOR_NOMBRE, 'TCHERLANG'                , INDICADOR_VALOR)) TCHERLANG,
                          MAX(DECODE(INDICADOR_NOMBRE, 'CS'                        , INDICADOR_VALOR)) CS,
                          MAX(DECODE(INDICADOR_NOMBRE, 'TRAFFICEDGE'               , INDICADOR_VALOR)) TRAFFICEDGE,
                          MAX(DECODE(INDICADOR_NOMBRE, 'HSDPAMACDMB'               , INDICADOR_VALOR)) HSDPAMACDMB,
                          MAX(DECODE(INDICADOR_NOMBRE, 'HSDPAACTUSERAVG'           , INDICADOR_VALOR)) HSDPAACTUSERAVG,
                          MAX(DECODE(INDICADOR_NOMBRE, 'DISPONIBILIDADOPE2G'       , INDICADOR_VALOR)) DISPONIBILIDADOPE2G,
                          MAX(DECODE(INDICADOR_NOMBRE, 'DISPONIBILIDADTOTAL2G'     , INDICADOR_VALOR)) DISPONIBILIDADTOTAL2G,
                          MAX(DECODE(INDICADOR_NOMBRE, 'DISPONIBILIDADOPE3G'       , INDICADOR_VALOR)) DISPONIBILIDADOPE3G,
                          MAX(DECODE(INDICADOR_NOMBRE, 'DISPONIBILIDADTOTAL3G'     , INDICADOR_VALOR)) DISPONIBILIDADTOTAL3G
                  FROM TABLERO_ACCESO_REFERENCES
                  WHERE FECHA > ADD_MONTHS(SYSDATE, -12) + F_CALCULO_BISIESTO_WO_FECHA
                  AND ELEMENT_TYPE = 'MERCADO'
                  AND SUMARIZACION = 'DAYW'
                  AND FLAG_STATUS = 'ENABLED'
                  GROUP BY FECHA,ELEMENT_NAME)
--TAB=TableroValueMercado
SELECT /*csv*/
       A.FECHA,
       A.MERCADO,
       DECODE(R.TOT_SUBS_LAC_GSM, NULL,
              C.TOT_SUBS_LAC_GSM, R.TOT_SUBS_LAC_GSM)                   TOT_SUBS_LAC_GSM,
       DECODE(R.TOT_SUBS_LAC_UMTS, NULL,
              C.TOT_SUBS_LAC_UMTS, R.TOT_SUBS_LAC_UMTS)                 TOT_SUBS_LAC_UMTS,
       DECODE(R.TCHERLANG, NULL,
              B.TCHERLANG, R.TCHERLANG)                                 TCHERLANG,
       DECODE(R.CS, NULL,
              A.CS, R.CS)                                               CS,
       DECODE(R.TRAFFICEDGE, NULL,
              B.TRAFFICEDGE, R.TRAFFICEDGE)                             TRAFFICEDGE,
       DECODE(R.HSDPAMACDMB, NULL,
              A.HSDPAMACDMB, R.HSDPAMACDMB)                             HSDPAMACDMB,
       DECODE(R.HSDPAACTUSERAVG, NULL,
              A.HSDPAACTUSERAVG, R.HSDPAACTUSERAVG)                     HSDPAACTUSERAVG,
       DECODE(R.DISPONIBILIDADOPE2G, NULL,
              D.DISPONIBILIDADOPE2G, R.DISPONIBILIDADOPE2G)             DISPONIBILIDADOPE2G,
       DECODE(R.DISPONIBILIDADTOTAL2G, NULL,
              D.DISPONIBILIDADTOTAL2G, R.DISPONIBILIDADTOTAL2G)         DISPONIBILIDADTOTAL2G,
       DECODE(R.DISPONIBILIDADOPE3G, NULL,
              E.DISPONIBILIDADOPE3G, R.DISPONIBILIDADOPE3G)             DISPONIBILIDADOPE3G,
       DECODE(R.DISPONIBILIDADTOTAL3G, NULL,
              E.DISPONIBILIDADTOTAL3G, R.DISPONIBILIDADTOTAL3G)         DISPONIBILIDADTOTAL3G
  FROM MERCADO3G A,
       MERCADOX2G B,
       MINUTOS C,
       DISPOGSMMERCADO D,
       DISPONIBILIDADUMTSMERCADO E,
       TABLAR R
  WHERE A.FECHA = B.FECHA (+)
    AND A.FECHA = C.FECHA (+)
    AND A.FECHA = D.FECHA (+)
    AND A.FECHA = E.FECHA (+)
    AND A.FECHA = R.FECHA (+)
    AND A.MERCADO = B.MERCADO (+)
    AND A.MERCADO = C.MERCADO (+)
    AND A.MERCADO = D.MERCADO (+)
    AND A.MERCADO = E.MERCADO (+)
    AND A.MERCADO = R.MERCADO (+)
 ORDER BY A.MERCADO, A.FECHA;
spool off