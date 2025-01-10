import cx_Oracle
import pandas as pd

#############################
# 1. Definir las consultas  #
#############################

def build_query_anomalias(anio):
    """
    Retorna la gran UNION ALL de la QUERY ANOMALÍAS,
    sustituyendo '2015' por el año 'anio' en cada subconsulta.
    Se han removido los filtros fijos de beneficiario (p.ej. 28388)
    para que tome el 100% de Beneficiarios.
    """
    query = f"""
    SELECT --FORM 01
        t.o_beneficiario                    AS Beneficiario,
        CAST(t.aa_resolucion AS VARCHAR2(10)) AS aa_formulario,
        CAST(t.t_resolucion  AS VARCHAR2(10)) AS t_formulario,
        CAST(t.o_resolucion  AS NUMBER)       AS o_formulario,
        TRUNC(t.f_ingreso)                   AS fh_imputacion,
        CAST(NULL AS VARCHAR2(10))           AS aa_comprobante,
        CAST(NULL AS VARCHAR2(10))           AS t_comprobante,
        CAST(NULL AS NUMBER)                 AS o_comprobante,
        CAST(NULL AS VARCHAR2(50))           AS c_mediopago,
        SUM(d.i_devengado)                   AS i_devengado,
        CAST(NULL AS NUMBER(16,2))           AS i_pagado,
        CAST(NULL AS NUMBER(16,2))           AS ia_pago,
        'tresolucion'                        AS comentario
    FROM gs_tresolucion t
    JOIN gs_dresol_item d
          ON t.aa_resolucion = d.aa_resolucion
         AND t.t_resolucion = d.t_resolucion
         AND t.o_resolucion = d.o_resolucion
    WHERE t.t_resolucion = 'RSD'
      AND t.e_resolucion = 'Z'
      AND d.c_numcred IS NOT NULL
      AND t.aa_resolucion = {anio}
    GROUP BY
        CAST(t.aa_resolucion AS VARCHAR2(10)),
        CAST(t.t_resolucion  AS VARCHAR2(10)),
        CAST(t.o_resolucion  AS NUMBER),
        TRUNC(t.f_ingreso),
        t.o_beneficiario
    HAVING NVL(SUM(d.i_devengado), 0) != 0

    UNION ALL

    SELECT --FORM 02
        t.o_ente                             AS Beneficiario,
        CAST(t.aa_devengado AS VARCHAR2(10)) AS aa_formulario,
        CAST(t.t_devengado  AS VARCHAR2(10)) AS t_formulario,
        CAST(t.n_devengado  AS NUMBER)       AS o_formulario,
        TRUNC(di.fh_imputacion)             AS fh_imputacion,
        CAST(NULL AS VARCHAR2(10))          AS aa_comprobante,
        CAST(NULL AS VARCHAR2(10))          AS t_comprobante,
        CAST(NULL AS NUMBER)                AS o_comprobante,
        CAST(NULL AS VARCHAR2(50))          AS c_mediopago,
        SUM(di.i_devengado)                 AS i_devengado,
        CAST(NULL AS NUMBER(16,2))          AS i_pagado,
        CAST(NULL AS NUMBER(16,2))          AS ia_pago,
        'tdevengado'                        AS comentario
    FROM gs_tdevengado t
    JOIN gs_ddevengado_ffi di
          ON t.o_devengado = di.o_devengado
    WHERE t.e_devengado = 'A'
      AND di.c_numcred IS NOT NULL
      AND t.aa_devengado = {anio}
      AND t.aa_devengado != EXTRACT(YEAR FROM di.fh_imputacion)
    GROUP BY
        CAST(t.aa_devengado AS VARCHAR2(10)),
        CAST(t.t_devengado  AS VARCHAR2(10)),
        CAST(t.n_devengado  AS NUMBER),
        TRUNC(di.fh_imputacion),
        t.o_ente
    HAVING NVL(SUM(di.i_devengado), 0) != 0

    UNION ALL

    SELECT -- FORM 03
        t.o_ente                             AS Beneficiario,
        CAST(t.aa_precepcion AS VARCHAR2(10)) AS aa_formulario,
        CAST(t.t_precepcion  AS VARCHAR2(10)) AS t_formulario,
        CAST(t.n_precepcion  AS NUMBER)       AS o_formulario,
        TRUNC(t.fh_imputacion)               AS fh_imputacion,
        CAST(t.aa_ocompra AS VARCHAR2(10))    AS aa_comprobante,
        CAST(t.t_ocompra  AS VARCHAR2(10))    AS t_comprobante,
        CAST(t.n_ocompra  AS NUMBER)          AS o_comprobante,
        CAST(NULL AS VARCHAR2(50))           AS c_mediopago,
        SUM(NVL(d.i_total, 0))               AS i_devengado,
        CAST(NULL AS NUMBER(16,2))           AS i_pagado,
        CAST(NULL AS NUMBER(16,2))           AS ia_pago,
        'tparte_recepcion'                   AS comentario
    FROM prd_tparte_recepcion t
    JOIN prd_dparte_recepcion_ffi d
          ON t.aa_precepcion = d.aa_precepcion
         AND t.t_precepcion  = d.t_precepcion
         AND t.n_precepcion  = d.n_precepcion
    WHERE t.e_formulario IN ('A')
      AND t.aa_precepcion = {anio}
      AND t.aa_precepcion != EXTRACT(YEAR FROM t.fh_imputacion)
    GROUP BY
        CAST(t.aa_precepcion AS VARCHAR2(10)),
        CAST(t.t_precepcion  AS VARCHAR2(10)),
        CAST(t.n_precepcion  AS NUMBER),
        TRUNC(t.fh_imputacion),
        CAST(t.aa_ocompra AS VARCHAR2(10)),
        CAST(t.t_ocompra  AS VARCHAR2(10)),
        CAST(t.n_ocompra  AS NUMBER),
        t.o_ente
    HAVING NVL(SUM(d.i_total), 0) != 0

    UNION ALL

    SELECT  --FORM 04
        t.o_contratista                      AS Beneficiario,
        CAST(t.aa_certificado AS VARCHAR2(10)) AS aa_formulario,
        'CAO'                                AS t_formulario,
        CAST(t.o_certificado  AS NUMBER)     AS o_formulario,
        TRUNC(t.f_certificacion)            AS fh_imputacion,
        CAST(t.t_contrato AS VARCHAR2(10))   AS aa_comprobante,
        CAST(t.n_contrato AS VARCHAR2(10))   AS t_comprobante,
        CAST(t.aa_contrato AS NUMBER)        AS o_comprobante,
        CAST(NULL AS VARCHAR2(50))          AS c_mediopago,
        SUM(d.i_devengado)                  AS i_devengado,
        CAST(NULL AS NUMBER(16,2))          AS i_pagado,
        CAST(NULL AS NUMBER(16,2))          AS ia_pago,
        'tcertif_avance'                    AS comentario
    FROM obp_tcertificado_avance t
    JOIN obp_dcert_avance_ffi d
          ON t.aa_certificado   = d.aa_certificado
         AND t.t_form_medicion = d.t_form_medicion
         AND t.o_certificado   = d.o_certificado
         AND t.c_obra          = d.co_obra
         AND t.n_dev           = d.n_dev
    WHERE t.e_certificado = 'A'
      AND t.aa_certificado = {anio}
      AND t.aa_certificado != EXTRACT(YEAR FROM d.F_IMPUTACION)
    GROUP BY
        CAST(t.aa_certificado AS VARCHAR2(10)),
        'CAO',
        CAST(t.o_certificado AS NUMBER),
        TRUNC(t.f_certificacion),
        CAST(t.t_contrato AS VARCHAR2(10)),
        CAST(t.n_contrato AS VARCHAR2(10)),
        CAST(t.aa_contrato AS NUMBER),
        t.o_contratista
    HAVING NVL(SUM(d.i_devengado), 0) != 0

    UNION ALL

    SELECT --FORM 05
        t.O_ENTE                             AS Beneficiario,
        CAST(t.aa_certificado AS VARCHAR2(10)) AS aa_formulario,
        CAST(t.t_certificado  AS VARCHAR2(10)) AS t_formulario,
        CAST(t.n_certificado  AS NUMBER)       AS o_formulario,
        TRUNC(t.fh_autorizacion)             AS fh_imputacion,
        CAST(t.t_ocompra AS VARCHAR2(10))     AS aa_comprobante,
        CAST(t.n_ocompra AS VARCHAR2(10))     AS t_comprobante,
        CAST(t.aa_ocompra AS NUMBER)          AS o_comprobante,
        CAST(NULL AS VARCHAR2(50))           AS c_mediopago,
        SUM(d.i_devengado)                   AS i_devengado,
        CAST(NULL AS NUMBER(16,2))           AS i_pagado,
        CAST(NULL AS NUMBER(16,2))           AS ia_pago,
        'tcertificado'                       AS comentario
    FROM obp_tcertificado t
    JOIN obp_dcertificado_ffi d
          ON t.aa_certificado = d.aa_certificado
         AND t.t_certificado  = d.t_certificado
         AND t.n_certificado  = d.n_certificado
    WHERE t.e_certificado = 'A'
      AND t.aa_certificado = {anio}
      AND t.aa_certificado != EXTRACT(YEAR FROM t.fh_autorizacion)
    GROUP BY
        CAST(t.aa_certificado AS VARCHAR2(10)),
        CAST(t.t_certificado  AS VARCHAR2(10)),
        CAST(t.n_certificado  AS NUMBER),
        TRUNC(t.fh_autorizacion),
        CAST(t.t_ocompra AS VARCHAR2(10)),
        CAST(t.n_ocompra AS VARCHAR2(10)),
        CAST(t.aa_ocompra AS NUMBER),
        t.O_ENTE
    HAVING NVL(SUM(d.i_devengado), 0) != 0

    UNION ALL

    SELECT --FORM 06
        t.O_ENTE                             AS Beneficiario,
        CAST(t.aa_certificado AS VARCHAR2(10)) AS aa_formulario,
        CAST(t.t_certificado  AS VARCHAR2(10)) AS t_formulario,
        CAST(t.n_certificado  AS NUMBER)       AS o_formulario,
        TRUNC(t.fh_autorizacion)             AS fh_imputacion,
        CAST(t.t_ocompra AS VARCHAR2(10))     AS aa_comprobante,
        CAST(t.n_ocompra AS VARCHAR2(10))     AS t_comprobante,
        CAST(t.aa_ocompra AS NUMBER)          AS o_comprobante,
        CAST(NULL AS VARCHAR2(50))           AS c_mediopago,
        SUM(d.i_devengado)                   AS i_devengado,
        CAST(NULL AS NUMBER(16,2))           AS i_pagado,
        CAST(NULL AS NUMBER(16,2))           AS ia_pago,
        'tanticipo_financiero'               AS comentario
    FROM slu.tanticipo_financiero t
    JOIN slu.danticipo_financiero_ffi d
          ON t.aa_certificado = d.aa_ejervg
         AND t.o_certificado  = d.o_certificado
    WHERE t.e_certificado = 'A'
      AND t.aa_certificado = {anio}
      AND t.aa_certificado != EXTRACT(YEAR FROM t.fh_autorizacion)
    GROUP BY
        CAST(t.aa_certificado AS VARCHAR2(10)),
        CAST(t.t_certificado  AS VARCHAR2(10)),
        CAST(t.n_certificado  AS NUMBER),
        TRUNC(t.fh_autorizacion),
        CAST(t.t_ocompra  AS VARCHAR2(10)),
        CAST(t.n_ocompra  AS VARCHAR2(10)),
        CAST(t.aa_ocompra AS NUMBER),
        t.O_ENTE
    HAVING NVL(SUM(d.i_devengado), 0) != 0

    UNION ALL

    SELECT -- FORM 08
        t.o_beneficiario                     AS Beneficiario,
        CAST(t.aa_formulario AS VARCHAR2(10)) AS aa_formulario,
        CAST(t.t_formulario  AS VARCHAR2(10)) AS t_formulario,
        CAST(t.o_formulario  AS NUMBER)       AS o_formulario,
        TRUNC(t.fh_imputacion)              AS fh_imputacion,
        CAST(t.aa_form_orig AS VARCHAR2(10)) AS aa_comprobante,
        CAST(t.t_form_orig  AS VARCHAR2(10)) AS t_comprobante,
        CAST(t.o_form_orig  AS NUMBER)       AS o_comprobante,
        CAST(NULL AS VARCHAR2(50))          AS c_mediopago,
        CAST(NULL AS NUMBER(16,2))          AS i_devengado,
        SUM(d.i_pagado)                     AS i_pagado,
        CAST(NULL AS NUMBER(16,2))          AS ia_pago,
        'tformulario_c55'                   AS comentario
    FROM gs_tformulario t
    JOIN gs_dform_item d
          ON t.aa_formulario = d.aa_formulario
         AND t.t_formulario  = d.t_formulario
         AND t.o_formulario  = d.o_formulario
    WHERE t.t_formulario = 'C55'
      AND t.e_formulario NOT IN ('X','I','S')
      AND d.c_numcred IS NOT NULL
      AND t.aa_formulario = {anio}
      AND t.aa_formulario != EXTRACT(YEAR FROM t.fh_imputacion)
    GROUP BY
        CAST(t.aa_formulario AS VARCHAR2(10)),
        CAST(t.t_formulario  AS VARCHAR2(10)),
        CAST(t.o_formulario  AS NUMBER),
        TRUNC(t.fh_imputacion),
        CAST(t.aa_form_orig AS VARCHAR2(10)),
        CAST(t.t_form_orig  AS VARCHAR2(10)),
        CAST(t.o_form_orig  AS NUMBER),
        t.o_beneficiario
    HAVING NVL(SUM(d.i_pagado), 0) != 0
    """
    return query


def build_query_total(anio, beneficiarios):
    """
    Retorna la QUERY TOTAL (10 subconsultas con UNION ALL)
    filtrando SOLO a los beneficiarios en la lista 'beneficiarios' (list_str).
    """
    if not beneficiarios:
        return "SELECT * FROM DUAL WHERE 1=0"

    # Convertimos la lista en texto "28388,30001,30002"
    list_str = ",".join(str(b) for b in beneficiarios)

    query = f"""
    SELECT --FORM 01
    t.o_beneficiario                               AS Beneficiario,
    CAST(t.aa_resolucion AS VARCHAR2(10))          AS aa_formulario,
    CAST(t.t_resolucion  AS VARCHAR2(10))          AS t_formulario,
    CAST(t.o_resolucion  AS NUMBER)                AS o_formulario,
    TRUNC(t.f_ingreso)                             AS fh_imputacion,
    CAST(NULL AS VARCHAR2(10))                     AS aa_comprobante,
    CAST(NULL AS VARCHAR2(10))                     AS t_comprobante,
    CAST(NULL AS NUMBER)                           AS o_comprobante,
    CAST(NULL AS VARCHAR2(50))                     AS c_mediopago,
    SUM(d.i_devengado)                             AS i_devengado,
    CAST(NULL AS NUMBER(16,2))                     AS i_pagado,
    CAST(NULL AS NUMBER(16,2))                     AS ia_pago,
    'tresolucion'                                  AS comentario
FROM gs_tresolucion t
JOIN gs_dresol_item d
      ON t.aa_resolucion = d.aa_resolucion
     AND t.t_resolucion  = d.t_resolucion
     AND t.o_resolucion  = d.o_resolucion
WHERE t.t_resolucion = 'RSD'
  AND t.e_resolucion = 'Z'
  AND d.c_numcred IS NOT NULL
  AND t.o_beneficiario IN ({list_str})
  AND t.aa_resolucion = {anio}
GROUP BY 
    CAST(t.aa_resolucion AS VARCHAR2(10)),
    CAST(t.t_resolucion  AS VARCHAR2(10)),
    CAST(t.o_resolucion  AS NUMBER),
    TRUNC(t.f_ingreso),
    t.o_beneficiario
HAVING NVL(SUM(d.i_devengado), 0) != 0

UNION ALL

SELECT --FORM 02
    t.o_ente                                      AS Beneficiario,
    CAST(t.aa_devengado AS VARCHAR2(10))          AS aa_formulario,
    CAST(t.t_devengado  AS VARCHAR2(10))          AS t_formulario,
    CAST(t.n_devengado  AS NUMBER)                AS o_formulario,
    TRUNC(di.fh_imputacion)                       AS fh_imputacion,
    CAST(NULL AS VARCHAR2(10))                    AS aa_comprobante,
    CAST(NULL AS VARCHAR2(10))                    AS t_comprobante,
    CAST(NULL AS NUMBER)                          AS o_comprobante,
    CAST(NULL AS VARCHAR2(50))                    AS c_mediopago,
    SUM(di.i_devengado)                           AS i_devengado,
    CAST(NULL AS NUMBER(16,2))                    AS i_pagado,
    CAST(NULL AS NUMBER(16,2))                    AS ia_pago,
    'tdevengado'                                  AS comentario
FROM gs_tdevengado t
JOIN gs_ddevengado_ffi di
      ON t.o_devengado = di.o_devengado
WHERE t.e_devengado = 'A'
  AND di.c_numcred IS NOT NULL
  AND t.o_ente IN ({list_str})
  AND t.aa_devengado = {anio}
GROUP BY
    CAST(t.aa_devengado AS VARCHAR2(10)),
    CAST(t.t_devengado  AS VARCHAR2(10)),
    CAST(t.n_devengado  AS NUMBER),
    TRUNC(di.fh_imputacion),
    t.o_ente
HAVING NVL(SUM(di.i_devengado), 0) != 0

UNION ALL

SELECT --FORM 03
    t.o_ente                                      AS Beneficiario,
    CAST(t.aa_precepcion AS VARCHAR2(10))         AS aa_formulario,
    CAST(t.t_precepcion  AS VARCHAR2(10))         AS t_formulario,
    CAST(t.n_precepcion  AS NUMBER)               AS o_formulario,
    TRUNC(t.fh_imputacion)                        AS fh_imputacion,
    CAST(t.aa_ocompra AS VARCHAR2(10))            AS aa_comprobante,
    CAST(t.t_ocompra  AS VARCHAR2(10))            AS t_comprobante,
    CAST(t.n_ocompra  AS NUMBER)                  AS o_comprobante,
    CAST(NULL AS VARCHAR2(50))                    AS c_mediopago,
    SUM(NVL(d.i_total, 0))                        AS i_devengado,
    CAST(NULL AS NUMBER(16,2))                    AS i_pagado,
    CAST(NULL AS NUMBER(16,2))                    AS ia_pago,
    'tparte_recepcion'                            AS comentario
FROM prd_tparte_recepcion t
JOIN prd_dparte_recepcion_ffi d
      ON t.aa_precepcion = d.aa_precepcion
     AND t.t_precepcion  = d.t_precepcion
     AND t.n_precepcion  = d.n_precepcion
WHERE t.e_formulario IN ('A')
  AND t.o_ente IN ({list_str})
  AND t.aa_precepcion = {anio}
GROUP BY
    CAST(t.aa_precepcion AS VARCHAR2(10)),
    CAST(t.t_precepcion  AS VARCHAR2(10)),
    CAST(t.n_precepcion  AS NUMBER),
    TRUNC(t.fh_imputacion),
    CAST(t.aa_ocompra AS VARCHAR2(10)),
    CAST(t.t_ocompra  AS VARCHAR2(10)),
    CAST(t.n_ocompra  AS NUMBER),
    t.o_ente
HAVING NVL(SUM(d.i_total), 0) != 0

UNION ALL

SELECT  --FORM 04
    t.o_contratista                               AS Beneficiario,
    CAST(t.aa_certificado AS VARCHAR2(10))        AS aa_formulario,
    'CAO'                                         AS t_formulario,
    CAST(t.o_certificado  AS NUMBER)              AS o_formulario,
    TRUNC(t.f_certificacion)                      AS fh_imputacion,
    CAST(t.t_contrato AS VARCHAR2(10))            AS aa_comprobante,
    CAST(t.n_contrato AS VARCHAR2(10))            AS t_comprobante,
    CAST(t.aa_contrato AS NUMBER)                 AS o_comprobante,
    CAST(NULL AS VARCHAR2(50))                    AS c_mediopago,
    SUM(d.i_devengado)                            AS i_devengado,
    CAST(NULL AS NUMBER(16,2))                    AS i_pagado,
    CAST(NULL AS NUMBER(16,2))                    AS ia_pago,
    'tcertif_avance'                              AS comentario
FROM obp_tcertificado_avance t
JOIN obp_dcert_avance_ffi d
      ON t.aa_certificado   = d.aa_certificado
     AND t.t_form_medicion = d.t_form_medicion
     AND t.o_certificado   = d.o_certificado
     AND t.c_obra          = d.co_obra
     AND t.n_dev           = d.n_dev
WHERE t.e_certificado = 'A'
  AND t.o_contratista IN ({list_str})
  AND t.aa_certificado = {anio}
GROUP BY
    CAST(t.aa_certificado AS VARCHAR2(10)),
    'CAO',
    CAST(t.o_certificado AS NUMBER),
    TRUNC(t.f_certificacion),
    CAST(t.t_contrato AS VARCHAR2(10)),
    CAST(t.n_contrato AS VARCHAR2(10)),
    CAST(t.aa_contrato AS NUMBER),
    t.o_contratista
HAVING NVL(SUM(d.i_devengado), 0) != 0

UNION ALL

SELECT --FORM 05
    t.O_ENTE                                     AS Beneficiario,
    CAST(t.aa_certificado AS VARCHAR2(10))       AS aa_formulario,
    CAST(t.t_certificado  AS VARCHAR2(10))       AS t_formulario,
    CAST(t.n_certificado  AS NUMBER)             AS o_formulario,
    TRUNC(t.fh_autorizacion)                     AS fh_imputacion,
    CAST(t.t_ocompra AS VARCHAR2(10))            AS aa_comprobante,
    CAST(t.n_ocompra AS VARCHAR2(10))            AS t_comprobante,
    CAST(t.aa_ocompra AS NUMBER)                 AS o_comprobante,
    CAST(NULL AS VARCHAR2(50))                   AS c_mediopago,
    SUM(d.i_devengado)                           AS i_devengado,
    CAST(NULL AS NUMBER(16,2))                   AS i_pagado,
    CAST(NULL AS NUMBER(16,2))                   AS ia_pago,
    'tcertificado'                               AS comentario
FROM obp_tcertificado t
JOIN obp_dcertificado_ffi d
      ON t.aa_certificado = d.aa_certificado
     AND t.t_certificado  = d.t_certificado
     AND t.n_certificado  = d.n_certificado
WHERE t.e_certificado = 'A'
  AND t.o_ente IN ({list_str})
  AND t.aa_certificado = {anio}
GROUP BY
    CAST(t.aa_certificado AS VARCHAR2(10)),
    CAST(t.t_certificado  AS VARCHAR2(10)),
    CAST(t.n_certificado  AS NUMBER),
    TRUNC(t.fh_autorizacion),
    CAST(t.t_ocompra AS VARCHAR2(10)),
    CAST(t.n_ocompra AS VARCHAR2(10)),
    CAST(t.aa_ocompra AS NUMBER),
    t.O_ENTE
HAVING NVL(SUM(d.i_devengado), 0) != 0

UNION ALL

SELECT --FORM 06
    t.O_ENTE                                     AS Beneficiario,
    CAST(t.aa_certificado AS VARCHAR2(10))       AS aa_formulario,
    CAST(t.t_certificado  AS VARCHAR2(10))       AS t_formulario,
    CAST(t.n_certificado  AS NUMBER)             AS o_formulario,
    TRUNC(t.fh_autorizacion)                     AS fh_imputacion,
    CAST(t.t_ocompra AS VARCHAR2(10))            AS aa_comprobante,
    CAST(t.n_ocompra AS VARCHAR2(10))            AS t_comprobante,
    CAST(t.aa_ocompra AS NUMBER)                 AS o_comprobante,
    CAST(NULL AS VARCHAR2(50))                   AS c_mediopago,
    SUM(d.i_devengado)                           AS i_devengado,
    CAST(NULL AS NUMBER(16,2))                   AS i_pagado,
    CAST(NULL AS NUMBER(16,2))                   AS ia_pago,
    'tanticipo_financiero'                       AS comentario
FROM slu.tanticipo_financiero t
JOIN slu.danticipo_financiero_ffi d
      ON t.aa_certificado = d.aa_ejervg
     AND t.o_certificado  = d.o_certificado
WHERE t.e_certificado = 'A'
  AND t.o_ente IN ({list_str})
  AND t.aa_certificado = {anio}
GROUP BY
    CAST(t.aa_certificado AS VARCHAR2(10)),
    CAST(t.t_certificado  AS VARCHAR2(10)),
    CAST(t.n_certificado  AS NUMBER),
    TRUNC(t.fh_autorizacion),
    CAST(t.t_ocompra  AS VARCHAR2(10)),
    CAST(t.n_ocompra  AS VARCHAR2(10)),
    CAST(t.aa_ocompra AS NUMBER),
    t.O_ENTE
HAVING NVL(SUM(d.i_devengado), 0) != 0

UNION ALL

SELECT -- FORM 07
    t.o_beneficiario                             AS Beneficiario,
    CAST(p.aa_pago AS VARCHAR2(10))              AS aa_formulario,
    'PAG'                                        AS t_formulario,
    CAST(p.o_pago AS NUMBER)                     AS o_formulario,
    TRUNC(p.fh_pago)                             AS fh_imputacion,
    CAST(p.aa_op AS VARCHAR2(10))                AS aa_comprobante,
    CAST(p.t_op  AS VARCHAR2(10))                AS t_comprobante,
    CAST(p.o_op  AS NUMBER)                      AS o_comprobante,
    CAST(p.c_mediopago AS VARCHAR2(50))          AS c_mediopago,
    CAST(NULL AS NUMBER(16,2))                   AS i_devengado,
    CAST(NULL AS NUMBER(16,2))                   AS i_pagado,
    SUM(p.ia_pago)                               AS ia_pago,
    'pg_tpago_1'                                 AS comentario
FROM gs_tformulario t
JOIN pg_tpago p
      ON t.aa_formulario = p.aa_op
     AND t.t_formulario  = p.t_op
     AND t.o_formulario  = p.o_op
WHERE p.t_op IN ('C41','C42')
  AND TO_NUMBER(TO_CHAR(p.fh_pago, 'YYYY')) = p.aa_op
  AND (p.fh_anulacion IS NULL OR TO_NUMBER(TO_CHAR(p.fh_anulacion, 'YYYY')) > p.aa_op)
  AND t.o_beneficiario IN ({list_str})
  AND p.aa_pago = {anio}
GROUP BY
    CAST(p.aa_pago AS VARCHAR2(10)),
    'PAG',
    CAST(p.o_pago AS NUMBER),
    TRUNC(p.fh_pago),
    CAST(p.aa_op AS VARCHAR2(10)),
    CAST(p.t_op  AS VARCHAR2(10)),
    CAST(p.o_op  AS NUMBER),
    CAST(p.c_mediopago AS VARCHAR2(50)),
    t.o_beneficiario
HAVING NVL(SUM(p.ia_pago), 0) != 0

UNION ALL

SELECT -- FORM 08
    t.o_beneficiario                             AS Beneficiario,
    CAST(t.aa_formulario AS VARCHAR2(10))        AS aa_formulario,
    CAST(t.t_formulario  AS VARCHAR2(10))        AS t_formulario,
    CAST(t.o_formulario  AS NUMBER)              AS o_formulario,
    TRUNC(t.fh_imputacion)                       AS fh_imputacion,
    CAST(t.aa_form_orig AS VARCHAR2(10))         AS aa_comprobante,
    CAST(t.t_form_orig  AS VARCHAR2(10))         AS t_comprobante,
    CAST(t.o_form_orig  AS NUMBER)               AS o_comprobante,
    CAST(NULL AS VARCHAR2(50))                   AS c_mediopago,
    CAST(NULL AS NUMBER(16,2))                   AS i_devengado,
    SUM(d.i_pagado)                              AS i_pagado,
    CAST(NULL AS NUMBER(16,2))                   AS ia_pago,
    'tformulario_c55'                            AS comentario
FROM gs_tformulario t
JOIN gs_dform_item d
      ON t.aa_formulario = d.aa_formulario
     AND t.t_formulario  = d.t_formulario
     AND t.o_formulario  = d.o_formulario
WHERE t.t_formulario = 'C55'
  AND t.e_formulario NOT IN ('X', 'I', 'S')
  AND d.c_numcred IS NOT NULL
  AND t.o_beneficiario IN ({list_str})
  AND t.aa_formulario = {anio}
GROUP BY
    CAST(t.aa_formulario AS VARCHAR2(10)),
    CAST(t.t_formulario  AS VARCHAR2(10)),
    CAST(t.o_formulario  AS NUMBER),
    TRUNC(t.fh_imputacion),
    CAST(t.aa_form_orig AS VARCHAR2(10)),
    CAST(t.t_form_orig  AS VARCHAR2(10)),
    CAST(t.o_form_orig  AS NUMBER),
    t.o_beneficiario
HAVING NVL(SUM(d.i_pagado), 0) != 0

UNION ALL

SELECT --FORM 09
    t.o_beneficiario                             AS Beneficiario,
    CAST(p.aa_pago AS VARCHAR2(10))              AS aa_formulario,
    'PAG'                                        AS t_formulario,
    CAST(p.o_pago AS NUMBER)                     AS o_formulario,
    TRUNC(p.fh_anulacion)                        AS fh_imputacion,
    CAST(p.aa_op AS VARCHAR2(10))                AS aa_comprobante,
    CAST(p.t_op  AS VARCHAR2(10))                AS t_comprobante,
    CAST(p.o_op  AS NUMBER)                      AS o_comprobante,
    CAST(NULL AS VARCHAR2(50))                   AS c_mediopago,
    CAST(NULL AS NUMBER(16,2))                   AS i_devengado,
    CAST(NULL AS NUMBER(16,2))                   AS i_pagado,
    SUM(p.ia_pago)                               AS ia_pago,
    'pg_tpago_anula'                             AS comentario
FROM gs_tformulario t
JOIN pg_tpago p
      ON t.aa_formulario = p.aa_op
     AND t.t_formulario  = p.t_op
     AND t.o_formulario  = p.o_op
WHERE p.t_op IN ('C41','C42')
  AND p.aa_op < TO_NUMBER(TO_CHAR(p.fh_anulacion, 'YYYY'))
  AND TO_NUMBER(TO_CHAR(p.fh_pago, 'YYYY')) < TO_NUMBER(TO_CHAR(p.fh_anulacion, 'YYYY'))
  AND t.o_beneficiario IN ({list_str})
  AND p.aa_pago = {anio}
GROUP BY
    CAST(p.aa_pago AS VARCHAR2(10)),
    'PAG',
    CAST(p.o_pago AS NUMBER),
    TRUNC(p.fh_anulacion),
    CAST(p.aa_op AS VARCHAR2(10)),
    CAST(p.t_op  AS VARCHAR2(10)),
    CAST(p.o_op  AS NUMBER),
    t.o_beneficiario
HAVING NVL(SUM(p.ia_pago), 0) != 0

UNION ALL

SELECT --FORM 10
    t.o_beneficiario                             AS Beneficiario,
    CAST(p.aa_pago AS VARCHAR2(10))              AS aa_formulario,
    'PAG'                                        AS t_formulario,
    CAST(p.o_pago AS NUMBER)                     AS o_formulario,
    TRUNC(p.fh_pago)                             AS fh_imputacion,
    CAST(p.aa_op AS VARCHAR2(10))                AS aa_comprobante,
    CAST(p.t_op  AS VARCHAR2(10))                AS t_comprobante,
    CAST(p.o_op  AS NUMBER)                      AS o_comprobante,
    CAST(p.c_mediopago AS VARCHAR2(50))          AS c_mediopago,
    CAST(NULL AS NUMBER(16,2))                   AS i_devengado,
    CAST(NULL AS NUMBER(16,2))                   AS i_pagado,
    SUM(p.ia_pago)                               AS ia_pago,
    'pg_tpago_mayor_op'                          AS comentario
FROM gs_tformulario t
JOIN pg_tpago p
      ON t.aa_formulario = p.aa_op
     AND t.t_formulario  = p.t_op
     AND t.o_formulario  = p.o_op
WHERE p.t_op IN ('C41','C41')
  AND TO_NUMBER(TO_CHAR(p.fh_pago, 'YYYY')) > p.aa_op
  AND p.fh_anulacion IS NULL
  AND t.o_beneficiario IN ({list_str})
  AND p.aa_pago = {anio}
GROUP BY
    CAST(p.aa_pago AS VARCHAR2(10)),
    'PAG',
    CAST(p.o_pago AS NUMBER),
    TRUNC(p.fh_pago),
    CAST(p.aa_op AS VARCHAR2(10)),
    CAST(p.t_op  AS VARCHAR2(10)),
    CAST(p.o_op  AS NUMBER),
    CAST(p.c_mediopago AS VARCHAR2(50)),
    t.o_beneficiario
HAVING NVL(SUM(p.ia_pago), 0) != 0
    """
    return query


#############################
# 2. Función para insertar subtotales, 2 líneas en blanco y la palabra "subtotal"
#############################

def add_subtotals_and_spaces(df):
    """
    Recibe un DataFrame con las columnas:
      [BENEFICIARIO, i_devengado, i_pagado, ia_pago, ...]
    Devuelve un nuevo DataFrame donde, para cada Beneficiario:
      - Se listan las filas de detalle,
      - Se insertan 2 filas en blanco,
      - Se agrega 1 fila con 'subtotal' + sum de las columnas numéricas.

    Asume que las columnas numéricas a subtotear son: i_devengado, i_pagado, ia_pago
    (Si cambian, ajusta en numeric_cols).
    """
    if df.empty:
        return df  # No tocamos nada

    # Columnas numéricas que deseas subtotear
    numeric_cols = []
    for col in ["i_devengado", "i_pagado", "ia_pago"]:
        if col in df.columns:
            numeric_cols.append(col)

    # Orden de columnas (para rearmar luego)
    original_cols = df.columns.tolist()

    # Agrupamos por beneficiario
    # NOTA: Asegúrate de que "BENEFICIARIO" existe. Si no, revisa alias.
    grouped = df.groupby("BENEFICIARIO", dropna=False)

    new_rows = []
    for bene_value, grp in grouped:
        # 1) Agregar filas de detalle
        new_rows.append(grp)

        # 3) Fila de subtotal
        #    Ponemos 'subtotal' en la columna BENEFICIARIO, sumamos numeric_cols
        subtotal_dict = {col: "" for col in original_cols}  # por defecto vacío
        subtotal_dict["BENEFICIARIO"] = "subtotal"  # Etiqueta en la col BENEFICIARIO

        # 2) Insertar 2 filas en blanco
        blank_df = pd.DataFrame([[""] * len(original_cols)] * 2, columns=original_cols)
        new_rows.append(blank_df)



        for nc in numeric_cols:
            subtotal_dict[nc] = grp[nc].sum()

        df_sub = pd.DataFrame([subtotal_dict], columns=original_cols)
        new_rows.append(df_sub)

        # 4) Otra fila en blanco (opcional) -> si quisieras otra
        #    blank_df2 = pd.DataFrame([[""] * len(original_cols)], columns=original_cols)
        #    new_rows.append(blank_df2)

    # Concatenar todo
    df_final = pd.concat(new_rows, ignore_index=True)

    return df_final[original_cols]


#############################
# 3. Lógica principal (main)
#############################

def main():
    # 1. Pedir rango de años
    anio_inicial = int(input("Ingrese año inicial (aa_formulario): "))
    anio_final = int(input("Ingrese año final (aa_formulario): "))

    # 2. Datos de conexión
    host = "10.17.25.52"
    sid = "preprod"
    user = "SLU"
    pwd = "fsugyt8h"

    dsn = cx_Oracle.makedsn(host, 1521, sid=sid)
    try:
        conn = cx_Oracle.connect(user, pwd, dsn)
        print("Conexión exitosa.")
    except cx_Oracle.Error as e:
        print(f"Error al conectar a la BD: {e}")
        return

    # 3. Crear un ExcelWriter (salida final)
    output_file = "resultado_anomalias_y_total.xlsx"
    writer = pd.ExcelWriter(output_file, engine='openpyxl')

    try:
        cursor = conn.cursor()

        for anio in range(anio_inicial, anio_final + 1):
            print(f"\nProcesando año {anio}...")

            #######################
            # 3.1 Query Anomalías
            #######################
            sql_anom = build_query_anomalias(anio)
            cursor.execute(sql_anom)
            rows_anom = cursor.fetchall()
            cols_anom = [desc[0] for desc in cursor.description]
            df_anom = pd.DataFrame(rows_anom, columns=cols_anom)

            # Ordenar por BENEFICIARIO si existe
            if not df_anom.empty and "BENEFICIARIO" in df_anom.columns:
                df_anom.sort_values(by="BENEFICIARIO", inplace=True)

            # Insertar subtotales y 2 líneas en blanco
            df_anom_st = add_subtotals_and_spaces(df_anom)

            # Escribimos la hoja "Anomalias_{anio}"
            sheet_anom_name = f"Anomalias_{anio}"
            df_anom_st.to_excel(writer, sheet_name=sheet_anom_name, index=False)
            print(f"  -> Anomalías año {anio}: {len(df_anom)} registros (sin contar subtotales).")

            #######################
            # 3.2 Beneficiarios únicos
            #######################
            benef_list = df_anom["BENEFICIARIO"].unique().tolist() if not df_anom.empty else []

            #######################
            # 3.3 Query Total
            #######################
            if len(benef_list) == 0:
                # Sin beneficiarios => DataFrame vacío
                df_total = pd.DataFrame(columns=cols_anom)
            else:
                sql_total = build_query_total(anio, benef_list)
                cursor.execute(sql_total)
                rows_tot = cursor.fetchall()
                cols_tot = [desc[0] for desc in cursor.description]
                df_total = pd.DataFrame(rows_tot, columns=cols_tot)

                # Ordenar por BENEFICIARIO si existe
                if not df_total.empty and "BENEFICIARIO" in df_total.columns:
                    df_total.sort_values(by="BENEFICIARIO", inplace=True)

            # Insertar subtotales y 2 líneas en blanco en la Query Total
            df_total_st = add_subtotals_and_spaces(df_total)

            # Guardamos la hoja "Total_{anio}"
            sheet_tot_name = f"Total_{anio}"
            df_total_st.to_excel(writer, sheet_name=sheet_tot_name, index=False)
            print(f"  -> Total año {anio}: {len(df_total)} registros (sin contar subtotales).")

    except cx_Oracle.Error as e:
        print(f"Error al ejecutar consultas: {e}")
    finally:
        cursor.close()
        conn.close()
        writer.close()

        print(f"\nArchivo Excel generado: {output_file}")


#############################
# 4. Ejecutar
#############################
if __name__ == "__main__":
    main()