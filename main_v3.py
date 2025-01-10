import cx_Oracle
import pandas as pd


def construir_query_diferente(anio):
    """
    Consulta base con la condición '!=', reemplazando '2015' por el año indicado.
    """
    query_base = """
    SELECT
        t.O_ENte AS Beneficiario,
        TO_CHAR(t.aa_devengado) AS aa_formulario,
        TO_CHAR(t.t_devengado) AS t_formulario,
        TO_CHAR(t.n_devengado) AS o_formulario,
        TRUNC(di.fh_imputacion) AS fh_imputacion,
        NULL AS aa_comprobante,
        NULL AS t_comprobante,
        NULL AS o_comprobante,
        SUM(di.i_devengado) AS i_devengado
    FROM gs_tdevengado t
    JOIN gs_ddevengado_ffi di ON t.o_devengado = di.o_devengado
    WHERE t.e_devengado = 'A'
      AND di.c_numcred IS NOT NULL
      AND t.aa_devengado = '2015'
      AND EXTRACT(YEAR FROM di.fh_imputacion) != t.aa_devengado
    GROUP BY t.aa_devengado,
             t.t_devengado,
             t.n_devengado,
             TRUNC(di.fh_imputacion),
             t.O_ENte

    UNION ALL

    SELECT
        t.O_ENte AS Beneficiario,
        TO_CHAR(t.aa_precepcion) AS aa_formulario,
        TO_CHAR(t.t_precepcion) AS t_formulario,
        TO_CHAR(t.n_precepcion) AS o_formulario,
        TRUNC(t.fh_imputacion) AS fh_imputacion,
        TO_CHAR(t.aa_ocompra) AS aa_comprobante,
        TO_CHAR(t.t_ocompra) AS t_comprobante,
        TO_CHAR(t.n_ocompra) AS o_comprobante,
        SUM(NVL(d.i_total, 0)) AS i_devengado
    FROM prd_tparte_recepcion t
    JOIN prd_dparte_recepcion_ffi d
      ON t.aa_precepcion = d.aa_precepcion
      AND t.t_precepcion = d.t_precepcion
      AND t.n_precepcion = d.n_precepcion
    WHERE t.e_formulario IN ('A')
      AND t.aa_precepcion = '2015'
      AND EXTRACT(YEAR FROM t.fh_imputacion) != t.aa_precepcion
    GROUP BY t.aa_precepcion,
             t.t_precepcion,
             t.n_precepcion,
             TRUNC(t.fh_imputacion),
             t.aa_ocompra,
             t.t_ocompra,
             t.n_ocompra,
             t.O_ENte

    UNION ALL

    SELECT
        t.O_CONTRATISTA AS Beneficiario,
        TO_CHAR(t.aa_certificado) AS aa_formulario,
        'CAO' AS t_formulario,
        TO_CHAR(t.o_certificado) AS o_formulario,
        TRUNC(t.f_certificacion) AS fh_imputacion,
        TO_CHAR(t.t_contrato) AS aa_comprobante,
        TO_CHAR(t.n_contrato) AS t_comprobante,
        TO_CHAR(t.aa_contrato) AS o_comprobante,
        SUM(d.i_devengado) AS i_devengado
    FROM obp_tcertificado_avance t
    JOIN obp_dcert_avance_ffi d 
      ON t.aa_certificado = d.aa_certificado
      AND t.t_form_medicion = d.t_form_medicion
      AND t.o_certificado = d.o_certificado
      AND t.c_obra = d.co_obra
      AND t.n_dev = d.n_dev
    WHERE t.e_certificado = 'A'
      AND EXTRACT(YEAR FROM d.f_imputacion) != t.aa_certificado
      AND t.aa_certificado = '2015'
    GROUP BY t.aa_certificado,
             t.o_certificado,
             TRUNC(t.f_certificacion),
             t.t_contrato,
             t.n_contrato,
             t.aa_contrato,
             t.O_CONTRATISTA

    UNION ALL

    SELECT
        t.O_ENTE  AS Beneficiario,
        TO_CHAR(t.aa_certificado) AS aa_formulario,
        TO_CHAR(t.t_certificado) AS t_formulario,
        TO_CHAR(t.n_certificado) AS o_formulario,
        TRUNC(t.fh_autorizacion) AS fh_imputacion,
        TO_CHAR(t.aa_ocompra) AS aa_comprobante,
        TO_CHAR(t.t_ocompra) AS t_comprobante,
        TO_CHAR(t.n_ocompra) AS o_comprobante,
        SUM(d.i_devengado) AS i_devengado
    FROM obp_tcertificado t
    JOIN obp_dcertificado_ffi d 
      ON t.aa_certificado = d.aa_certificado
      AND t.t_certificado = d.t_certificado
      AND t.n_certificado = d.n_certificado
    WHERE t.e_certificado = 'A'
      AND t.aa_certificado = '2015'
      AND EXTRACT(YEAR FROM t.fh_autorizacion) != t.aa_certificado
    GROUP BY t.aa_certificado,
             t.t_certificado,
             t.n_certificado,
             TRUNC(t.fh_autorizacion),
             t.aa_ocompra,
             t.t_ocompra,
             t.n_ocompra,
             t.O_ENTE

    UNION ALL

    SELECT
        t.O_ENTE AS Beneficiario,
        TO_CHAR(t.aa_certificado) AS aa_formulario,
        TO_CHAR(t.t_certificado) AS t_formulario,
        TO_CHAR(t.n_certificado) AS o_formulario,
        TRUNC(t.fh_autorizacion) AS fh_imputacion,
        TO_CHAR(t.aa_ocompra) AS aa_comprobante,
        TO_CHAR(t.t_ocompra) AS t_comprobante,
        TO_CHAR(t.n_ocompra) AS o_comprobante,
        SUM(d.i_devengado) AS i_devengado
    FROM slu.tanticipo_financiero t
    JOIN slu.danticipo_financiero_ffi d 
      ON t.aa_certificado = d.aa_ejervg
      AND t.o_certificado = d.o_certificado
    WHERE t.e_certificado = 'A'
      AND t.aa_certificado = '2015'
      AND EXTRACT(YEAR FROM t.fh_autorizacion) != t.aa_certificado
    GROUP BY t.aa_certificado,
             t.t_certificado,
             t.n_certificado,
             TRUNC(t.fh_autorizacion),
             t.aa_ocompra,
             t.t_ocompra,
             t.n_ocompra,
             t.O_ENTE
    """
    return query_base.replace("'2015'", f"'{anio}'")


def construir_query_igual(anio, lista_beneficiarios):
    """
    Consulta base con la condición '=', reemplazando '2015' por el año indicado
    y filtrando SOLO los beneficiarios que aparecieron en la consulta !=.
    """
    # Convertir la lista de beneficiarios a un string para la cláusula IN
    # Ej: "'Bene1','Bene2','Bene3'"
    in_clause = ",".join(f"'{b}'" for b in lista_beneficiarios)

    query_base = f"""
    SELECT
        t.O_ENte AS Beneficiario,
        TO_CHAR(t.aa_devengado) AS aa_formulario,
        TO_CHAR(t.t_devengado) AS t_formulario,
        TO_CHAR(t.n_devengado) AS o_formulario,
        TRUNC(di.fh_imputacion) AS fh_imputacion,
        NULL AS aa_comprobante,
        NULL AS t_comprobante,
        NULL AS o_comprobante,
        SUM(di.i_devengado) AS i_devengado
    FROM gs_tdevengado t
    JOIN gs_ddevengado_ffi di ON t.o_devengado = di.o_devengado
    WHERE t.e_devengado = 'A'
      AND di.c_numcred IS NOT NULL
      AND t.aa_devengado = '{anio}'
      AND EXTRACT(YEAR FROM di.fh_imputacion) = t.aa_devengado
      AND t.O_ENte IN ({in_clause})
    GROUP BY t.aa_devengado,
             t.t_devengado,
             t.n_devengado,
             TRUNC(di.fh_imputacion),
             t.O_ENte

    UNION ALL

    SELECT
        t.O_ENte AS Beneficiario,
        TO_CHAR(t.aa_precepcion) AS aa_formulario,
        TO_CHAR(t.t_precepcion) AS t_formulario,
        TO_CHAR(t.n_precepcion) AS o_formulario,
        TRUNC(t.fh_imputacion) AS fh_imputacion,
        TO_CHAR(t.aa_ocompra) AS aa_comprobante,
        TO_CHAR(t.t_ocompra) AS t_comprobante,
        TO_CHAR(t.n_ocompra) AS o_comprobante,
        SUM(NVL(d.i_total, 0)) AS i_devengado
    FROM prd_tparte_recepcion t
    JOIN prd_dparte_recepcion_ffi d
      ON t.aa_precepcion = d.aa_precepcion
      AND t.t_precepcion = d.t_precepcion
      AND t.n_precepcion = d.n_precepcion
    WHERE t.e_formulario IN ('A')
      AND t.aa_precepcion = '{anio}'
      AND EXTRACT(YEAR FROM t.fh_imputacion) = t.aa_precepcion
      AND t.O_ENte IN ({in_clause})
    GROUP BY t.aa_precepcion,
             t.t_precepcion,
             t.n_precepcion,
             TRUNC(t.fh_imputacion),
             t.aa_ocompra,
             t.t_ocompra,
             t.n_ocompra,
             t.O_ENte

    UNION ALL

    SELECT
        t.O_CONTRATISTA AS Beneficiario,
        TO_CHAR(t.aa_certificado) AS aa_formulario,
        'CAO' AS t_formulario,
        TO_CHAR(t.o_certificado) AS o_formulario,
        TRUNC(t.f_certificacion) AS fh_imputacion,
        TO_CHAR(t.t_contrato) AS aa_comprobante,
        TO_CHAR(t.n_contrato) AS t_comprobante,
        TO_CHAR(t.aa_contrato) AS o_comprobante,
        SUM(d.i_devengado) AS i_devengado
    FROM obp_tcertificado_avance t
    JOIN obp_dcert_avance_ffi d 
      ON t.aa_certificado = d.aa_certificado
      AND t.t_form_medicion = d.t_form_medicion
      AND t.o_certificado = d.o_certificado
      AND t.c_obra = d.co_obra
      AND t.n_dev = d.n_dev
    WHERE t.e_certificado = 'A'
      AND t.aa_certificado = '{anio}'
      AND EXTRACT(YEAR FROM d.f_imputacion) = t.aa_certificado
      AND t.O_CONTRATISTA IN ({in_clause})
    GROUP BY t.aa_certificado,
             t.o_certificado,
             TRUNC(t.f_certificacion),
             t.t_contrato,
             t.n_contrato,
             t.aa_contrato,
             t.O_CONTRATISTA

    UNION ALL

    SELECT
        t.O_ENTE  AS Beneficiario,
        TO_CHAR(t.aa_certificado) AS aa_formulario,
        TO_CHAR(t.t_certificado) AS t_formulario,
        TO_CHAR(t.n_certificado) AS o_formulario,
        TRUNC(t.fh_autorizacion) AS fh_imputacion,
        TO_CHAR(t.aa_ocompra) AS aa_comprobante,
        TO_CHAR(t.t_ocompra) AS t_comprobante,
        TO_CHAR(t.n_ocompra) AS o_comprobante,
        SUM(d.i_devengado) AS i_devengado
    FROM obp_tcertificado t
    JOIN obp_dcertificado_ffi d 
      ON t.aa_certificado = d.aa_certificado
      AND t.t_certificado = d.t_certificado
      AND t.n_certificado = d.n_certificado
    WHERE t.e_certificado = 'A'
      AND t.aa_certificado = '{anio}'
      AND EXTRACT(YEAR FROM t.fh_autorizacion) = t.aa_certificado
      AND t.O_ENTE IN ({in_clause})
    GROUP BY t.aa_certificado,
             t.t_certificado,
             t.n_certificado,
             TRUNC(t.fh_autorizacion),
             t.aa_ocompra,
             t.t_ocompra,
             t.n_ocompra,
             t.O_ENTE

    UNION ALL

    SELECT
        t.O_ENTE AS Beneficiario,
        TO_CHAR(t.aa_certificado) AS aa_formulario,
        TO_CHAR(t.t_certificado) AS t_formulario,
        TO_CHAR(t.n_certificado) AS o_formulario,
        TRUNC(t.fh_autorizacion) AS fh_imputacion,
        TO_CHAR(t.aa_ocompra) AS aa_comprobante,
        TO_CHAR(t.t_ocompra) AS t_comprobante,
        TO_CHAR(t.n_ocompra) AS o_comprobante,
        SUM(d.i_devengado) AS i_devengado
    FROM slu.tanticipo_financiero t
    JOIN slu.danticipo_financiero_ffi d 
      ON t.aa_certificado = d.aa_ejervg
      AND t.o_certificado = d.o_certificado
    WHERE t.e_certificado = 'A'
      AND t.aa_certificado = '{anio}'
      AND EXTRACT(YEAR FROM t.fh_autorizacion) = t.aa_certificado
      AND t.O_ENTE IN ({in_clause})
    GROUP BY t.aa_certificado,
             t.t_certificado,
             t.n_certificado,
             TRUNC(t.fh_autorizacion),
             t.aa_ocompra,
             t.t_ocompra,
             t.n_ocompra,
             t.O_ENTE
    """
    return query_base


def main():
    # ---------------------------------------------------------------------
    # 1. Pedir al usuario el rango de años
    # ---------------------------------------------------------------------
    anio_inicial = int(input("Ingrese el año inicial: "))
    anio_final = int(input("Ingrese el año final: "))

    # ---------------------------------------------------------------------
    # 2. Configuración de la conexión (usando SID)
    # ---------------------------------------------------------------------
    host = "10.17.25.52"
    sid = "preprod"
    usuario = "SLU"
    contrasena = "fsugyt8h"

    dsn_tns = cx_Oracle.makedsn(host, 1521, sid=sid)
    try:
        connection = cx_Oracle.connect(user=usuario, password=contrasena, dsn=dsn_tns)
        print("Conexión establecida correctamente.")
    except cx_Oracle.Error as e:
        print(f"Error al conectar a la base de datos: {e}")
        return

    # ---------------------------------------------------------------------
    # 3. Preparar un ExcelWriter para generar un XLSX
    #    Tendremos 2 hojas por cada año: "AÑO_dif" y "AÑO_ig"
    # ---------------------------------------------------------------------
    output_file = "resultado_comparacion_por_anio.xlsx"
    writer = pd.ExcelWriter(output_file, engine='openpyxl')

    try:
        cursor = connection.cursor()

        for anio in range(anio_inicial, anio_final + 1):
            # -------------------------------------------------------------
            # 3.1. Ejecutar la consulta de "!="
            # -------------------------------------------------------------
            query_dif = construir_query_diferente(anio)
            cursor.execute(query_dif)
            rows_dif = cursor.fetchall()
            col_names_dif = [desc[0] for desc in cursor.description]

            df_dif = pd.DataFrame(rows_dif, columns=col_names_dif)
            # Ordenar por Beneficiario (si hay filas)
            if not df_dif.empty:
                df_dif.sort_values(by="BENEFICIARIO", ascending=True, inplace=True)

            # -------------------------------------------------------------
            # 3.2. Determinar la lista de beneficiarios
            #      para usarlos en la consulta de "="
            # -------------------------------------------------------------
            beneficiarios_unicos = df_dif["BENEFICIARIO"].unique().tolist()

            # -------------------------------------------------------------
            # 3.3. Ejecutar la consulta "="
            #      pero solo para los beneficiarios de la primera.
            # -------------------------------------------------------------
            if beneficiarios_unicos:
                query_ig = construir_query_igual(anio, beneficiarios_unicos)
                cursor.execute(query_ig)
                rows_ig = cursor.fetchall()
                col_names_ig = [desc[0] for desc in cursor.description]

                df_ig = pd.DataFrame(rows_ig, columns=col_names_ig)
                # Ordenar por Beneficiario
                if not df_ig.empty:
                    df_ig.sort_values(by="BENEFICIARIO", ascending=True, inplace=True)
            else:
                # Si no hubo beneficiarios en la primera, la segunda estará vacía
                df_ig = pd.DataFrame(columns=col_names_dif)

                # -------------------------------------------------------------
            # 3.4. Exportar cada DataFrame en su respectiva hoja
            # -------------------------------------------------------------
            sheet_name_dif = f"{anio}_dif"  # Hoja con registros !=
            sheet_name_ig = f"{anio}_ig"  # Hoja con registros =

            df_dif.to_excel(writer, sheet_name=sheet_name_dif, index=False)
            df_ig.to_excel(writer, sheet_name=sheet_name_ig, index=False)

            print(f"Año {anio}: != {len(df_dif)} registros, = {len(df_ig)} registros.")

    except cx_Oracle.Error as e:
        print(f"Error al ejecutar las consultas: {e}")
    finally:
        # Cerrar cursor y conexión
        cursor.close()
        connection.close()

        # Guardar el archivo XLSX
        writer.close()
        print(f"Archivo Excel generado: {output_file}")


# -------------------------------------------------------------------------
# Ejecución principal
# -------------------------------------------------------------------------
if __name__ == "__main__":
    main()