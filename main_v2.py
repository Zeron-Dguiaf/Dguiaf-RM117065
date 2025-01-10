import cx_Oracle
import pandas as pd

def construir_query(anio):
    """
    Reemplaza en la consulta original todas las apariciones de '2015' por el año indicado.
    Si necesitas mayor flexibilidad, podrías usar parámetros bind.
    """
    # Consulta base con '2015' en cada cláusula WHERE.
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
    # Reemplazamos '2015' por el año que se recibe como parámetro
    query_anio = query_base.replace("'2015'", f"'{anio}'")
    return query_anio

def main():
    # ---------------------------------------------------------------------
    # 1. Pedir al usuario el rango de años
    # ---------------------------------------------------------------------
    anio_inicial = int(input("Ingrese el año inicial: "))
    anio_final   = int(input("Ingrese el año final: "))

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
    # 3. Preparar un ExcelWriter para generar un XLSX con múltiples hojas
    # ---------------------------------------------------------------------
    output_file = "resultado_query_por_anio.xlsx"
    writer = pd.ExcelWriter(output_file, engine='openpyxl')

    try:
        cursor = connection.cursor()

        for anio in range(anio_inicial, anio_final + 1):
            # 3.1 Construimos la consulta para el año en curso
            sql_query = construir_query(anio)

            # 3.2 Ejecutar la consulta
            cursor.execute(sql_query)
            rows = cursor.fetchall()
            col_names = [desc[0] for desc in cursor.description]

            # 3.3 Crear DataFrame y ordenar por 'Beneficiario'
            df = pd.DataFrame(rows, columns=col_names)
            if not df.empty:
                # Ordenar por la columna "Beneficiario" (respetar mayúsculas):
                df.sort_values(by="BENEFICIARIO", ascending=True, inplace=True)

            # 3.4 Guardar en una hoja por cada año
            sheet_name = str(anio)
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            print(f"Año {anio}: {len(df)} registros exportados.")

    except cx_Oracle.Error as e:
        print(f"Error al ejecutar la consulta: {e}")
    finally:
        # Cerrar cursor y conexión
        cursor.close()
        connection.close()

        # Guardar el archivo XLSX
        writer.close()  # <-- cambiar save() por close()
        print(f"Archivo Excel generado: {output_file}")

# -------------------------------------------------------------------------
# Ejecución principal
# -------------------------------------------------------------------------
if __name__ == "__main__":
    main()