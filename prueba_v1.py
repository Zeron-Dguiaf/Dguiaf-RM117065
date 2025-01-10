import pandas as pd


def style_subtotal_rows(styler):
    def highlight_subtotal(row):
        if row["BENEFICIARIO"] == "subtotal":
            return ["background-color: green"] * len(row)  # color verde
        return ["" for _ in row]

    # 1) Pintar fila subtotal en verde
    styler = styler.apply(highlight_subtotal, axis=1)

    # 2) Formato monetario en i_devengado, i_pagado, ia_pago
    for col in ["i_devengado", "i_pagado", "ia_pago"]:
        if col in styler.columns:
            styler.format({col: "${:,.2f}"})
    return styler


df_test = pd.DataFrame({
    "BENEFICIARIO": ["A", "A", "subtotal", "B", "B", "subtotal"],
    "i_devengado": [1000, 2500, 3500, 4000, 5000, 9000],
    "i_pagado": [500, 700, 1200, 1500, 1000, 2500],
    "ia_pago": [None, None, None, 300, 700, 1000]
})

writer = pd.ExcelWriter("test_styler.xlsx", engine="openpyxl")
df_test_styler = df_test.style.pipe(style_subtotal_rows)
df_test_styler.to_excel(writer, sheet_name="Test", index=False)
writer.close()