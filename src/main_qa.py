import src.database as db
import pandas as pd
import sqlalchemy as sa
import os

# Mantengo las conexiones para la lectura de datos
eng_geocom = db.get_connection('geocom_qa')
eng_dw = db.get_connection('dw')

def ejecutar_etl_excel(df_totals, IVA_RATE, modulos, ruta_salida='reporte_cierres.xlsx'):
    """
    Procesa la data y genera un Excel con múltiples hojas.
    """
    
    # 1. Inicializamos listas para acumular los datos de cada iteración
    acc_cabecera = []
    acc_detalles = []
    acc_precios = []
    acc_medios_pago = []
    acc_depositos = []
    acc_guias_cabecera = []
    acc_guias_detalle = []
    
    print(f">> Iniciando procesamiento para {len(df_totals)} cierres...")

    # Formateo de fechas masivo antes del loop (optimización)
    df_totals["opened_fmt"] = pd.to_datetime(df_totals["opened"]).dt.strftime('%Y%m%d')
    df_totals["closed_fmt"] = pd.to_datetime(df_totals["closed"]).dt.strftime('%Y%m%d')

    for index, row in df_totals.iterrows():
        # Variables de contexto
        localid = row.localid
        pos = row.pos
        ticketnumber_opened = row.ticketnumber_opened
        ticketnumber_closed = row.ticketnumber_closed
        opened_fmt = row.opened_fmt
        closed_fmt = row.closed_fmt
        z = row.znumber
        id_cierre = row.id # Renombramos para evitar conflicto con función id()
        
        print(f">> Procesando Cierre ID: {id_cierre} | Local: {localid}", flush=True)

        # ---------------------------------------------------------
        # BLOQUE VENTAS
        # ---------------------------------------------------------
        if "VENTAS" in modulos:
            # ... (Tu Query de Ventas original) ...
            query_ventas = f"""
                SELECT 
                    CAST(replace(convert(varchar, tickets.opendate, 101), '/', '') 
                        + replace(convert(varchar, tickets.opendate, 108), ':', '') AS varchar)
                        + '.' + CAST(tickets.ticketnumber AS varchar)
                        + '.' + CAST(tickets.localid AS varchar)
                        + '.' + CAST(tickets.pos AS varchar)
                        + '.' + CAST(ticketitems.item AS varchar) AS theCode,
                    tickets.ticketnumber, tickets.localid, tickets.pos, tickets.document,
                    ticketitems.item, ticketitems.description,
                    SUM(ticketitems.umsignedquantity) AS umquantity,
                    ticketitems.unitamount,
                    SUM(ticketitems.signednationalamount) AS amount,
                    tickets.invoiceType, tickets.documenttype,
                    SUM(tickets.rounded) AS rounded,
                    measures.id idmeasure, measures.name descripcion, measures.decimals
                FROM tickets
                INNER JOIN ticketitems ON tickets.opendate = ticketitems.opendate 
                    AND tickets.ticketnumber = ticketitems.ticketnumber 
                    AND tickets.localid = ticketitems.localid 
                    AND tickets.pos = ticketitems.pos
                JOIN measures ON measures.id = ticketitems.measure
                WHERE tickets.localid = {localid} AND tickets.pos = {pos}
                    AND tickets.ticketnumber BETWEEN {ticketnumber_opened} AND {ticketnumber_closed}
                    AND CAST(CONVERT(VARCHAR, tickets.opendate, 112) AS INT) BETWEEN {opened_fmt} AND {closed_fmt}
                    AND tickets.documenttype IN('sale', 'sale-cancel')
                    AND tickets.invoiceType IN ('TEL', 'ICAE')
                GROUP BY tickets.opendate, tickets.ticketnumber, tickets.localid, tickets.pos,
                        tickets.document, ticketitems.item, ticketitems.description,
                        ticketitems.unitamount, tickets.invoiceType, tickets.documenttype, 
                        measures.id, measures.name, measures.decimals
            """
            df_ventas = pd.read_sql_query(query_ventas, eng_geocom)

            # ... (Tu Query de Descuentos original) ...
            query_desc = f"""
                SELECT 
                    CAST(replace(convert(varchar, discounts.opendate, 101), '/', '') 
                        + replace(convert(varchar, discounts.opendate, 108), ':', '') AS varchar)
                        + '.' + CAST(discounts.ticketnumber AS varchar)
                        + '.' + CAST(discounts.localid AS varchar)
                        + '.' + CAST(discounts.pos AS varchar)
                        + '.' + CAST(discounts.item AS varchar) AS thecode,
                    SUM(discounts.discountamount) AS discountamount  
                FROM discounts
                WHERE discounts.localid = {localid} AND discounts.pos = {pos}
                    AND discounts.ticketnumber BETWEEN {ticketnumber_opened} AND {ticketnumber_closed}
                    AND CAST(CONVERT(VARCHAR, discounts.opendate, 112) AS INT) BETWEEN {opened_fmt} AND {closed_fmt}
                GROUP BY CAST(replace(convert(varchar, discounts.opendate, 101), '/', '') 
                        + replace(convert(varchar, discounts.opendate, 108), ':', '') AS varchar)
                        + '.' + CAST(discounts.ticketnumber AS varchar)
                        + '.' + CAST(discounts.localid AS varchar)
                        + '.' + CAST(discounts.pos AS varchar)
                        + '.' + CAST(discounts.item AS varchar)
            """
            df_discounts = pd.read_sql_query(query_desc, eng_geocom)

            # Lógica de Merge y Cálculos (Idéntica a tu código original)
            df_ventas_realizadas = df_ventas[df_ventas['documenttype'] == 'sale']
            df_ventas_canceladas = df_ventas[df_ventas['documenttype'] == 'sale-cancel']
            
            df_ventas_realizadas = pd.merge(df_ventas_realizadas, df_discounts, how='left', left_on='theCode', right_on='thecode')
            df_ventas_canceladas = pd.merge(df_ventas_canceladas, df_discounts, how='left', left_on='theCode', right_on='thecode')
            
            df_ventas_realizadas['discountamount'] = df_ventas_realizadas['discountamount'].fillna(0)
            df_ventas_realizadas['item_amount'] = df_ventas_realizadas['amount'] - df_ventas_realizadas['discountamount']
            df_ventas_realizadas.loc[df_ventas_realizadas["item_amount"] == 0, "item_amount"] = 1
            df_ventas_canceladas['discountamount'] = df_ventas_canceladas['discountamount'].fillna(0)

            # Preparación de datos consolidados
            df_bol = df_ventas_realizadas[df_ventas_realizadas['invoiceType'] == 'TEL']
            df_fac = df_ventas_realizadas[df_ventas_realizadas['invoiceType'] == 'ICAE']

            discountamount_sale = df_ventas_realizadas['discountamount'].sum() if not df_ventas_realizadas.empty else 0
            discountamount_sale_cancel = df_ventas_canceladas['discountamount'].sum() if not df_ventas_canceladas.empty else 0
            gross_amount_bol = df_bol['amount'].sum() - df_bol['discountamount'].sum()
            gross_amount_fac = df_fac['amount'].sum() - df_fac['discountamount'].sum()
            gross_amount_tot = gross_amount_bol + gross_amount_fac

            # Cálculos de impuestos finales
            net_amount_boleta = round(gross_amount_bol / (1 + IVA_RATE), 3)
            tax_amount_boleta = round(gross_amount_bol - net_amount_boleta, 3)
            net_amount_factura = round(gross_amount_fac / (1 + IVA_RATE), 3)
            tax_amount_factura = round(gross_amount_fac - net_amount_factura, 3)
            net_amount_total = round(gross_amount_tot / (1 + IVA_RATE), 3)
            tax_amount_total = round(gross_amount_tot - net_amount_total, 3)

            # Construcción de la Cabecera de Cierre
            data_cierre = {
                'id': id_cierre, 'localid': localid, 'pos': pos, 'opened': row.opened, 'closed': row.closed,
                'ticketnumberopened': ticketnumber_opened, 'ticketnumberclosed': ticketnumber_closed, 'z': z,
                'discountamountsale': discountamount_sale, 'discountamountsalecancel': discountamount_sale_cancel,
                'grossamountboleta': gross_amount_bol, 'taxamountboleta': tax_amount_boleta, 'netamountboleta': net_amount_boleta,
                'grossamountfactura': gross_amount_fac, 'taxamountfactura': tax_amount_factura, 'netamountfactura': net_amount_factura,
                'grossamounttotal': gross_amount_tot, 'taxamounttotal': tax_amount_total, 'netamounttotal': net_amount_total,
                'sendstate': '0', 'sendresponse': '0'
            }
            # ACUMULAR CABECERA
            acc_cabecera.append(data_cierre)

            # Procesamiento detalle producto a producto (Vectorizado donde sea posible sería mejor, pero mantengo tu lógica)
            detalles = []
            precios = []
            
            for product in df_ventas_realizadas.itertuples(index=False):
                net_unitamount = round(product.unitamount / (1 + IVA_RATE), 3)
                tax_unitamount = round(product.unitamount - net_unitamount, 3)
                net_amount = round(product.item_amount / (1 + IVA_RATE), 3)
                tax_amount = round(product.item_amount - net_amount, 3)
                tax_amount_discount = round(product.discountamount * IVA_RATE / (1 + IVA_RATE), 3)
                net_amount_discount = round(product.discountamount - tax_amount_discount, 3)

                product_detail = {
                    'id': id_cierre, 'localid': localid, 'pos': pos, 'opened': row.opened, 'closed': row.closed,
                    'ticketnumberopened': ticketnumber_opened, 'ticketnumberclosed': ticketnumber_closed, 'z': z,
                    'item': product.item, 'description': product.description,
                    'umquantity': product.umquantity, 'discountamount': product.discountamount,
                    'discounttaxamount': tax_amount_discount, 'discountnetamount': net_amount_discount,
                    'grossamount': product.item_amount, 'taxamount': tax_amount, 'netamount': net_amount,
                    'invoicetype': product.invoiceType
                }
                
                precio_product = {
                    'id': id_cierre, 'localid': localid, 'pos': pos, 'opened': row.opened, 'closed': row.closed,
                    'ticketnumberopened': ticketnumber_opened, 'ticketnumberclosed': ticketnumber_closed, 'z': z,
                    'item': product.item, 'description': product.description, 'idmeasure': product.idmeasure,
                    'descripcion': product.descripcion, 'decimals': product.decimals,
                    'unitamount': product.unitamount, 'netunitamount': net_unitamount,
                    'taxunitamount': tax_unitamount, 'discountamount': product.discountamount,
                }
                detalles.append(product_detail)
                precios.append(precio_product)

            if detalles:
                df_detalles = pd.DataFrame(detalles)
                df_precios = pd.DataFrame(precios)

                # Agrupaciones
                campos_grupo_detalle = ['id', 'localid', 'pos', 'opened', 'closed', 'ticketnumberopened', 'ticketnumberclosed', 'z', 'item', 'description', 'invoicetype']
                df_detalles_group = df_detalles.groupby(campos_grupo_detalle, as_index=False).sum(numeric_only=True).round(3)
                df_detalles_group = df_detalles_group[df_detalles_group['umquantity'] != 0]

                campos_grupo_precios = ['id', 'localid', 'pos', 'opened', 'closed', 'ticketnumberopened', 'ticketnumberclosed', 'z', 'item', 'description', 'idmeasure', 'descripcion', 'decimals']
                df_precios_group = df_precios.groupby(campos_grupo_precios, as_index=False).mean(numeric_only=True).round(3)

                # ACUMULAR DETALLES Y PRECIOS
                acc_detalles.append(df_detalles_group)
                acc_precios.append(df_precios_group)

        # ---------------------------------------------------------
        # BLOQUE MEDIOS PAGO
        # ---------------------------------------------------------
        if "MEDIOS_PAGO" in modulos:
            query_pagos = f"""
                SELECT paymentmodes.id paymentid, paymentmodes.name, ISNULL(payments.cardtypespdh4, '') AS cardtype,
                    invoiceType invoicetype, SUM(payments.amount) as grossamount
                FROM tickets (nolock)
                INNER JOIN payments ON tickets.opendate = payments.opendate AND tickets.localid = payments.localid
                    AND tickets.ticketnumber = payments.ticketnumber AND tickets.pos = payments.pos
                INNER JOIN paymentmodes ON payments.paymentmode = paymentmodes.id
                WHERE tickets.localid = {localid} AND tickets.pos = {pos}
                    AND ISNUMERIC(tickets.ticketnumber) = 1
                    AND CAST(tickets.ticketnumber AS INT) BETWEEN {ticketnumber_opened} AND {ticketnumber_closed}
                    AND CAST(CONVERT(VARCHAR, tickets.opendate, 112) AS INT) BETWEEN {opened_fmt} AND {closed_fmt}
                    AND paymentmodes.id NOT IN (40) AND tickets.documenttype = 'sale'
                GROUP BY paymentmodes.id, paymentmodes.name, payments.cardtypespdh4, tickets.invoiceType
            """
            df_medio_pago = pd.read_sql(query_pagos, eng_geocom)
            df_medio_pago = df_medio_pago[df_medio_pago['grossamount'] > 0].copy()

            if not df_medio_pago.empty:
                df_medio_pago['localid'] = localid
                df_medio_pago['id'] = id_cierre
                df_medio_pago['pos'] = pos
                df_medio_pago['opened'] = row.opened
                df_medio_pago['closed'] = row.closed
                df_medio_pago['ticketnumberopened'] = ticketnumber_opened
                df_medio_pago['ticketnumberclosed'] = ticketnumber_closed
                df_medio_pago['z'] = z
                df_medio_pago['sendstate'] = '0'
                df_medio_pago['sendresponse'] = '0'
                df_medio_pago['taxamount'] = round(df_medio_pago['grossamount'] * IVA_RATE / (1 + IVA_RATE), 3)
                df_medio_pago['netamount'] = round(df_medio_pago['grossamount'] - df_medio_pago['taxamount'], 3)
                
                # ACUMULAR MEDIOS PAGO
                acc_medios_pago.append(df_medio_pago)

        # ---------------------------------------------------------
        # BLOQUE REDONDEOS (Se trata como un medio de pago más)
        # ---------------------------------------------------------
        if "REDONDEOS" in modulos:
            query_redondeos = f"""
                SELECT tickets.localid, tickets.pos, SUM(tickets.rounded) AS grossamount, tickets.invoiceType invoicetype
                FROM tickets
                WHERE tickets.localid = {localid} AND tickets.pos = {pos}
                    AND tickets.ticketnumber BETWEEN {ticketnumber_opened} AND {ticketnumber_closed}
                    AND CAST(CONVERT(VARCHAR, tickets.opendate, 112) AS INT) BETWEEN {opened_fmt} AND {closed_fmt}
                    AND tickets.documenttype IN('sale') AND tickets.invoiceType IN ('TEL', 'ICAE')
                GROUP BY tickets.localid, tickets.pos, tickets.invoiceType
            """
            df_redondeos = pd.read_sql_query(query_redondeos, eng_geocom)
            
            if not df_redondeos.empty and df_redondeos['grossamount'].sum() != 0:
                df_redondeos['id'] = id_cierre
                df_redondeos['localid'] = localid
                df_redondeos['pos'] = pos
                df_redondeos['opened'] = row.opened
                df_redondeos['closed'] = row.closed
                df_redondeos['ticketnumberopened'] = ticketnumber_opened
                df_redondeos['ticketnumberclosed'] = ticketnumber_closed
                df_redondeos['z'] = z
                df_redondeos['sendstate'] = 0
                df_redondeos['sendresponse'] = 0
                df_redondeos['taxamount'] = 0
                df_redondeos['netamount'] = 0
                df_redondeos['name'] = 'REDONDEO'
                df_redondeos['cardtype'] = ''
                df_redondeos['paymentid'] = 0
                
                # ACUMULAR REDONDEOS (En la misma lista de medios de pago)
                acc_medios_pago.append(df_redondeos)

        # ---------------------------------------------------------
        # BLOQUE DEPOSITOS
        # ---------------------------------------------------------
        if "DEPOSITOS" in modulos:
            query_dep = f"""
                SELECT tickets.numerodeposito as folio, tickets.tipodeposito as type, tickets.montodeposito as amount 
                FROM tickets (nolock) 
                WHERE tickets.localid = {localid} AND tickets.pos = {pos} AND tickets.documenttype = 'deposit'
                    AND CAST(tickets.ticketnumber AS INT) BETWEEN {ticketnumber_opened} AND {ticketnumber_closed}
                    AND CAST(CONVERT(VARCHAR, tickets.opendate, 112) AS INT) BETWEEN {opened_fmt} AND {closed_fmt}
            """
            df_depositos = pd.read_sql(query_dep, eng_geocom)
            
            if not df_depositos.empty:
                df_depositos['id'] = id_cierre
                df_depositos['localid'] = localid
                df_depositos['pos'] = pos
                df_depositos['opened'] = row.opened
                df_depositos['closed'] = row.closed
                df_depositos['ticketnumberopened'] = ticketnumber_opened
                df_depositos['ticketnumberclosed'] = ticketnumber_closed
                df_depositos['z'] = z
                df_depositos['sendstate'] = 0
                df_depositos['sendresponse'] = 0
                
                # ACUMULAR DEPOSITOS
                acc_depositos.append(df_depositos)
        
        # ---------------------------------------------------------
        # BLOQUE GUIAS
        # ---------------------------------------------------------
        if "GUIAS" in modulos:
            query_guias = f"""
               SELECT tickets.localid, tickets.pos, sheets.thenumber as documentnumber, ticketitems.item,
                    ticketitems.umsignedquantity quantity, CAST(ticketitems.amount as int) AS amount,
                    CONVERT(date, tickets.opendate, 112) AS date, CONVERT(varchar(8), tickets.opendate, 108) AS hour,
                    dispatchguide.patent,
                    CASE WHEN dispatchguide.patent IN(2,6) THEN
                            CASE WHEN (SELECT id FROM geopos2server.dbo.DG_LocalDeliveryAddress d WHERE UPPER(d.address) = dispatchguide.deliveryAddress) IS NULL THEN 0
                            ELSE (SELECT id FROM geopos2server.dbo.DG_LocalDeliveryAddress d WHERE UPPER(d.address) = dispatchguide.deliveryAddress) END
                        ELSE tickets.localid END localiddestino
                FROM geopos2server.dbo.tickets (NOLOCK)
                INNER JOIN geopos2server.dbo.ticketitems (NOLOCK) ON tickets.opendate = ticketitems.opendate AND tickets.localid = ticketitems.localid AND tickets.pos = ticketitems.pos AND tickets.ticketnumber = ticketitems.ticketnumber
                INNER JOIN geopos2server.dbo.sheets (NOLOCK) ON tickets.pos = sheets.pos AND tickets.ticketnumber = sheets.ticketnumber AND tickets.opendate = sheets.opendate
                INNER JOIN geopos2server.dbo.dispatchguide (NOLOCK) ON tickets.pos = dispatchguide.pos AND tickets.ticketnumber = dispatchguide.ticketnumber AND tickets.opendate = dispatchguide.opendate
                WHERE tickets.localid = '{localid}' AND tickets.pos = '{pos}' AND tickets.documenttype = 'sale' AND tickets.invoiceType = 'DGE'
                    AND CAST(tickets.ticketnumber AS INT) > {ticketnumber_opened} AND CAST(tickets.ticketnumber AS INT) < {ticketnumber_closed}
                    AND CAST(CONVERT(VARCHAR, tickets.opendate, 112) AS INT) BETWEEN {opened_fmt} AND {closed_fmt}
            """
            df_guias = pd.read_sql(query_guias, eng_geocom)

            if not df_guias.empty:
                # Datos fijos
                df_guias['id'] = id_cierre
                df_guias['localid'] = localid
                df_guias['pos'] = pos
                df_guias['opened'] = row.opened
                df_guias['closed'] = row.closed
                df_guias['ticketnumberopened'] = ticketnumber_opened
                df_guias['ticketnumberclosed'] = ticketnumber_closed
                df_guias['z'] = z
                df_guias['sendstate'] = 0
                df_guias['sendresponse'] = 0
                
                # Separar cabecera y detalle
                cols_comunes = ['id', 'localid', 'pos', 'opened', 'closed', 'ticketnumberopened', 'ticketnumberclosed', 'z', 'documentnumber']
                
                df_cab = df_guias[cols_comunes + ['date', 'hour', 'patent', 'localiddestino', 'sendstate', 'sendresponse']].drop_duplicates()
                
                df_det = df_guias[cols_comunes + ['item', 'quantity', 'amount']].copy()
                df_det['netamount'] = round(df_det['amount'] / (1 + IVA_RATE), 3)
                df_det['taxamount'] = round(df_det['amount'] - df_det['netamount'], 3)
                
                # Agrupación detalle
                grp_cols = cols_comunes + ['item']
                df_det_group = df_det.groupby(grp_cols, as_index=False).sum(numeric_only=True).round(3)
                df_det_group = df_det_group[df_det_group['quantity'] != 0]
                df_det_group['position'] = df_det_group.reset_index().index + 1

                # ACUMULAR GUIAS
                acc_guias_cabecera.append(df_cab)
                acc_guias_detalle.append(df_det_group)

    # ---------------------------------------------------------
    # CONSOLIDACIÓN Y EXPORTACIÓN A EXCEL
    # ---------------------------------------------------------
    print(f"\n>> Generando archivo Excel en: {ruta_salida}")
    
    with pd.ExcelWriter(ruta_salida, engine='openpyxl') as writer:
        
        # 1. Cabecera Cierres
        if acc_cabecera:
            pd.DataFrame(acc_cabecera).to_excel(writer, sheet_name='Cierres_Cabecera', index=False)
        else:
            print("Warning: No data for Cabecera")

        # 2. Detalles Cierres
        if acc_detalles:
            pd.concat(acc_detalles, ignore_index=True).to_excel(writer, sheet_name='Cierres_Detalle', index=False)

        # 3. Precios
        if acc_precios:
            pd.concat(acc_precios, ignore_index=True).to_excel(writer, sheet_name='Cierres_Precios', index=False)

        # 4. Medios Pago (Incluye Redondeos)
        if acc_medios_pago:
            # Reordenamos columnas para que quede prolijo
            cols_order = ['id', 'localid', 'pos', 'opened', 'closed', 'ticketnumberopened', 'ticketnumberclosed', 'z',
                          'paymentid', 'name', 'cardtype', 'invoicetype', 'grossamount', 'taxamount', 'netamount', 'sendstate', 'sendresponse']
            df_final_pagos = pd.concat(acc_medios_pago, ignore_index=True)
            # Filtramos solo columnas existentes
            existing_cols = [c for c in cols_order if c in df_final_pagos.columns]
            df_final_pagos[existing_cols].to_excel(writer, sheet_name='Cierres_MediosPago', index=False)

        # 5. Depósitos
        if acc_depositos:
            pd.concat(acc_depositos, ignore_index=True).to_excel(writer, sheet_name='Cierres_Depositos', index=False)

        # 6. Guías
        if acc_guias_cabecera:
            pd.concat(acc_guias_cabecera, ignore_index=True).to_excel(writer, sheet_name='Guias_Cabecera', index=False)
        if acc_guias_detalle:
            pd.concat(acc_guias_detalle, ignore_index=True).to_excel(writer, sheet_name='Guias_Detalle', index=False)

    print(f">> Archivo Excel generado exitosamente.")


def procesar_cierres_excel(config, output_file='Cierres_Consolidados.xlsx'):
    LOCALID = config.get('localid', None)
    POS = config.get('pos', None)
    PAR_INI = config['fecha_ini']
    PAR_FIN = config['fecha_fin']
    IVA_RATE = config.get('iva_rate', 0.19)
    MODULOS = config.get('modulos', [])

    print(f">> [MODO EXCEL] Procesando cierres, fechas={PAR_INI}-{PAR_FIN}")
    
    query_cierres = f"""
        SELECT
            RIGHT(CAST(YEAR(curr.closed) AS VARCHAR), 2) + 
            RIGHT('0' + CAST(MONTH(curr.closed) AS VARCHAR), 2) + 
            RIGHT('0' + CAST(DAY(curr.closed) AS VARCHAR), 2) + 
            RIGHT('0' + CAST(DATEPART(HOUR, curr.closed) AS VARCHAR), 2) + 
            RIGHT('0' + CAST(DATEPART(MINUTE, curr.closed) AS VARCHAR), 2) + 
            RIGHT('0' + CAST(DATEPART(SECOND, curr.closed) AS VARCHAR), 2) + 
            CAST(curr.localid AS VARCHAR) + 
            CAST(curr.pos AS VARCHAR) AS id,
            curr.localid, curr.pos, curr.opened, curr.closed,
            COALESCE(prev.ticketsequencenumber, 0) AS ticketnumber_opened,
            COALESCE(curr.ticketsequencenumber, 0) AS ticketnumber_closed,
            curr.znumber, curr.subclass, '0' state
        FROM totals curr
        LEFT JOIN totals prev ON curr.localid = prev.localid AND curr.pos = prev.pos 
            AND prev.subclass = 'postotal'
            AND prev.opened = (SELECT MAX(opened) FROM totals WHERE localid = curr.localid AND pos = curr.pos AND subclass = 'postotal' AND opened < curr.opened)
        WHERE curr.subclass = 'postotal'
        {"AND curr.localid IN(" + str(LOCALID) +")" if LOCALID else ""}
        {"AND curr.pos = " + str(POS) if POS else ""}
        AND CAST(CONVERT(VARCHAR, curr.closed, 112) AS INT) BETWEEN {PAR_INI} AND {PAR_FIN}
        AND curr.localid BETWEEN 100 AND 999
        ORDER BY curr.localid, curr.pos, curr.opened
    """

    df_totals = pd.read_sql_query(query_cierres, eng_geocom)
    
    print(f">> Se encontraron un total de {len(df_totals)} cierres.\n")

    if df_totals.empty:
        print(">> No hay cierres para procesar. Saliendo...")
        return

    # OJO: He quitado el filtro de duplicados contra la base de datos DW (df_distinct)
    # porque al exportar a Excel usualmente queremos ver la data, exista o no en el DW.
    # Si quieres reactivar ese filtro, descomenta la lógica original.
    
    # Llamamos a la función ETL adaptada a Excel
    ejecutar_etl_excel(df_totals, IVA_RATE, MODULOS, ruta_salida=output_file)

    print(">> Proceso Excel finalizado.")