import src.database as db
import pandas as pd
import sqlalchemy as sa

# eng_geocom = db.get_connection('geocom')
eng_geocom = db.get_connection('geocom')
eng_dw = db.get_connection('dw')

def ejecutar_etl(df_totals, IVA_RATE, modulos):
    # for row in df_totals.itertuples(index=False):
    
    
    df_totals["opened_fmt"] = pd.to_datetime(df_totals["opened"]).dt.strftime('%Y%m%d')
    df_totals["closed_fmt"] = pd.to_datetime(df_totals["closed"]).dt.strftime('%Y%m%d')
    
    for index, row in df_totals.iterrows():
        index += 1
        # Extraer variables necesarias
        opened = row.opened
        closed = row.closed
        opened_fmt = row.opened_fmt
        closed_fmt = row.closed_fmt
        ticketnumber_opened = row.ticketnumber_opened
        ticketnumber_closed = row.ticketnumber_closed
        localid = row.localid
        pos = row.pos
        z = row.znumber
        id = row.id
        
        print(f"\n")
        print(f">> Ejecución en curso - Cierre: {index}", flush=True)
        print(f">> opened: {opened} - closed: {closed}", flush=True)
        print(f">> ticketnumber_opened: {ticketnumber_opened} - ticketnumber_closed: {ticketnumber_closed}", flush=True)
        print(f">> localid: {localid} - pos: {pos} - z: {z}", flush=True)
            
        if "VENTAS" in modulos:
            
            print(f">> Ejecutando Ventas", flush=True)
                        
            """VENTAS"""
            
            # Query de ventas
            df_ventas = pd.read_sql_query(f"""
                SELECT 
                    CAST(replace(convert(varchar, tickets.opendate, 101), '/', '') 
                        + replace(convert(varchar, tickets.opendate, 108), ':', '') AS varchar)
                        + '.' + CAST(tickets.ticketnumber AS varchar)
                        + '.' + CAST(tickets.localid AS varchar)
                        + '.' + CAST(tickets.pos AS varchar)
                        + '.' + CAST(ticketitems.item AS varchar) AS theCode,
                    tickets.ticketnumber,
                    tickets.localid,
                    tickets.pos,
                    tickets.document,
                    ticketitems.item,
                    ticketitems.description,
                    SUM(ticketitems.umsignedquantity) AS umquantity,
                    ticketitems.unitamount,
                    SUM(ticketitems.signednationalamount) AS amount,
                    tickets.invoiceType,
                    tickets.documenttype,
                    SUM(tickets.rounded) AS rounded,
                    measures.id idmeasure,
                    measures.name descripcion,
                    measures.decimals
                FROM tickets
                INNER JOIN ticketitems  
                    ON tickets.opendate = ticketitems.opendate  
                    AND tickets.ticketnumber = ticketitems.ticketnumber  
                    AND tickets.localid = ticketitems.localid  
                    AND tickets.pos = ticketitems.pos
                JOIN measures
   	                ON measures.id = ticketitems.measure
                WHERE tickets.localid = {localid}
                    AND tickets.pos = {pos}
                    AND tickets.ticketnumber BETWEEN {ticketnumber_opened} AND {ticketnumber_closed}
                    AND CAST(CONVERT(VARCHAR, tickets.opendate, 112) AS INT) BETWEEN {opened_fmt} AND {closed_fmt}
                    AND tickets.documenttype IN('sale', 'sale-cancel')
                    AND tickets.invoiceType IN ('TEL', 'ICAE')
                GROUP BY tickets.opendate, tickets.ticketnumber, tickets.localid, tickets.pos,
                        tickets.document, ticketitems.item, ticketitems.description,
                        ticketitems.unitamount, tickets.invoiceType, tickets.documenttype, 
                        measures.id, measures.name, measures.decimals
            """, eng_geocom)
            
            # Query de descuentos
            df_discounts = pd.read_sql_query(f"""
                SELECT 
                    CAST(replace(convert(varchar, discounts.opendate, 101), '/', '') 
                        + replace(convert(varchar, discounts.opendate, 108), ':', '') AS varchar)
                        + '.' + CAST(discounts.ticketnumber AS varchar)
                        + '.' + CAST(discounts.localid AS varchar)
                        + '.' + CAST(discounts.pos AS varchar)
                        + '.' + CAST(discounts.item AS varchar) AS thecode,
                    SUM(discounts.discountamount) AS discountamount  
                FROM discounts
                WHERE discounts.localid = {localid}
                    AND discounts.pos = {pos}
                    AND discounts.ticketnumber BETWEEN {ticketnumber_opened} AND {ticketnumber_closed}
                    AND CAST(CONVERT(VARCHAR, discounts.opendate, 112) AS INT) BETWEEN {opened_fmt} AND {closed_fmt}
                GROUP BY CAST(replace(convert(varchar, discounts.opendate, 101), '/', '') 
                        + replace(convert(varchar, discounts.opendate, 108), ':', '') AS varchar)
                        + '.' + CAST(discounts.ticketnumber AS varchar)
                        + '.' + CAST(discounts.localid AS varchar)
                        + '.' + CAST(discounts.pos AS varchar)
                        + '.' + CAST(discounts.item AS varchar)
            """, eng_geocom)
            
            # Unimos ventas y descuentos
            df_ventas_realizadas = df_ventas[df_ventas['documenttype'] == 'sale']
            df_ventas_canceladas = df_ventas[df_ventas['documenttype'] == 'sale-cancel']
            
            # Se realiza el merge de los descuentos a las ventas realizadas y canceladas
            df_ventas_realizadas = pd.merge(df_ventas_realizadas, df_discounts, how='left', left_on='theCode', right_on='thecode')
            df_ventas_canceladas = pd.merge(df_ventas_canceladas, df_discounts, how='left', left_on='theCode', right_on='thecode')
            
            # Reemplazar NaN en discountamount con 0
            df_ventas_realizadas['discountamount'] = df_ventas_realizadas['discountamount'].fillna(0)

            # Calculamos el monto por item
            df_ventas_realizadas['item_amount'] = df_ventas_realizadas['amount'] - df_ventas_realizadas['discountamount']
            
            df_ventas_realizadas.loc[df_ventas_realizadas["item_amount"] == 0, "item_amount"] = 1
            
            # Reemplazar NaN en discountamount con 0
            df_ventas_canceladas['discountamount'] = df_ventas_canceladas['discountamount'].fillna(0)

            # Debugging intermedio
            # print(f"\n>> Iteración {index} | localid={localid} | pos={pos} | tickets: {ticketnumber_opened}-{ticketnumber_closed} | z={z}")
                
            # Filtramos por tipo de documento (TEL y ICAE)
            df_bol = df_ventas_realizadas[df_ventas_realizadas['invoiceType'] == 'TEL']
            df_fac = df_ventas_realizadas[df_ventas_realizadas['invoiceType'] == 'ICAE']

            # Calculamos los valores consolidados
            discountamount_sale = df_ventas_realizadas['discountamount'].sum() if not df_ventas_realizadas.empty else 0
            discountamount_sale_cancel = df_ventas_canceladas['discountamount'].sum() if not df_ventas_canceladas.empty else 0
            gross_amount_bol = df_bol['amount'].sum() - df_bol['discountamount'].sum()
            gross_amount_fac = df_fac['amount'].sum() - df_fac['discountamount'].sum()
            gross_amount_tot = gross_amount_bol + gross_amount_fac

            detalles = []
            precios = []

            # Para cada producto (detalle de ventas)
            for product in df_ventas_realizadas.itertuples(index=False):
                # Crear la cabecera para este producto con la información agregada

                net_unitamount = round(product.unitamount / (1 + IVA_RATE), 3)  # Neto (sin IVA)
                tax_unitamount = round(product.unitamount - net_unitamount, 3)     # IVA calculado
                
                net_amount = round(product.item_amount / (1 + IVA_RATE), 3)  # Neto (sin IVA)
                tax_amount = round(product.item_amount - net_amount, 3)     # IVA calculado
                
                tax_amount_discount = round(product.discountamount * IVA_RATE / (1 + IVA_RATE), 3)  # IVA calculado del descuento
                net_amount_discount = round(product.discountamount - tax_amount_discount, 3)  # Neto del descuento

                product_detail = {
                    'id': id,
                    'localid': localid,
                    'pos': pos,
                    'opened': opened,
                    'closed': closed,
                    'ticketnumberopened': ticketnumber_opened,
                    'ticketnumberclosed': ticketnumber_closed,
                    'z': z,
                    'item': product.item,
                    'description': product.description,
                    'umquantity': product.umquantity,
                    'discountamount': product.discountamount,
                    'discounttaxamount': tax_amount_discount,  # IVA calculado del descuento
                    'discountnetamount': net_amount_discount,  # Neto del descuento
                    'grossamount': product.item_amount,  # Total con IVA
                    'taxamount': tax_amount,             # IVA calculado
                    'netamount': net_amount,             # Neto (sin IVA)
                    'invoicetype': product.invoiceType
                    # 'sendstate': '0',
                    # 'sendresponse': '0',
                }
                
                precio_product = {
                    'id': id,
                    'localid': localid,
                    'pos': pos,
                    'opened': opened,
                    'closed': closed,
                    'ticketnumberopened': ticketnumber_opened,
                    'ticketnumberclosed': ticketnumber_closed,
                    'z': z,
                    'item': product.item,
                    'description': product.description,
                    'idmeasure': product.idmeasure,
                    'descripcion': product.descripcion,
                    'decimals': product.decimals,
                    'unitamount': product.unitamount,
                    'netunitamount': net_unitamount,
                    'taxunitamount': tax_unitamount,
                    'discountamount': product.discountamount,
                }

                detalles.append(product_detail)
                precios.append(precio_product)                

            # Una sola escritura a base de datos
            df_detalles = pd.DataFrame(detalles)            
            df_precios = pd.DataFrame(precios)
            
            # Verificamos si hay datos para evitar errores
            if df_detalles.empty or df_precios.empty:
                print(f">> No hay datos de ventas para el cierre {index}. Saltando...")
                continue
            
            # Agrupamos los detalles y precios por campos comunes                        
            campos_grupo_detalle = [
                'id', 'localid', 'pos', 'opened', 'closed',
                'ticketnumberopened', 'ticketnumberclosed',
                'z', 'item', 'description', 'invoicetype'
            ]
            
            campos_grupo_precios = [
                'id', 'localid', 'pos', 'opened', 'closed',
                'ticketnumberopened', 'ticketnumberclosed',
                'z', 'item', 'description', 
                'idmeasure', 'descripcion', 'decimals'
            ]
            
            # Agrupamos los detalles y precios
            df_detalles_group = df_detalles.groupby(campos_grupo_detalle, as_index=False).agg({
                'umquantity': 'sum',
                'discountamount': 'sum',
                'grossamount': 'sum',
                'taxamount': 'sum',
                'netamount': 'sum',
                'discounttaxamount': 'sum',
                'discountnetamount': 'sum'
            }).round(3)
            
            # Eliminar registros donde umquantity es 0
            df_detalles_group = df_detalles_group[df_detalles_group['umquantity'] != 0]
            
            df_precios_group = df_precios.groupby(campos_grupo_precios, as_index=False).agg({
                'unitamount': 'mean',
                'netunitamount': 'mean',
                'taxunitamount': 'mean',
                'discountamount': 'mean'
            }).round(3)

            # Guardamos el detalle en la base de datos
            df_detalles_group.to_sql('cierres_detalle', eng_dw,if_exists='append', index=False, schema='modelo_ventas_rauco')
            
            # Guardamos los precios del detalle en la base de datos
            df_precios_group.to_sql('cierres_precio_producto', eng_dw,if_exists='append', index=False, schema='modelo_ventas_rauco')
            
            # Calculamos los montos totales para boleta y factura
            net_amount_boleta = round(gross_amount_bol / (1 + IVA_RATE), 3)  # Neto (sin IVA)
            tax_amount_boleta = round(gross_amount_bol - net_amount_boleta, 3)  # IVA calculado    
            
            net_amount_factura = round(gross_amount_fac / (1 + IVA_RATE), 3)  # Neto (sin IVA)
            tax_amount_factura = round(gross_amount_fac - net_amount_factura, 3)  # IVA calculado    
            
            net_amount_total = round(gross_amount_tot / (1 + IVA_RATE), 3)  # Neto (sin IVA)
            tax_amount_total = round(gross_amount_tot - net_amount_total, 3)  # IVA calculado    

            # Creamos fila final con todos los datos consolidados y el detalle de cierre
            data = {
                'id': id,
                'localid': localid,
                'pos': pos,
                'opened': opened,
                'closed': closed,
                'ticketnumberopened': ticketnumber_opened,
                'ticketnumberclosed': ticketnumber_closed,
                'z': z,
                'discountamountsale': discountamount_sale,
                'discountamountsalecancel': discountamount_sale_cancel,
                'grossamountboleta': gross_amount_bol,
                'taxamountboleta': tax_amount_boleta,  # IVA calculado
                'netamountboleta': net_amount_boleta,  # Neto (sin IVA)
                'grossamountfactura': gross_amount_fac,
                'taxamountfactura': tax_amount_factura,  # IVA calculado
                'netamountfactura': net_amount_factura,  # Neto (sin IVA)
                'grossamounttotal': gross_amount_tot,
                'taxamounttotal': tax_amount_total,  # IVA calculado
                'netamounttotal': net_amount_total,  # Neto (sin IVA)
                'sendstate': '0',
                'sendresponse': '0',
                # 'detalle_cierre': df_detalle_cierre  # Usamos la lista de detalles en lugar de DataFrame
            }

            # Convertimos el diccionario a un DataFrame
            df_cierre = pd.DataFrame([data])
            
            # Guardamos el cierre en la base de datos
            df_cierre.to_sql('cierres_cabecera', eng_dw, if_exists='append', index=False, schema='modelo_ventas_rauco')
             
        if "MEDIOS_PAGO" in modulos:
            
            print(f">> Ejecutando Medios de Pago", flush=True)
            
            # df_payments = pd.read_sql(f"""SELECT id FROM paymentmodes WHERE eliminated = 0 AND id NOT IN(40)""", eng_geocom)
            # payments_str = ','.join(map(str, df_payments['id'].tolist()))
            
            """MEDIOS DE PAGO"""
        
            query = f"""
                SELECT
                    paymentmodes.id paymentid,
                    paymentmodes.name,
                    ISNULL(payments.cardtypespdh4, '') AS cardtype,
                    invoiceType invoicetype,
                    SUM(payments.amount) as grossamount
                FROM tickets (nolock)
                INNER JOIN payments
                    ON tickets.opendate = payments.opendate
                    AND tickets.localid = payments.localid
                    AND tickets.ticketnumber = payments.ticketnumber
                    AND tickets.pos = payments.pos
                INNER JOIN paymentmodes
                    ON payments.paymentmode = paymentmodes.id
                WHERE tickets.localid = {localid}
                    AND tickets.pos = {pos}
                    AND ISNUMERIC(tickets.ticketnumber) = 1
                    AND CAST(tickets.ticketnumber AS INT) BETWEEN {ticketnumber_opened} AND {ticketnumber_closed}
                    AND CAST(CONVERT(VARCHAR, tickets.opendate, 112) AS INT) BETWEEN {opened_fmt} AND {closed_fmt}
                    AND paymentmodes.id NOT IN (40)
                    AND tickets.documenttype = 'sale'
                GROUP BY paymentmodes.id, paymentmodes.name, payments.cardtypespdh4, tickets.invoiceType
            """

            df_medio_pago = pd.read_sql(query, eng_geocom)

            # Filtrar filas con grossamount > 0
            df_medio_pago = df_medio_pago[df_medio_pago['grossamount'] > 0].copy()

            if df_medio_pago.empty:
                continue

            # Añadir columnas fijas
            df_medio_pago['localid'] = localid
            df_medio_pago['id'] = id
            df_medio_pago['pos'] = pos
            df_medio_pago['opened'] = opened
            df_medio_pago['closed'] = closed
            df_medio_pago['ticketnumberopened'] = ticketnumber_opened
            df_medio_pago['ticketnumberclosed'] = ticketnumber_closed
            df_medio_pago['z'] = z
            df_medio_pago['sendstate'] = '0'
            df_medio_pago['sendresponse'] = '0'

            # Calcular taxamount y netamount
            df_medio_pago['taxamount'] = round(df_medio_pago['grossamount'] * IVA_RATE / (1 + IVA_RATE), 3)
            df_medio_pago['netamount'] = round(df_medio_pago['grossamount'] - df_medio_pago['taxamount'], 3)

            # Ordenar columnas para insertar
            cols_order = [
                'id', 'localid', 'pos', 'opened', 'closed', 'ticketnumberopened', 'ticketnumberclosed', 'z',
                'paymentid', 'name', 'cardtype', 'invoicetype',
                'grossamount', 'taxamount', 'netamount',
                'sendstate', 'sendresponse'
            ]

            # Asegurar que todas las columnas existen antes de ordenar
            cols_existentes = [col for col in cols_order if col in df_medio_pago.columns]

            df_medio_pago = df_medio_pago[cols_existentes]

            df_medio_pago.to_sql('cierres_medio_pago', eng_dw, if_exists='append', index=False, schema='modelo_ventas_rauco')  
                     
        if "REDONDEOS" in modulos:
            
            print(f">> Ejecutando Redondeos", flush=True)
            
            # Query de ventas
            df_redondeos = pd.read_sql_query(f"""
                SELECT 
                    tickets.localid,
                    tickets.pos,
                    SUM(tickets.rounded) AS grossamount,
                    tickets.invoiceType invoicetype
                FROM tickets
                WHERE tickets.localid = {localid}
                    AND tickets.pos = {pos}
                    AND tickets.ticketnumber BETWEEN {ticketnumber_opened} AND {ticketnumber_closed}
                    AND CAST(CONVERT(VARCHAR, tickets.opendate, 112) AS INT) BETWEEN {opened_fmt} AND {closed_fmt}
                    AND tickets.documenttype IN('sale')
                    AND tickets.invoiceType IN ('TEL', 'ICAE')
                GROUP BY tickets.localid, tickets.pos, tickets.invoiceType
            """, eng_geocom)
            
            
            """REDONDEOS"""
            # Se agregan los redondeos para boletas y facturas como un nuevo medio de pago
                
            # Añadir columnas fijas
            df_redondeos['id'] = id
            df_redondeos['localid'] = localid
            df_redondeos['pos'] = pos
            df_redondeos['opened'] = opened
            df_redondeos['closed'] = closed
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

            # Ordenar columnas para insertar
            cols_order = [
                'id', 'localid', 'pos', 'opened', 'closed', 'ticketnumberopened', 'ticketnumberclosed', 'z',
                'paymentid', 'name', 'cardtype', 'invoicetype',
                'grossamount', 'taxamount', 'netamount',
                'sendstate', 'sendresponse'
            ]

            # Asegurar que todas las columnas existen antes de ordenar
            cols_existentes = [col for col in cols_order if col in df_redondeos.columns]

            df_redondeos = df_redondeos[cols_existentes]

            df_redondeos.to_sql('cierres_medio_pago', eng_dw, if_exists='append', index=False, schema='modelo_ventas_rauco')
            
        if "DEPOSITOS" in modulos:
            
            print(f">> Ejecutando Depositos", flush=True)
            
            """DEPOSITOS"""
            query = f"""
                SELECT
                    tickets.numerodeposito as folio, 
                    tickets.tipodeposito as type,
                    tickets.montodeposito as amount 
                FROM tickets (nolock) 
                WHERE tickets.localid = {localid}
                    AND tickets.pos = {pos}
                    AND tickets.documenttype = 'deposit'
                    AND CAST(tickets.ticketnumber AS INT) BETWEEN {ticketnumber_opened} AND {ticketnumber_closed}
                    AND CAST(CONVERT(VARCHAR, tickets.opendate, 112) AS INT) BETWEEN {opened_fmt} AND {closed_fmt}"""

            df_depositos = pd.read_sql(query, eng_geocom)

            if df_depositos.empty:
                continue

            # Añadir columnas fijas
            df_depositos['id'] = id
            df_depositos['localid'] = localid
            df_depositos['pos'] = pos
            df_depositos['opened'] = opened
            df_depositos['closed'] = closed
            df_depositos['ticketnumberopened'] = ticketnumber_opened
            df_depositos['ticketnumberclosed'] = ticketnumber_closed
            df_depositos['z'] = z
            df_depositos['sendstate'] = 0
            df_depositos['sendresponse'] = 0

            # Ordenar columnas para insertar
            cols_order = [
                'id', 'localid', 'pos', 'opened', 'closed', 'ticketnumberopened', 'ticketnumberclosed', 'z',
                'folio', 'type', 'amount', 
                'sendstate', 'sendresponse'
            ]

            # Asegurar que todas las columnas existen antes de ordenar
            cols_existentes = [col for col in cols_order if col in df_depositos.columns]

            df_depositos = df_depositos[cols_existentes]

            df_depositos.to_sql('cierres_depositos', eng_dw, if_exists='append', index=False, schema='modelo_ventas_rauco')

        if "GUIAS" in modulos:
    
            print(f">> Ejecutando Guias", flush=True)

            query = f"""
                SELECT
                    tickets.localid as localid,
                    tickets.pos as pos,
                    sheets.thenumber as documentnumber,
                    -- ticketitems.id as position,
                    ticketitems.item,
                    ticketitems.umsignedquantity quantity,
                    CAST(ticketitems.amount as int) AS amount,
                    CONVERT(date, tickets.opendate, 112) AS date,
                    CONVERT(varchar(8), tickets.opendate, 108) AS hour,
                    dispatchguide.patent,
                    CASE
                        WHEN dispatchguide.patent IN(2,6) THEN
                            CASE
                                WHEN (SELECT id FROM geopos2server.dbo.DG_LocalDeliveryAddress d WHERE UPPER(d.address) = dispatchguide.deliveryAddress) IS NULL THEN 0
                                ELSE (SELECT id FROM geopos2server.dbo.DG_LocalDeliveryAddress d WHERE UPPER(d.address) = dispatchguide.deliveryAddress)
                            END
                        ELSE tickets.localid
                    END localiddestino
                FROM geopos2server.dbo.tickets (NOLOCK)
                INNER JOIN geopos2server.dbo.ticketitems (NOLOCK)
                    ON tickets.opendate = ticketitems.opendate
                    AND tickets.localid = ticketitems.localid
                    AND tickets.pos = ticketitems.pos
                    AND tickets.ticketnumber = ticketitems.ticketnumber
                INNER JOIN geopos2server.dbo.sheets (NOLOCK)
                    ON tickets.pos = sheets.pos
                    AND tickets.ticketnumber = sheets.ticketnumber
                    AND tickets.opendate = sheets.opendate
                INNER JOIN geopos2server.dbo.dispatchguide (NOLOCK)
                    ON tickets.pos = dispatchguide.pos
                    AND tickets.ticketnumber = dispatchguide.ticketnumber
                    AND tickets.opendate = dispatchguide.opendate
                WHERE tickets.localid = '{localid}'
                    AND tickets.pos = '{pos}'
                    AND tickets.documenttype = 'sale'
                    AND tickets.invoiceType = 'DGE'
                    AND CAST(tickets.ticketnumber AS INT) > {ticketnumber_opened}
                    AND CAST(tickets.ticketnumber AS INT) < {ticketnumber_closed}                    
                    AND CAST(CONVERT(VARCHAR, tickets.opendate, 112) AS INT) BETWEEN {opened_fmt} AND {closed_fmt}
            """

            df_guias = pd.read_sql(query, eng_geocom)

            if df_guias.empty:
                continue

            # Añadir columnas fijas
            df_guias['id'] = id
            df_guias['localid'] = localid
            df_guias['pos'] = pos
            df_guias['opened'] = opened
            df_guias['closed'] = closed
            df_guias['ticketnumberopened'] = ticketnumber_opened
            df_guias['ticketnumberclosed'] = ticketnumber_closed
            df_guias['z'] = z
            df_guias['sendstate'] = 0
            df_guias['sendresponse'] = 0

            # Definir columnas fijas y adicionales para cada tabla
            columnas_comunes = [
                'id', 'localid', 'pos', 'opened', 'closed',
                'ticketnumberopened', 'ticketnumberclosed', 'z', 'documentnumber'
            ]

            columnas_guias = columnas_comunes + ['date', 'hour', 'patent', 'localiddestino', 'sendstate', 'sendresponse']
            columnas_guias_detalle = columnas_comunes + ['item', 'quantity', 'amount']

            # Separar en cabecera y detalle
            df_guias_cabecera = df_guias[columnas_guias].drop_duplicates()
            df_guias_detalle = df_guias[columnas_guias_detalle]
            
            df_guias_detalle = df_guias_detalle.copy()
            df_guias_detalle['netamount'] = round(df_guias_detalle['amount'] / (1 + IVA_RATE), 3)  # Neto (sin IVA)
            df_guias_detalle['taxamount'] = round(df_guias_detalle['amount'] - df_guias_detalle['netamount'], 3)  # IVA calculado
            
            
            # Agrupamos los detalles y precios por campos comunes                        
            campos_grupo_detalle = [
                'id', 'localid', 'pos', 'opened', 'closed',
                'ticketnumberopened', 'ticketnumberclosed',
                'z', 'documentnumber', 'item'
                # 'amount', 'netamount', 'taxamount'
            ]
            
            # Agrupamos los detalles y precios
            df_guias_detalle = df_guias_detalle.groupby(campos_grupo_detalle, as_index=False).agg({
                'quantity': 'sum',
                'amount': 'sum',
                'netamount': 'sum',
                'taxamount': 'sum'
            }).round(3)
            
            # Eliminar registros donde quantity es 0
            df_guias_detalle = df_guias_detalle[df_guias_detalle['quantity'] != 0]
            
            df_guias_detalle['position'] = df_guias_detalle.reset_index().index + 1            

            # Guardar en la base de datos
            df_guias_cabecera.to_sql(
                'cierres_guias',
                eng_dw,
                if_exists='append',
                index=False,
                schema='modelo_ventas_rauco'
            )

            df_guias_detalle.to_sql(
                'cierres_guias_detalle',
                eng_dw,
                if_exists='append',
                index=False,
                schema='modelo_ventas_rauco'
            )

        
        print(f">> Ejecución finalizada.", flush=True)
        
def procesar_cierres(config):
    LOCALID = config.get('localid', None)
    POS = config.get('pos', None)  # opcional
    PAR_INI = config['fecha_ini']
    PAR_FIN = config['fecha_fin']
    IVA_RATE = config.get('iva_rate', 0.19)
    MODULOS = config.get('modulos', [])

    # --- CIERRES ---
    print(f">> Procesando cierres, fechas={PAR_INI}-{PAR_FIN}")
    
    if POS != None:
        print(f">> POS={POS}")
        
    if LOCALID != None:
        print(f">> LOCALID={LOCALID}")
    
    query_cierres = f"""
        SELECT
            RIGHT(CAST(YEAR(curr.closed) AS VARCHAR), 2) + -- YY
            RIGHT('0' + CAST(MONTH(curr.closed) AS VARCHAR), 2) + -- MM
            RIGHT('0' + CAST(DAY(curr.closed) AS VARCHAR), 2) + -- DD
            RIGHT('0' + CAST(DATEPART(HOUR, curr.closed) AS VARCHAR), 2) + -- HH
            RIGHT('0' + CAST(DATEPART(MINUTE, curr.closed) AS VARCHAR), 2) + -- MM
            RIGHT('0' + CAST(DATEPART(SECOND, curr.closed) AS VARCHAR), 2) + -- SS
            CAST(curr.localid AS VARCHAR) + -- TIE
            CAST(curr.pos AS VARCHAR) AS id,
            curr.localid,
            curr.pos,
            curr.opened,
            curr.closed,
            COALESCE(prev.ticketsequencenumber, 0) AS ticketnumber_opened,
            COALESCE(curr.ticketsequencenumber, 0) AS ticketnumber_closed,
            curr.znumber,
            curr.subclass,
            '0' state
        FROM totals curr
        LEFT JOIN totals prev
            ON curr.localid = prev.localid
            AND curr.pos = prev.pos
            AND prev.subclass = 'postotal'
            AND prev.opened = (
                SELECT MAX(opened)
                FROM totals
                WHERE localid = curr.localid    
                AND pos = curr.pos
                AND subclass = 'postotal'
                AND opened < curr.opened
            )
        WHERE curr.subclass = 'postotal'
        {"AND curr.localid IN(" + str(LOCALID) +")" if LOCALID else ""}
        {"AND curr.pos = " + str(POS) if POS else ""}
        AND CAST(CONVERT(VARCHAR, curr.closed, 112) AS INT) BETWEEN {PAR_INI} AND {PAR_FIN}
        AND curr.localid BETWEEN 100 AND 999
        ORDER BY curr.localid, curr.pos, curr.opened
    """

    df_totals = pd.read_sql_query(query_cierres, eng_geocom)
    
    query_distinct = f"""
        select distinct id
        from DWCASTANO.modelo_ventas_rauco.cierres c 
        where CAST(CONVERT(VARCHAR, closed, 112) AS INT) BETWEEN {PAR_INI} AND {PAR_FIN}
        ;
    """
    
    df_distinct = pd.read_sql_query(query_distinct, eng_dw)
    
    print(f">> Se encontraron un total de {len(df_totals)} cierres pre-filtro de duplicados.\n")
    
    df_dup = df_totals[df_totals['id'].isin(df_distinct['id'].tolist())]

    print(f">> Cierres ya procesados: {len(df_dup)}. Se eliminan de la lista de cierres a procesar.\n")

    df_totals = df_totals[~df_totals['id'].isin(df_distinct['id'].tolist())]
    
    print(f">> Se encontraron un total de {len(df_totals)} cierres para los filtros aplicados.\n")

    if df_totals.empty:
        print(">> No hay cierres para procesar. Saliendo...")
        return
    
    # Guardamos el detalle en la base de datos
    df_totals.to_sql('cierres', eng_dw,if_exists='append', index=False, schema='modelo_ventas_rauco')
    
    ejecutar_etl(df_totals, IVA_RATE, MODULOS)

    print(">> Proceso finalizado.")