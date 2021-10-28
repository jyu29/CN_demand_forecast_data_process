-- OFFLINE
SELECT TOP 100 sum(tdt.f_qty_item)
FROM cds.f_transaction_detail_current tdt
inner join cds.d_sku sku on sku.sku_idr_sku = tdt.sku_idr_sku
inner join cds.d_business_unit but on but.but_idr_business_unit = tdt.but_idr_business_unit
inner join cds_supply.sites_attribut_0plant_branches sapb on ltrim(sapb.plant_id, '0') = ''||but.but_num_business_unit and but.but_num_typ_but = 7
where sapb.purch_org IN ('Z001','Z002','Z003','Z004','Z005','Z006','Z008','Z011','Z012','Z013','Z017','Z019','Z022','Z025','Z026','Z027','Z028','Z042','Z060','Z061','Z065',
'Z066','Z078','Z091','Z093','Z094','Z095','Z096','Z098','Z101','Z102','Z104','Z105','Z106','Z107','Z112','Z115')
and sku.unv_num_univers NOT IN (0,14,89,90)
and sapb.sapsrc = 'PRT'
and tdt.tdt_date_to_ordered between '2021-10-17 00:00:00' and '2021-10-23 23:59:59'
and tdt.the_to_type = 'offline'
;
-- ONLINE
SELECT TOP 100 sum(dyd.f_qty_item)
FROM cds.f_delivery_detail_current dyd
inner join cds.d_sku sku on sku.sku_idr_sku = dyd.sku_idr_sku
inner join cds.d_business_unit but on but.but_idr_business_unit = dyd.but_idr_business_unit_sender
inner join cds_supply.d_general_data_customer gdc on ''||but.but_code_international = gdc.ean_1||gdc.ean_2||gdc.ean_3
inner join cds_supply.sites_attribut_0plant_branches sapb on ltrim(sapb.plant_id, '0') = ''||but.but_num_business_unit
where sapb.purch_org IN ('Z001','Z002','Z003','Z004','Z005','Z006','Z008','Z011','Z012','Z013','Z017','Z019','Z022','Z025','Z026','Z027','Z028','Z042','Z060','Z061','Z065',
'Z066','Z078','Z091','Z093','Z094','Z095','Z096','Z098','Z101','Z102','Z104','Z105','Z106','Z107','Z112','Z115')
and sapb.sapsrc = 'PRT'
and gdc.sapsrc = 'PRT'
and sku.unv_num_univers NOT IN (0,14,89,90)
and dyd.tdt_date_to_ordered between '2021-10-17 00:00:00' and '2021-10-23 23:59:59'
and dyd.the_to_type = 'online'
and dyd.tdt_type_detail = 'sale'
;