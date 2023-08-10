/* 1.Lista coletelor organizată pe transporturi efectuate în perioada 1.01-15.01.2018. Ce colete conține fiecare transport efectuat în data de 1.01-15.01.2018 */
WITH RECURSIVE i_t(id_transport,id_colet, nr_crt,lista_colete) AS(
SELECT col.id_transport,col.id_colet,1, CAST('\1-'||continut_colet AS VARCHAR(500))
FROM col
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN transporturi t ON col.id_transport=t.id_transport
WHERE nr_crt=1
UNION ALL
SELECT i_t.id_transport,i_t.id_colet,i_t.nr_crt + 1,CAST(i_t.lista_colete||' \'||i_t.nr_crt+1||'-'||c.continut_colet AS VARCHAR(500))
FROM col
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN i_t ON col.id_transport=i_t.id_transport
AND col.nr_crt = i_t.nr_crt+ 1),
col AS (
SELECT d_t.*,t.data_plecare,ROW_NUMBER()OVER(PARTITION BY d_t.id_transport ORDER BY d_t.id_colet) AS nr_crt
FROM detalii_transport d_t 
INNER JOIN transporturi t ON d_t.id_transport=t.id_transport
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND 
EXTRACT (MONTH FROM data_plecare)=1 AND
EXTRACT (DAY FROM data_plecare) BETWEEN 1 AND 15)
SELECT id_transport,MAX(nr_crt) AS numar_colete, MAX(lista_colete) AS lista_colete
FROM i_t
GROUP BY id_transport
ORDER BY 1;

/* 2.Lista coletelor organizată pe facturi întocmite în perioada 01.01-30.01.2018. Ce colete conține fiecare factură întocmită în perioada 01.01-30.01.2018? */
WITH RECURSIVE l_f(nr_factura, nr_crt, id_colet, lista_colete) AS(
SELECT col.nr_factura,1,col.id_colet, CAST('\1-'||continut_colet AS VARCHAR(500))
FROM col
INNER JOIN colete c ON col.id_colet=c.id_colet
WHERE nr_crt=1
UNION ALL
SELECT l_f.nr_factura,l_f.nr_crt + 1,l_f.id_colet,CAST(l_f.lista_colete||' \'||l_f.nr_crt+1||'-'||c.continut_colet AS VARCHAR(500))
FROM col
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN l_f ON col.nr_factura=l_f.nr_factura
AND col.nr_crt=l_f.nr_crt+ 1),
col AS (
SELECT d_f.*,f.data,ROW_NUMBER()OVER(PARTITION BY d_f.nr_factura ORDER BY d_f.id_colet) AS nr_crt
FROM detalii_facturi d_f
INNER JOIN facturi f ON d_f.nr_factura=f.nr_factura
WHERE EXTRACT (YEAR FROM data)=2018 AND 
EXTRACT (MONTH FROM data)=1 AND
EXTRACT (DAY FROM data) BETWEEN 1 AND 30)
SELECT nr_factura,MAX(nr_crt) AS numar_colete, MAX(lista_colete) AS lista_colete
FROM l_f
GROUP BY nr_factura
ORDER BY 1;

/* 3.Lista coletelor expediate de către fiecare expeditor în parte în perioada 01.01-08.01.2018. Ce colete a expediat fiecare expeditor în parte în perioada 01.01-08.01.2018? */
WITH RECURSIVE c_e(id_expeditor, expeditor, id_colet, nr_crt, lista_colete)AS(
SELECT col.id_expeditor, CAST(CASE WHEN e.prenume IS NOT NULL THEN e.nume||' '||e.prenume ELSE e.nume END AS VARCHAR(500)), col.id_colet, 1, CAST(' \'||col.id_colet||': '||col.continut_colet AS VARCHAR(500))
FROM col 
INNER JOIN expeditori e ON col.id_expeditor=e.id_expeditor
WHERE nr_crt=1
UNION ALL
SELECT c_e.id_expeditor,c_e.expeditor,c_e.id_colet,c_e.nr_crt+1,CAST(c_e.lista_colete||' \'||col.id_colet||'-'||col.continut_colet AS VARCHAR(500))
FROM col
INNER JOIN expeditori e ON col.id_expeditor=e.id_expeditor
INNER JOIN c_e ON col.id_expeditor=c_e.id_expeditor
AND col.nr_crt=c_e.nr_crt+ 1),
col AS (
SELECT c.*, ROW_NUMBER()OVER(PARTITION BY id_expeditor ORDER BY id_colet) AS nr_crt
FROM colete c
WHERE EXTRACT (YEAR FROM data_expediere)=2018 AND 
EXTRACT (MONTH FROM data_expediere)=1 AND
EXTRACT (DAY FROM data_expediere) BETWEEN 1 AND 8)
SELECT id_expeditor,expeditor,MAX(nr_crt) AS numar_colete, MAX(lista_colete) AS lista_colete
FROM c_e
GROUP BY id_expeditor,expeditor
ORDER BY 1;

/* 4.Lista coletelor primite de către fiecare destinatar în perioada 15.02-30.02.2018. Ce colete a primit fiecare destinatar în perioada 15.02-30.02.2018 */
WITH RECURSIVE c_d(id_destinatar, destinatar, id_colet, nr_crt, lista_colete)AS(
SELECT col.id_destinatar, CAST(CASE WHEN d.prenume IS NOT NULL THEN d.nume||' '||d.prenume ELSE d.nume END AS VARCHAR(500)), col.id_colet, 1, CAST(' \'||col.id_colet||': '||col.continut_colet AS VARCHAR(500))
FROM col 
INNER JOIN destinatari d ON col.id_destinatar=d.id_destinatar
WHERE nr_crt=1
UNION ALL
SELECT c_d.id_destinatar,c_d.destinatar,c_d.id_colet,c_d.nr_crt+1,CAST(c_d.lista_colete||' \'||col.id_colet||'-'||col.continut_colet AS VARCHAR(500))
FROM col
INNER JOIN destinatari d ON col.id_destinatar=d.id_destinatar
INNER JOIN c_d ON col.id_destinatar=c_d.id_destinatar
AND col.nr_crt=c_d.nr_crt+ 1),
col AS (
SELECT c.*, ROW_NUMBER()OVER(PARTITION BY id_destinatar ORDER BY id_colet) AS nr_crt
FROM colete c
WHERE EXTRACT (YEAR FROM data_primire_colet)=2018 AND 
EXTRACT (MONTH FROM data_primire_colet)=2 AND
EXTRACT (DAY FROM data_primire_colet) BETWEEN 15 AND 30)
SELECT id_destinatar,destinatar,MAX(nr_crt) AS numar_colete, MAX(lista_colete) AS lista_colete
FROM c_d
GROUP BY id_destinatar,destinatar
ORDER BY 1;

/* 5.Lista coletelor ce au plecat din fiecare depozit în perioada 01.10-06.10.2018. Ce colete au plecat din fiecare depozit în perioada 01.10-06.10.2018 */
WITH RECURSIVE col_dep(id_depozit_1,depozit, nr_crt, colete) AS(
SELECT col.id_depozit_1,CAST('Depozitul '||col.id_depozit_1||' ('||d.adresa||')' AS VARCHAR(500)),1, CAST ('\1: ' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN depozite d ON col.id_depozit_1=d.id_depozit
WHERE nr_crt=1
UNION ALL
SELECT col.id_depozit_1,col_dep.depozit,col_dep.nr_crt + 1,CAST (col_dep.colete || ' \' || col_dep.nr_crt + 1 || ' :' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN depozite d ON col.id_depozit_1=d.id_depozit
INNER JOIN col_dep ON col.id_depozit_1=col_dep.id_depozit_1
AND col.nr_crt = col_dep.nr_crt+ 1),
col AS (
SELECT t.*, d_t.id_colet,ROW_NUMBER()OVER(PARTITION BY id_depozit_1 ORDER BY id_colet) AS nr_crt 
FROM transporturi t 
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport 
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND
EXTRACT (MONTH FROM data_plecare)=10 AND 
EXTRACT (DAY FROM data_plecare) BETWEEN 1 AND 6)
SELECT depozit,MAX(nr_crt) AS nr_colete, MAX(colete) AS lista_colete
FROM col_dep
GROUP BY id_depozit_1,depozit
ORDER BY id_depozit_1;

/* 6.Lista coletelor transportate din depozit în depozit în perioada 15.06-18.06.2018. Ce colete au fost transportate din depozit în depozit în perioada 15.06-20.06.2018 */
WITH RECURSIVE col_dep2(id_depozit_1,id_depozit_2,depozite, nr_crt, colete) AS(
SELECT col.id_depozit_1,col.id_depozit_2,CAST('Depozitul '||col.id_depozit_1||' -> '||'Depozitul '||col.id_depozit_2 AS VARCHAR(500)),1, CAST ('\1-' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
WHERE nr_crt=1
UNION ALL
SELECT col.id_depozit_1,col.id_depozit_2,col_dep2.depozite,col_dep2.nr_crt + 1,CAST (col_dep2.colete || ' \' || col_dep2.nr_crt + 1 || '-' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN col_dep2 ON col.id_depozit_1=col_dep2.id_depozit_1 AND col.id_depozit_2=col_dep2.id_depozit_2
AND col.nr_crt = col_dep2.nr_crt+ 1),
col AS (
SELECT t.*,d_t.id_colet,ROW_NUMBER()OVER(PARTITION BY id_depozit_1,id_depozit_2 ORDER BY id_colet) AS nr_crt
FROM transporturi t 
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport 
WHERE id_depozit_2 IS NOT NULL AND 
EXTRACT (YEAR FROM data_plecare)=2018 AND
EXTRACT (MONTH FROM data_plecare)=06 AND
EXTRACT (DAY FROM data_plecare) BETWEEN 15 AND 20)
SELECT depozite,MAX(nr_crt) AS nr_colete, MAX(colete) AS lista_colete
FROM col_dep2
GROUP BY id_depozit_1,id_depozit_2,depozite
ORDER BY id_depozit_1;

/* 7.Lista transporturilor efectuate de catre feicare șofer în parte în lunile Ianuarie și Februarie 2018. Ce transporturi a efectuat fiecare șofer în lunile Ianuarie și Februarie 2018? */
WITH RECURSIVE tr_s(id_sofer, id_angajat, id_transport,sofer, nr_crt, transporturi) AS(
SELECT tran.id_sofer, a.id_angajat,tran.id_transport, CAST(a.nume||' '||a.prenume AS VARCHAR(500)), 1, CAST('\1: '||tran.id_transport||'('||tran.data_plecare||')' AS VARCHAR(500))
FROM tran 
INNER JOIN soferi s ON tran.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
WHERE nr_crt=1
UNION ALL
SELECT tr_s.id_sofer,tr_s.id_angajat,tr_s.id_transport,tr_s.sofer,tr_s.nr_crt + 1,CAST (tr_s.transporturi|| ' \' || tr_s.nr_crt + 1 ||tran.id_transport||'('||tran.data_plecare||')' AS VARCHAR(500))
FROM tran 
INNER JOIN soferi s ON tran.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
INNER JOIN tr_s ON tran.id_sofer=tr_s.id_sofer
AND tran.nr_crt = tr_s.nr_crt+ 1),
tran AS (
SELECT t.*,ROW_NUMBER()OVER(PARTITION BY id_sofer ORDER BY id_transport) AS nr_crt
FROM transporturi t
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND 
EXTRACT (MONTH FROM data_plecare) BETWEEN 1 AND 2)
SELECT id_sofer,sofer,MAX(nr_crt) AS nr_transporturi, MAX(transporturi) AS lista_transporturi
FROM tr_s
GROUP BY id_sofer,sofer
ORDER BY id_sofer;

/* 8.Lista transporturilor efectuate cu fiecare mijloc de transport în perioada xx.01-xx.04.2018. Ce transporturi au fost efectuate cu fiecare mijloc de transport în perioada xx.01-xx.04.2018? */
WITH RECURSIVE tr_v(id_vehicul, model, id_transport,linie, transporturi) AS(
SELECT tran.id_vehicul, CAST(v.model||' ('||v.nr_inmatriculare||')' AS VARCHAR(500)),tran.id_transport,1,CAST('\1: '||tran.id_transport||'('||tran.data_plecare||')' AS VARCHAR(500))
FROM tran 
INNER JOIN vehicule v ON tran.id_vehicul=v.id_vehicul
WHERE linie=1
UNION ALL
SELECT tr_v.id_vehicul,tr_v.model,tr_v.id_transport,tr_v.linie + 1,CAST (tr_v.transporturi|| ' \' || tr_v.linie + 1 ||tran.id_transport||'('||tran.data_plecare||')' AS VARCHAR(500))
FROM tran 
INNER JOIN vehicule v ON tran.id_vehicul=v.id_vehicul
INNER JOIN tr_v ON tran.id_vehicul=tr_v.id_vehicul
AND tran.linie = tr_v.linie+ 1),
tran AS (
SELECT t.*,ROW_NUMBER()OVER(PARTITION BY id_vehicul ORDER BY id_transport) AS linie
FROM transporturi t
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND 
EXTRACT (MONTH FROM data_plecare) BETWEEN 1 AND 4)
SELECT id_vehicul,model,MAX(linie) AS nr_transporturi, MAX(transporturi) AS lista_transporturi
FROM tr_v
GROUP BY id_vehicul,model
ORDER BY id_vehicul;

/* 9.Lista transporturilor ce au plecat din fiecare depozit în săptămâna 13-19.03.2018. Ce transporturi au plecat din fiecare depozit în săptamana 13-19.03.2018? */
WITH RECURSIVE tr_dep(id_depozit, depozit, id_transport,linie, transporturi) AS(
SELECT tran.id_depozit_1, CAST('Depozitul ' ||tran.id_depozit_1||'('||adresa||', '||c_p.judet||')' AS VARCHAR(500)) AS depozit,tran.id_transport,1,CAST('\1: '||tran.id_transport||'('||tran.data_plecare||')' AS VARCHAR(500))
FROM tran
INNER JOIN depozite d ON tran.id_depozit_1=d.id_depozit
INNER JOIN coduri_postale c_p ON d.cod_postal=c_p.cod_postal
WHERE linie=1
UNION ALL
SELECT tr_dep.id_depozit,tr_dep.depozit,tr_dep.id_transport,tr_dep.linie + 1,CAST (tr_dep.transporturi|| ' \' || tr_dep.linie + 1 ||tran.id_transport||'('||tran.data_plecare||')' AS VARCHAR(500))
FROM tran
INNER JOIN depozite d ON tran.id_depozit_1=d.id_depozit
INNER JOIN coduri_postale c_p ON d.cod_postal=c_p.cod_postal
INNER JOIN tr_dep ON tran.id_depozit_1=tr_dep.id_depozit
AND tran.linie = tr_dep.linie+ 1),
tran AS (
SELECT t.*,ROW_NUMBER()OVER(PARTITION BY id_depozit_1 ORDER BY id_transport) AS linie
FROM transporturi t
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND 
EXTRACT (MONTH FROM data_plecare)=3 AND
EXTRACT (DAY FROM data_plecare) BETWEEN 13 AND 19)
SELECT id_depozit,depozit,MAX(linie) AS nr_transporturi, MAX(transporturi) AS lista_transporturi
FROM tr_dep
GROUP BY id_depozit,depozit
ORDER BY id_depozit;

/* 10.Lista coletelor returnate în lunile Ianuarie-Mai 2018. Ce colete au fost returnate în lunile Ianuarie-Mari 2018? */
WITH RECURSIVE col_ret(id_retur,retur,nr_factura, nr_crt, id_colet, lista_colete)  AS(
SELECT ret.id_retur,CAST(ret.id_retur||' ('||ret.motiv_retur||')' AS VARCHAR(500)),ret.nr_factura, 1, ret.id_colet, CAST(' \1-'||c.continut_colet AS VARCHAR(500))
FROM ret
INNER JOIN colete c ON ret.id_colet=c.id_colet
WHERE nr_crt=1
UNION ALL
SELECT col_ret.id_retur,col_ret.retur,col_ret.nr_factura,col_ret.nr_crt+1,col_ret.id_colet,CAST (col_ret.lista_colete|| ' \' || col_ret.nr_crt + 1 ||'-'||c.continut_colet AS VARCHAR(500))
FROM ret
INNER JOIN colete c ON ret.id_colet=c.id_colet
INNER JOIN col_ret ON ret.id_retur=col_ret.id_retur
AND ret.nr_crt = col_ret.nr_crt+ 1),
ret AS (
SELECT r.*,d_f.id_colet,ROW_NUMBER()OVER(PARTITION BY id_retur ORDER BY d_f.id_colet) AS nr_crt
FROM retururi r
INNER JOIN detalii_facturi d_f ON r.nr_factura=d_f.nr_factura
WHERE EXTRACT (YEAR FROM data_retur)=2018 AND 
EXTRACT (MONTH FROM data_retur) BETWEEN 1 AND 5)
SELECT retur,MAX(nr_crt) AS numar_colete, MAX(lista_colete) AS lista_colete
FROM col_ret
GROUP BY id_retur,retur
ORDER BY id_retur;

/* 11.Lista transporturilor din depozit în depozit efectuate în luna Februarie 2018. Ce transporturi din depozit în depozit au fost efectuate în luna Februarie 2018? */
WITH RECURSIVE tr_dep2(id_depozit_1,id_depozit_2, depozite, id_transport,linie, transporturi) AS(
SELECT tran.id_depozit_1,tran.id_depozit_2, CAST('Depozitul '||tran.id_depozit_1||' -> '||'Depozitul '||tran.id_depozit_2 AS VARCHAR(500)),tran.id_transport,1,CAST('\1: '||tran.id_transport||'('||tran.data_plecare||')' AS VARCHAR(500))
FROM tran
WHERE linie=1
UNION ALL
SELECT tr_dep2.id_depozit_1,tr_dep2.id_depozit_2,tr_dep2.depozite,tr_dep2.id_transport,tr_dep2.linie + 1,CAST (tr_dep2.transporturi|| ' \' || tr_dep2.linie + 1 ||tran.id_transport||'('||tran.data_plecare||')' AS VARCHAR(500))
FROM tran
INNER JOIN tr_dep2 ON tran.id_depozit_1=tr_dep2.id_depozit_1 AND tran.id_depozit_2=tr_dep2.id_depozit_2
AND tran.linie = tr_dep2.linie+ 1),
tran AS (
SELECT t.*,ROW_NUMBER()OVER(PARTITION BY id_depozit_1,id_depozit_2 ORDER BY id_transport) AS linie
FROM transporturi t
WHERE id_depozit_2 IS NOT NULL AND 
EXTRACT (YEAR FROM data_plecare)=2018 AND 
EXTRACT (MONTH FROM data_plecare)=2)
SELECT depozite,MAX(linie) AS nr_transporturi, MAX(transporturi) AS lista_transporturi
FROM tr_dep2
GROUP BY id_depozit_1,depozite
ORDER BY id_depozit_1;

/* 12.Lista tuturor coletelor expediată de fiecare persoană jurdică(firmă) din județul Suceava în perioada xx.01-xx.04. Ce colete a expediat fiecare persoană juridică(firmă) din județul Suceava în perioada xx.01-xx.04.2018? */
WITH RECURSIVE e_jur(id_expeditor, expeditor,id_colet, nr_crt, lista_colete)AS(
SELECT col.id_expeditor, e.nume, col.id_colet, 1, CAST(' \'||col.id_colet||': '||col.continut_colet AS VARCHAR(500))
FROM col 
INNER JOIN expeditori e ON col.id_expeditor=e.id_expeditor
WHERE nr_crt=1
UNION ALL
SELECT e_jur.id_expeditor,e_jur.expeditor,e_jur.id_colet,e_jur.nr_crt+1,CAST(e_jur.lista_colete||' \'||col.id_colet||'-'||col.continut_colet AS VARCHAR(500))
FROM col
INNER JOIN expeditori e ON col.id_expeditor=e.id_expeditor
INNER JOIN e_jur ON col.id_expeditor=e_jur.id_expeditor
AND col.nr_crt=e_jur.nr_crt+ 1),
col AS (
SELECT c.*,e.prenume,c_p.judet, ROW_NUMBER()OVER(PARTITION BY c.id_expeditor ORDER BY id_colet) AS nr_crt
FROM colete c
INNER JOIN expeditori e ON c.id_expeditor=e.id_expeditor
INNER JOIN coduri_postale c_p ON e.cod_postal=c_p.cod_postal
WHERE e.prenume IS NULL AND c_p.judet='Suceava' AND
EXTRACT (YEAR FROM data_expediere)=2018 AND 
EXTRACT (MONTH FROM data_expediere) BETWEEN 1 AND 4)
SELECT id_expeditor,expeditor,MAX(nr_crt) AS numar_colete, MAX(lista_colete) AS lista_colete
FROM e_jur
GROUP BY id_expeditor,expeditor
ORDER BY 1;

/*13.Lista tututor incidentelor raportate de fiecare șofer în parte în perioada xx.01-xx.03.2018. Ce incidente a raportat fiecare șofer în parte în perioada xx.01-xx.03.2018? */
WITH RECURSIVE i_s(id_sofer, sofer, id_incident, nr_crt, lista_incidente)AS(
SELECT i.id_sofer, CAST(a.nume||' '||a.prenume AS VARCHAR(500)), i.id_incident, 1, CAST('\1: '||i.tip_incident||'('||i.data_raportarii||')' AS VARCHAR(500))
FROM incidente i
INNER JOIN soferi s ON i.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
WHERE EXTRACT (YEAR FROM data_raportarii)=2018 AND 
EXTRACT (MONTH FROM data_raportarii) BETWEEN 1 AND 3
UNION ALL
SELECT i_s.id_sofer, sofer, i.id_incident, i_s.nr_crt+1, CAST(i_s.lista_incidente||' \'||i_s.nr_crt+1||': '||i.tip_incident||'('||i.data_raportarii||')' AS VARCHAR(500)) 
FROM incidente i
INNER JOIN soferi s ON i.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
INNER JOIN i_s ON i.id_sofer=i_s.id_sofer AND i.id_incident=
(SELECT MIN(id_incident)
FROM incidente
WHERE id_sofer=i_s.id_sofer AND id_incident>i_s.id_incident)
WHERE EXTRACT (YEAR FROM data_raportarii)=2018 AND 
EXTRACT (MONTH FROM data_raportarii) BETWEEN 1 AND 3)
SELECT id_sofer, sofer,nr_crt AS numar_incidente, lista_incidente
FROM i_s
WHERE (id_sofer, sofer, nr_crt) IN
(SELECT id_sofer, sofer, max(nr_crt)
FROM i_s
GROUP BY id_sofer, sofer)
ORDER BY 1;

/*14.Lista coletelor primite de către fiecare persoană juridică(firmă) din județul Iași în perioada xx.02-xx.06.2018. Ce colete a primit fiecare persoană juridică(firmă) din județul Iași în perioadă xx.02-xx.06.2018? */
WITH RECURSIVE d_jur(id_destinatar, destinatar,id_colet, nr_crt, lista_colete)AS(
SELECT col.id_destinatar, d.nume, col.id_colet, 1, CAST(' \'||col.id_colet||': '||col.continut_colet AS VARCHAR(500))
FROM col 
INNER JOIN destinatari d ON col.id_destinatar=d.id_destinatar
WHERE nr_crt=1
UNION ALL
SELECT d_jur.id_destinatar,d_jur.destinatar,d_jur.id_colet,d_jur.nr_crt+1,CAST(d_jur.lista_colete||' \'||col.id_colet||'-'||col.continut_colet AS VARCHAR(500))
FROM col
INNER JOIN destinatari d ON col.id_destinatar=d.id_destinatar
INNER JOIN d_jur ON col.id_destinatar=d_jur.id_destinatar
AND col.nr_crt=d_jur.nr_crt+ 1),
col AS (
SELECT c.*,d.prenume,c_p.judet, ROW_NUMBER()OVER(PARTITION BY c.id_destinatar ORDER BY id_colet) AS nr_crt
FROM colete c
INNER JOIN destinatari d ON c.id_destinatar=d.id_destinatar
INNER JOIN coduri_postale c_p ON d.cod_postal=c_p.cod_postal
WHERE d.prenume IS NULL AND c_p.judet='Iași' AND 
EXTRACT (YEAR FROM data_expediere)=2018 AND 
EXTRACT (MONTH FROM data_expediere) BETWEEN 2 AND 6)
SELECT id_destinatar,destinatar,MAX(nr_crt) AS numar_colete, MAX(lista_colete) AS lista_colete
FROM d_jur
GROUP BY id_destinatar,destinatar
ORDER BY 1;

/* 15.Lista coletelor livrate de către fiecare șofer în perioada 21.07-23.07.2018. Ce colete au fost livtrate de către fiecare șofer în perioada 21.07-23.07.2018? */
WITH RECURSIVE col_sof(id_sofer,sofer, nr_crt, colete) AS(
SELECT col.id_sofer,CAST(a.nume||' '||a.prenume AS VARCHAR(500)),1, CAST ('\1: ' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN soferi s ON col.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
WHERE nr_crt=1
UNION ALL
SELECT col.id_sofer,col_sof.sofer,col_sof.nr_crt + 1,CAST (col_sof.colete || ' \' || col_sof.nr_crt + 1 || ' :' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN soferi s ON col.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
INNER JOIN col_sof ON col.id_sofer=col_sof.id_sofer
AND col.nr_crt = col_sof.nr_crt+ 1),
col AS (
SELECT t.*, d_t.id_colet,ROW_NUMBER()OVER(PARTITION BY id_sofer ORDER BY id_colet) AS nr_crt 
FROM transporturi t 
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport 
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND
EXTRACT (MONTH FROM data_plecare)=7 AND 
EXTRACT (DAY FROM data_plecare) BETWEEN 21 AND 23)
SELECT id_sofer,sofer,MAX(nr_crt) AS nr_colete, MAX(colete) AS lista_colete
FROM col_sof
GROUP BY id_sofer,sofer
ORDER BY 1;

/* 16.Lista facturilor primite de către fiecare destinatar în parte în perioada xx.03-xx.06.2018. Ce facturi a primit fiecare destinatar în perioada xx.03-xx.06.2018? */
WITH RECURSIVE f_d(id_destinatar,destinatar, nr_crt, facturi) AS(
SELECT fact.id_destinatar,CAST(CASE WHEN d.prenume IS NOT NULL THEN d.nume||' '||d.prenume ELSE d.nume END AS VARCHAR(500)),1, CAST ('\' ||fact.nr_factura||'('||fact.data||')' AS VARCHAR(1000))
FROM fact 
INNER JOIN destinatari d ON fact.id_destinatar=d.id_destinatar
WHERE nr_crt=1
UNION ALL
SELECT f_d.id_destinatar,f_d.destinatar,f_d.nr_crt + 1,CAST (f_d.facturi || ' \' ||fact.nr_factura||'('||fact.data||')'  AS VARCHAR(1000))
FROM fact 
INNER JOIN destinatari d ON fact.id_destinatar=d.id_destinatar
INNER JOIN f_d ON fact.id_destinatar=f_d.id_destinatar
AND fact.nr_crt = f_d.nr_crt+ 1),
fact AS (
SELECT f.*,ROW_NUMBER()OVER(PARTITION BY id_destinatar ORDER BY nr_factura) AS nr_crt
FROM facturi f
WHERE EXTRACT (YEAR FROM data)=2018 AND 
EXTRACT (MONTH FROM data) BETWEEN 3 AND 6)
SELECT id_destinatar,destinatar,MAX(nr_crt) AS nr_facturi, MAX(facturi) AS lista_facturi
FROM f_d
GROUP BY id_destinatar,destinatar
ORDER BY 1;

/* 17.Lista facturilor întocmite de către fiecare expeditor în perioada xx.04-xx.08.2018. Ce facturi a întocmit fiecare expeditor în perioada xx.04-xx.08.2018? */
WITH RECURSIVE f_e(id_expeditor,expeditor, nr_crt, facturi) AS(
SELECT fact.id_expeditor,CAST(CASE WHEN e.prenume IS NOT NULL THEN e.nume||' '||e.prenume ELSE e.nume END AS VARCHAR(500)),1, CAST ('\' ||fact.nr_factura||'('||fact.data||')' AS VARCHAR(1000))
FROM fact 
INNER JOIN expeditori e ON fact.id_expeditor=e.id_expeditor
WHERE nr_crt=1
UNION ALL
SELECT f_e.id_expeditor,f_e.expeditor, f_e.nr_crt + 1,CAST (f_e.facturi || ' \' ||fact.nr_factura||'('||fact.data||')'  AS VARCHAR(1000))
FROM fact 
INNER JOIN expeditori e ON fact.id_expeditor=e.id_expeditor
INNER JOIN f_e ON fact.id_expeditor=f_e.id_expeditor
AND fact.nr_crt = f_e.nr_crt+ 1),
fact AS (
SELECT f.*,ROW_NUMBER()OVER(PARTITION BY id_expeditor ORDER BY nr_factura) AS nr_crt
FROM facturi f
WHERE EXTRACT (YEAR FROM data)=2018 AND 
EXTRACT (MONTH FROM data) BETWEEN 4 AND 8)
SELECT id_expeditor,expeditor,MAX(nr_crt) AS nr_facturi, MAX(facturi) AS lista_facturi
FROM f_e
GROUP BY id_expeditor,expeditor
ORDER BY id_expeditor;

/* 18.Lista expeditorilor organizată pe coduri poștale pentru primii 10.000 de expeditori. Pentru primii 10.000 de expeditori să se alcătuiască lista acestora organizată pe coduri poștale */
WITH RECURSIVE cp_exp(cod_postal,id_expeditor,nr_crt,expeditori) AS (
SELECT e.cod_postal,e.id_expeditor, 1,CAST('\1: '||CASE WHEN e.prenume IS NOT NULL THEN e.nume||' '||e.prenume ELSE e.nume END AS VARCHAR(500))
FROM expeditori e 
WHERE (e.cod_postal, e.id_expeditor) IN 
(SELECT cod_postal,MIN(id_expeditor)
FROM expeditori
GROUP BY cod_postal) AND
id_expeditor<=10000
UNION ALL 
SELECT cp_exp.cod_postal,e.id_expeditor,cp_exp.nr_crt+1,CAST(cp_exp.expeditori||' \'||cp_exp.nr_crt+1||': '||CASE WHEN e.prenume IS NOT NULL THEN e.nume||' '||e.prenume ELSE e.nume END AS VARCHAR(500))
FROM expeditori e 
INNER JOIN cp_exp ON e.cod_postal=cp_exp.cod_postal AND e.id_expeditor=
(SELECT MIN(id_expeditor)
FROM expeditori 
WHERE cod_postal=cp_exp.cod_postal AND id_expeditor>cp_exp.id_expeditor
AND id_expeditor<=10000))
SELECT cod_postal,nr_crt AS nr_expeditori,expeditori AS lista_expeditori
FROM cp_exp
WHERE (cod_postal,nr_crt) IN
(SELECT cod_postal,MAX(nr_crt)
FROM cp_exp
GROUP BY cod_postal)
ORDER BY 1;

/* 19.Lista destinatarilor organizată pe coduri poștale pentru destinatarii al caror id este cuprins între 60000 și 70000. Să se afișeze lista destinatarilor oganizată pe coduri poștale pentru destinatarii cuprinși între 60.000 și 70.000 */
WITH RECURSIVE cp_dest(cod_postal,id_destinatar,nr_crt,destinatari) AS (
SELECT d.cod_postal,d.id_destinatar, 1,CAST('\1: '||CASE WHEN d.prenume IS NOT NULL THEN d.nume||' '||d.prenume ELSE d.nume END AS VARCHAR(500))
FROM destinatari d
WHERE id_destinatar>=60000 AND id_destinatar<=70000
UNION ALL 
SELECT cp_dest.cod_postal,d.id_destinatar,cp_dest.nr_crt+1,CAST(cp_dest.destinatari||' \'||cp_dest.nr_crt+1||': '||CASE WHEN d.prenume IS NOT NULL THEN d.nume||' '||d.prenume ELSE d.nume END AS VARCHAR(500))
FROM destinatari d 
INNER JOIN cp_dest ON d.cod_postal=cp_dest.cod_postal AND d.id_destinatar=
(SELECT MIN(id_destinatar)
FROM destinatari 
WHERE cod_postal=cp_dest.cod_postal AND id_destinatar>cp_dest.id_destinatar AND
id_destinatar>=60000 AND id_destinatar<=70000))
SELECT cod_postal,nr_crt AS nr_destinatari,destinatari AS lista_destinatari
FROM cp_dest
WHERE (cod_postal,nr_crt) IN
(SELECT cod_postal,MAX(nr_crt)
FROM cp_dest
GROUP BY cod_postal)
ORDER BY 1;

/* 20.Lista angajaților organizată pe coduri poștale */
WITH RECURSIVE cp_ang(cod_postal,id_angajat,nr_crt,angajati) AS (
SELECT a.cod_postal,a.id_angajat, 1,CAST('\1: '||a.nume||' '||a.prenume AS VARCHAR(500))
FROM angajati a 
UNION ALL 
SELECT cp_ang.cod_postal,a.id_angajat,cp_ang.nr_crt+1,CAST(cp_ang.angajati||' \'||cp_ang.nr_crt+1||': '||a.nume||' '||a.prenume AS VARCHAR(500))
FROM angajati a 
INNER JOIN cp_ang ON a.cod_postal=cp_ang.cod_postal AND a.id_angajat=
(SELECT MIN(id_angajat)
FROM angajati
WHERE cod_postal=cp_ang.cod_postal AND id_angajat>cp_ang.id_angajat))
SELECT cod_postal,nr_crt AS nr_angajati,angajati AS lista_angajati
FROM cp_ang
WHERE (cod_postal,nr_crt) IN
(SELECT cod_postal,MAX(nr_crt)
FROM cp_ang
GROUP BY cod_postal)
ORDER BY 1;

/* 21.Lista chitanțelor achitare de către fiecare destinatar în parte in perioada xx.05-xx.07.2018.*/
WITH RECURSIVE ch_d(id_destinatar,destinatar, nr_crt, chitante) AS(
SELECT chit.id_destinatar,CAST(CASE WHEN d.prenume IS NOT NULL THEN d.nume||' '||d.prenume ELSE d.nume END AS VARCHAR(500)),1, CAST ('\'||ch.nr_chitanta||'('||ch.data_achitare||')' AS VARCHAR(1000))
FROM chit 
INNER JOIN destinatari d ON chit.id_destinatar=d.id_destinatar
INNER JOIN chitante ch ON chit.nr_chitanta=ch.nr_chitanta
WHERE nr_crt=1
UNION ALL
SELECT chit.id_destinatar,ch_d.destinatar,ch_d.nr_crt + 1,CAST (ch_d.chitante || ' \' ||ch.nr_chitanta||'('||ch.data_achitare||')'AS VARCHAR(1000))
FROM chit 
INNER JOIN destinatari d ON chit.id_destinatar=d.id_destinatar
INNER JOIN chitante ch ON chit.nr_chitanta=ch.nr_chitanta
INNER JOIN ch_d ON chit.id_destinatar=ch_d.id_destinatar
AND chit.nr_crt = ch_d.nr_crt+ 1),
chit AS (
SELECT f.*,nr_chitanta,ROW_NUMBER()OVER(PARTITION BY id_destinatar ORDER BY nr_chitanta) AS nr_crt 
FROM facturi f
INNER JOIN chitante ch ON f.nr_factura=ch.nr_factura
WHERE EXTRACT(YEAR FROM data_achitare)=2018 AND
EXTRACT (MONTH FROM data_achitare) BETWEEN 5 AND 7)
SELECT id_destinatar,destinatar,MAX(nr_crt) AS nr_chitante, MAX(chitante) AS lista_chitante
FROM ch_d
GROUP BY id_destinatar,destinatar
ORDER BY 1;

/* 22.Lista coletelor transportate cu fiecare vehicul în perioada 01.05-06.05.2018. Ce colete au fost transportate prin intermediul fiecărui vehicul în perioada 01.05-06.05.2018? */
WITH RECURSIVE col_veh(id_vehicul,model, nr_crt, colete) AS(
SELECT col.id_vehicul,CAST(v.model||' ('||v.nr_inmatriculare||')' AS VARCHAR(500)),1, CAST ('\1: ' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN vehicule v ON col.id_vehicul=v.id_vehicul
WHERE nr_crt=1
UNION ALL
SELECT col.id_vehicul,col_veh.model,col_veh.nr_crt + 1,CAST (col_veh.colete || ' \' || col_veh.nr_crt + 1 || ' :' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN vehicule v ON col.id_vehicul=v.id_vehicul
INNER JOIN col_veh ON col.id_vehicul=col_veh.id_vehicul
AND col.nr_crt = col_veh.nr_crt+ 1),
col AS (
SELECT t.*, d_t.id_colet,ROW_NUMBER()OVER(PARTITION BY id_vehicul ORDER BY id_colet) AS nr_crt 
FROM transporturi t 
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport 
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND
EXTRACT (MONTH FROM data_plecare)=5 AND 
EXTRACT (DAY FROM data_plecare) BETWEEN 1 AND 6)
SELECT id_vehicul,model,MAX(nr_crt) AS nr_colete, MAX(colete) AS lista_colete
FROM col_veh
GROUP BY id_vehicul,model
ORDER BY id_vehicul;

/* 23.Lista coletelor primite de către destinatarii din fiecare județ în perioada 04.04-06.04.2018. Ce colete au fost livrate către fiecare județ în perioada 04.04-06.04.2018? */
WITH RECURSIVE col_djud(judet, nr_crt, colete) AS(
SELECT col.judet,1, CAST ('\1: ' ||c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
WHERE nr_crt=1
UNION ALL
SELECT col.judet,col_djud.nr_crt + 1,CAST (col_djud.colete || ' \' || col_djud.nr_crt + 1 || ': ' ||c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN col_djud ON col.judet=col_djud.judet
AND col.nr_crt = col_djud.nr_crt+ 1),
col AS (
SELECT cp.*, c.id_colet,ROW_NUMBER()OVER(PARTITION BY cp.judet ORDER BY id_colet) AS nr_crt 
FROM coduri_postale cp
INNER JOIN destinatari d ON cp.cod_postal=d.cod_postal
INNER JOIN colete c ON d.id_destinatar=c.id_destinatar
WHERE EXTRACT (YEAR FROM data_primire_colet)=2018 AND
EXTRACT (MONTH FROM data_primire_colet)=4 AND 
EXTRACT (DAY FROM data_primire_colet) BETWEEN 4 AND 6)
SELECT judet,MAX(nr_crt) AS nr_colete, MAX(colete) AS lista_colete
FROM col_djud
GROUP BY judet
ORDER BY judet;

/* 24.Lista coletelor trimise de expeditorii din fiecare județ în perioada 20.08-24.08.2018. Ce colete au fost trimise(expediate) din fiecare județ în perioada 20.08-24.08.2018? */
WITH RECURSIVE col_ejud(judet, nr_crt, colete) AS(
SELECT col.judet,1, CAST ('\1: ' ||c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
WHERE nr_crt=1
UNION ALL
SELECT col.judet,col_ejud.nr_crt + 1,CAST (col_ejud.colete || ' \' || col_ejud.nr_crt + 1 || ': ' ||c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN col_ejud ON col.judet=col_ejud.judet
AND col.nr_crt = col_ejud.nr_crt+ 1),
col AS (
SELECT cp.*, c.id_colet,ROW_NUMBER()OVER(PARTITION BY cp.judet ORDER BY id_colet) AS nr_crt 
FROM coduri_postale cp
INNER JOIN expeditori e ON cp.cod_postal=e.cod_postal
INNER JOIN colete c ON e.id_expeditor=c.id_expeditor
WHERE EXTRACT (YEAR FROM data_expediere)=2018 AND
EXTRACT (MONTH FROM data_expediere)=8 AND 
EXTRACT (DAY FROM data_expediere) BETWEEN 20 AND 24)
SELECT judet,MAX(nr_crt) AS nr_colete, MAX(colete) AS lista_colete
FROM col_ejud
GROUP BY judet
ORDER BY judet;

/* 25.Lista transporturilor pentru fiecare colet expediat în luna Iulie a anului 2018. Pentru fiecare colet expediat în luna Iulie 2018, să se afișeze lista transporturilor. */
WITH RECURSIVE col_tran(id_colet,continut_colet, nr_crt, transporturi) AS(
SELECT tran.id_colet,c.continut_colet,1, CAST('\1: '||tran.id_transport||'('||tran.data_plecare||')' AS VARCHAR(1000))
FROM tran
INNER JOIN colete c ON tran.id_colet=c.id_colet
WHERE nr_crt=1
UNION ALL
SELECT col_tran.id_colet,col_tran.continut_colet,col_tran.nr_crt + 1,CAST (col_tran.transporturi || ' \' || col_tran.nr_crt + 1 ||': '||tran.id_transport||'('||tran.data_plecare||')' AS VARCHAR(1000))
FROM tran
INNER JOIN colete c ON tran.id_colet=c.id_colet
INNER JOIN col_tran ON tran.id_colet=col_tran.id_colet
AND tran.nr_crt = col_tran.nr_crt+ 1),
tran AS (
SELECT d_t.*,t.data_plecare,ROW_NUMBER()OVER(PARTITION BY id_colet ORDER BY d_t.id_transport) AS nr_crt
FROM detalii_transport d_t 
INNER JOIN transporturi t ON d_t.id_transport=t.id_transport
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND
EXTRACT (MONTH FROM data_plecare)=7)
SELECT id_colet,continut_colet,MAX(nr_crt) AS nr_transporturi, MAX(transporturi) AS lista_transporturi
FROM col_tran
GROUP BY id_colet,continut_colet
ORDER BY 1;

/* 26.Lista coletelor organizată pe transporturi efectuate în luna Ianuarie. Ce colete conține fiecare transport efectuat în luna Ianuarie? */
WITH RECURSIVE i_t(id_transport,id_colet, nr_crt,lista_colete) AS(
SELECT col.id_transport,col.id_colet,1, CAST('\1-'||continut_colet AS VARCHAR(500))
FROM col
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN transporturi t ON col.id_transport=t.id_transport
WHERE nr_crt=1
UNION ALL
SELECT i_t.id_transport,i_t.id_colet,i_t.nr_crt + 1,CAST(i_t.lista_colete||' \'||i_t.nr_crt+1||'-'||c.continut_colet AS VARCHAR(500))
FROM col
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN i_t ON col.id_transport=i_t.id_transport
AND col.nr_crt = i_t.nr_crt+ 1),
col AS (
SELECT d_t.*,t.data_plecare,ROW_NUMBER()OVER(PARTITION BY d_t.id_transport ORDER BY d_t.id_colet) AS nr_crt
FROM detalii_transport d_t 
INNER JOIN transporturi t ON d_t.id_transport=t.id_transport
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND 
EXTRACT (MONTH FROM data_plecare)=1)
SELECT id_transport,MAX(nr_crt) AS numar_colete, MAX(lista_colete) AS lista_colete
FROM i_t
GROUP BY id_transport
ORDER BY 1;

/* 27.Lista coletelor organizată pe transporturi efectuate în anul 2018, ce au avut ca și oră de plecare, ora 15:00. Ce colete conține fiecare transport efectuat în anul 2018, ce au avut ca și oră de plecare, ora 15:00? */
WITH RECURSIVE i_t(id_transport,id_colet, nr_crt,lista_colete) AS(
SELECT col.id_transport,col.id_colet,1, CAST('\1-'||continut_colet AS VARCHAR(500))
FROM col
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN transporturi t ON col.id_transport=t.id_transport
WHERE nr_crt=1
UNION ALL
SELECT i_t.id_transport,i_t.id_colet,i_t.nr_crt + 1,CAST(i_t.lista_colete||' \'||i_t.nr_crt+1||'-'||c.continut_colet AS VARCHAR(500))
FROM col
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN i_t ON col.id_transport=i_t.id_transport
AND col.nr_crt = i_t.nr_crt+ 1),
col AS (
SELECT d_t.*,t.data_plecare,t.ora_plecare,ROW_NUMBER()OVER(PARTITION BY d_t.id_transport ORDER BY d_t.id_colet) AS nr_crt
FROM detalii_transport d_t 
INNER JOIN transporturi t ON d_t.id_transport=t.id_transport
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND
ora_plecare='15:00')
SELECT id_transport,MAX(nr_crt) AS numar_colete, MAX(lista_colete) AS lista_colete
FROM i_t
GROUP BY id_transport
ORDER BY 1;

/* 28.Lista coletelor pentru primele 2000 de transporturi. Ce colete conțin -primele 2000 de transporturi? */
WITH RECURSIVE i_t(id_transport,id_colet, nr_crt,lista_colete) AS(
SELECT col.id_transport,col.id_colet,1, CAST('\1-'||continut_colet AS VARCHAR(500))
FROM col
INNER JOIN colete c ON col.id_colet=c.id_colet
WHERE nr_crt=1
UNION ALL
SELECT i_t.id_transport,i_t.id_colet,i_t.nr_crt + 1,CAST(i_t.lista_colete||' \'||i_t.nr_crt+1||'-'||c.continut_colet AS VARCHAR(500))
FROM col
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN i_t ON col.id_transport=i_t.id_transport
AND col.nr_crt = i_t.nr_crt+ 1),
col AS (
SELECT d_t.*,ROW_NUMBER()OVER(PARTITION BY d_t.id_transport ORDER BY d_t.id_colet) AS nr_crt
FROM detalii_transport d_t 
WHERE d_t.id_transport<=2000)
SELECT id_transport,MAX(nr_crt) AS numar_colete, MAX(lista_colete) AS lista_colete
FROM i_t
GROUP BY id_transport
ORDER BY 1;

/* 29.Lista coletelor pentru transporturile efectuate în anul 2018 cu un vehicul, model Ford Transit. Ce colete au fost transportate în anul 2018, cu un vehicul, model Ford Transit? */
WITH RECURSIVE i_t(id_transport,id_colet, nr_crt,lista_colete) AS(
SELECT col.id_transport,col.id_colet,1, CAST('\1-'||continut_colet AS VARCHAR(500))
FROM col
INNER JOIN colete c ON col.id_colet=c.id_colet
WHERE nr_crt=1
UNION ALL
SELECT i_t.id_transport,i_t.id_colet,i_t.nr_crt + 1,CAST(i_t.lista_colete||' \'||i_t.nr_crt+1||'-'||c.continut_colet AS VARCHAR(500))
FROM col
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN i_t ON col.id_transport=i_t.id_transport
AND col.nr_crt = i_t.nr_crt+ 1),
col AS (
SELECT d_t.*,t.id_vehicul,t.data_plecare,ROW_NUMBER()OVER(PARTITION BY d_t.id_transport ORDER BY d_t.id_colet) AS nr_crt
FROM detalii_transport d_t 
INNER JOIN transporturi t ON d_t.id_transport=t.id_transport
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul
WHERE v.model='Ford Transit' AND EXTRACT(YEAR FROM data_plecare)=2018)
SELECT id_transport,MAX(nr_crt) AS numar_colete, MAX(lista_colete) AS lista_colete
FROM i_t
GROUP BY id_transport
ORDER BY 1;

/* 30.Lista coletelor pentru transporturile efectuate în anul 2018 cu un vehicul, model Ford Transit, ce au fost conduse de către șoferi ce au depășit pragul de 5000 de km. Ce colete au fost transportate în anul 2018, cu un vehicul, model Ford Transit, ce au fost conduse de către șoferi ce au depășit pragul de 5000 de km  ? */
WITH RECURSIVE i_t(id_transport,id_colet, nr_crt,lista_colete) AS(
SELECT col.id_transport,col.id_colet,1, CAST('\1-'||continut_colet AS VARCHAR(500))
FROM col
INNER JOIN colete c ON col.id_colet=c.id_colet
WHERE nr_crt=1
UNION ALL
SELECT i_t.id_transport,i_t.id_colet,i_t.nr_crt + 1,CAST(i_t.lista_colete||' \'||i_t.nr_crt+1||'-'||c.continut_colet AS VARCHAR(500))
FROM col
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN i_t ON col.id_transport=i_t.id_transport
AND col.nr_crt = i_t.nr_crt+ 1),
col AS (
SELECT d_t.*,t.id_vehicul,t.id_sofer,t.data_plecare,ROW_NUMBER()OVER(PARTITION BY d_t.id_transport ORDER BY d_t.id_colet) AS nr_crt
FROM detalii_transport d_t 
INNER JOIN transporturi t ON d_t.id_transport=t.id_transport
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul
INNER JOIN soferi s ON t.id_sofer=s.id_sofer 
WHERE model='Ford Transit' AND km_parcursi>=5000 AND EXTRACT(YEAR FROM data_plecare)=2018)
SELECT id_transport,MAX(nr_crt) AS numar_colete, MAX(lista_colete) AS lista_colete
FROM i_t
GROUP BY id_transport
ORDER BY 1;

/* 31.Lista coletelor pentru transporturile efectuate în anul 2018 cu un vehicul, model Ford Transit, ce au fost conduse de către șoferi ce au depășit pragul de 5000 de km. Ce colete au fost transportate în anul 2018, cu un vehicul, model Ford Transit, ce au fost conduse de către șoferi ce au depășit pragul de 5000 de km  ? */
WITH RECURSIVE i_t(id_transport,depozit,sofer,id_colet, nr_crt,lista_colete) AS(
SELECT col.id_transport,CAST('Depozitul '||t.id_depozit_1||' ('||d.adresa||')' AS VARCHAR(500)), CAST(a.nume||' '||a.prenume AS VARCHAR(500)),col.id_colet,1, CAST('\1-'||continut_colet AS VARCHAR(500))
FROM col
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN soferi s ON col.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
INNER JOIN transporturi t ON col.id_transport=t.id_transport
INNER JOIN depozite d ON t.id_depozit_1=d.id_depozit
WHERE nr_crt=1
UNION ALL
SELECT i_t.id_transport,i_t.depozit,i_t.sofer,i_t.id_colet,i_t.nr_crt + 1,CAST(i_t.lista_colete||' \'||i_t.nr_crt+1||'-'||c.continut_colet AS VARCHAR(500))
FROM col
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN soferi s ON col.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
INNER JOIN transporturi t ON col.id_transport=t.id_transport
INNER JOIN depozite d ON t.id_depozit_1=d.id_depozit
INNER JOIN i_t ON col.id_transport=i_t.id_transport
AND col.nr_crt = i_t.nr_crt+ 1),
col AS (
SELECT d_t.*,t.id_vehicul,t.id_sofer,t.data_plecare,ROW_NUMBER()OVER(PARTITION BY d_t.id_transport ORDER BY d_t.id_colet) AS nr_crt
FROM detalii_transport d_t 
INNER JOIN transporturi t ON d_t.id_transport=t.id_transport
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul
INNER JOIN soferi s ON t.id_sofer=s.id_sofer 
WHERE model='Ford Transit' AND km_parcursi>=5000 AND EXTRACT(YEAR FROM data_plecare)=2018)
SELECT id_transport,depozit,sofer,MAX(nr_crt) AS numar_colete, MAX(lista_colete) AS lista_colete
FROM i_t
GROUP BY id_transport,depozit,sofer
ORDER BY 1;

/* 32.Lista coletelor organizată pe facturi întocmite în anul 2018. Ce colete conține fiecare factură întocmită în anul 2018?*/
WITH RECURSIVE l_f(nr_factura, nr_crt, id_colet, lista_colete) AS(
SELECT col.nr_factura,1,col.id_colet, CAST('\1-'||continut_colet AS VARCHAR(500))
FROM col
INNER JOIN colete c ON col.id_colet=c.id_colet
WHERE nr_crt=1
UNION ALL
SELECT l_f.nr_factura,l_f.nr_crt + 1,l_f.id_colet,CAST(l_f.lista_colete||' \'||l_f.nr_crt+1||'-'||c.continut_colet AS VARCHAR(500))
FROM col
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN l_f ON col.nr_factura=l_f.nr_factura
AND col.nr_crt=l_f.nr_crt+ 1),
col AS (
SELECT d_f.*,f.data,ROW_NUMBER()OVER(PARTITION BY d_f.nr_factura ORDER BY d_f.id_colet) AS nr_crt
FROM detalii_facturi d_f
INNER JOIN facturi f ON d_f.nr_factura=f.nr_factura
WHERE EXTRACT (YEAR FROM data)=2018)
SELECT nr_factura,MAX(nr_crt) AS numar_colete, MAX(lista_colete) AS lista_colete
FROM l_f
GROUP BY nr_factura
ORDER BY 1;

/* 33.Lista coletelor organizată pe facturi întocmite în anul 2018 de către primii 90.000 de expeditori. Ce colete conține fiecare factură întocmită în anul 2018 de către primii 90.000 de expeditori?*/
WITH RECURSIVE l_f(nr_factura,expeditor, nr_crt, id_colet, lista_colete) AS(
SELECT col.nr_factura,CAST(CASE WHEN e.prenume IS NOT NULL THEN e.nume||' '||e.prenume ELSE e.nume END AS VARCHAR(500)),1,col.id_colet, CAST('\1-'||continut_colet AS VARCHAR(500))
FROM col
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN expeditori e ON col.id_expeditor=e.id_expeditor
WHERE nr_crt=1
UNION ALL
SELECT l_f.nr_factura,l_f.expeditor,l_f.nr_crt + 1,l_f.id_colet,CAST(l_f.lista_colete||' \'||l_f.nr_crt+1||'-'||c.continut_colet AS VARCHAR(500))
FROM col
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN expeditori e ON col.id_expeditor=e.id_expeditor
INNER JOIN l_f ON col.nr_factura=l_f.nr_factura
AND col.nr_crt=l_f.nr_crt+ 1),
col AS (
SELECT d_f.*,f.data,f.id_expeditor,ROW_NUMBER()OVER(PARTITION BY d_f.nr_factura ORDER BY d_f.id_colet) AS nr_crt
FROM detalii_facturi d_f
INNER JOIN facturi f ON d_f.nr_factura=f.nr_factura
WHERE EXTRACT (YEAR FROM data)=2018 AND id_expeditor BETWEEN 1 AND 90000)
SELECT nr_factura,expeditor,MAX(nr_crt) AS numar_colete, MAX(lista_colete) AS lista_colete
FROM l_f
GROUP BY nr_factura,expeditor
ORDER BY 1;

/* 34.Lista coletelor ce depășesc valoarea de 1500lei și greutatea de 15kg organizată pe facturi primite în anul 2018 de către primii 85.000 de destinatari. Ce colete ce depășesc valoarea de 1500lei și greutatea de 15kg conține fiecare factură primită în anul 2018 de către primii 85.000 de destinatari?*/
WITH RECURSIVE l_f(nr_factura,expeditor, nr_crt, id_colet, lista_colete) AS(
SELECT col.nr_factura,CAST(CASE WHEN d.prenume IS NOT NULL THEN d.nume||' '||d.prenume ELSE d.nume END AS VARCHAR(500)),1,col.id_colet, CAST('\1-'||continut_colet AS VARCHAR(500))
FROM col
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN destinatari d ON col.id_destinatar=d.id_destinatar
WHERE nr_crt=1
UNION ALL
SELECT l_f.nr_factura,l_f.expeditor,l_f.nr_crt + 1,l_f.id_colet,CAST(l_f.lista_colete||' \'||l_f.nr_crt+1||'-'||c.continut_colet AS VARCHAR(500))
FROM col
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN destinatari d ON col.id_destinatar=d.id_destinatar
INNER JOIN l_f ON col.nr_factura=l_f.nr_factura
AND col.nr_crt=l_f.nr_crt+ 1),
col AS (
SELECT d_f.*,f.data,f.id_destinatar,c.valoare,c.greutate,ROW_NUMBER()OVER(PARTITION BY d_f.nr_factura ORDER BY d_f.id_colet) AS nr_crt
FROM detalii_facturi d_f
INNER JOIN facturi f ON d_f.nr_factura=f.nr_factura
INNER JOIN colete c ON d_f.id_colet=c.id_colet
WHERE EXTRACT (YEAR FROM data)=2018 AND 
f.id_destinatar BETWEEN 1 AND 85000 AND 
valoare>1500 AND greutate<15)
SELECT nr_factura,expeditor,MAX(nr_crt) AS numar_colete, MAX(lista_colete) AS lista_colete
FROM l_f
GROUP BY nr_factura,expeditor
ORDER BY 1;

/* 35.Lista coletelor ce depășesc valoarea de 500lei și greutatea de 5kg organizată pe facturi primite în anul 2018 de către primii 95.000 de destinatari din județul Iași. Ce colete ce depășesc valoarea de 1500lei și greutatea de 15kg conține fiecare factură primită în anul 2018 de către primii 95.000 de destinatari din judetul Iași?*/
WITH RECURSIVE l_f(nr_factura,destinatar,expeditor,nr_crt, id_colet, lista_colete) AS(
SELECT col.nr_factura,CAST(CASE WHEN d.prenume IS NOT NULL THEN d.nume||' '||d.prenume ELSE d.nume END AS VARCHAR(500)),CAST(CASE WHEN e.prenume IS NOT NULL THEN e.nume||' '||e.prenume ELSE e.nume END AS VARCHAR(500)),1,col.id_colet, CAST('\1-'||continut_colet AS VARCHAR(500))
FROM col
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN destinatari d ON col.id_destinatar=d.id_destinatar
INNER JOIN expeditori e ON col.id_expeditor=e.id_expeditor
WHERE nr_crt=1
UNION ALL
SELECT l_f.nr_factura,l_f.destinatar,l_f.expeditor,l_f.nr_crt + 1,l_f.id_colet,CAST(l_f.lista_colete||' \'||l_f.nr_crt+1||'-'||c.continut_colet AS VARCHAR(500))
FROM col
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN destinatari d ON col.id_destinatar=d.id_destinatar
INNER JOIN expeditori e ON col.id_expeditor=e.id_expeditor
INNER JOIN l_f ON col.nr_factura=l_f.nr_factura
AND col.nr_crt=l_f.nr_crt+ 1),
col AS (
SELECT d_f.*,f.data,f.id_destinatar,f.id_expeditor,c.valoare,c.greutate,cp.judet,ROW_NUMBER()OVER(PARTITION BY d_f.nr_factura ORDER BY d_f.id_colet) AS nr_crt
FROM detalii_facturi d_f
INNER JOIN facturi f ON d_f.nr_factura=f.nr_factura
INNER JOIN destinatari d ON f.id_destinatar=d.id_destinatar
INNER JOIN coduri_postale cp ON d.cod_postal=cp.cod_postal
INNER JOIN colete c ON d_f.id_colet=c.id_colet
WHERE EXTRACT (YEAR FROM data)=2018 AND 
f.id_destinatar BETWEEN 1 AND 95000 AND 
valoare>500 AND greutate<5 AND judet='Iași')
SELECT nr_factura,destinatar,expeditor,MAX(nr_crt) AS numar_colete, MAX(lista_colete) AS lista_colete
FROM l_f
GROUP BY nr_factura,destinatar,expeditor
ORDER BY 1;

/* 36.Lista coletelor expediate de către fiecare expeditor în parte în prima luna a anului 2018. Ce colete a expediat fiecare expeditor în parte în prima luna a anului 2018? */
WITH RECURSIVE c_e(id_expeditor, expeditor, id_colet, nr_crt, lista_colete)AS(
SELECT col.id_expeditor, CAST(CASE WHEN e.prenume IS NOT NULL THEN e.nume||' '||e.prenume ELSE e.nume END AS VARCHAR(500)), col.id_colet, 1, CAST(' \'||col.id_colet||': '||col.continut_colet AS VARCHAR(500))
FROM col 
INNER JOIN expeditori e ON col.id_expeditor=e.id_expeditor
WHERE nr_crt=1
UNION ALL
SELECT c_e.id_expeditor,c_e.expeditor,c_e.id_colet,c_e.nr_crt+1,CAST(c_e.lista_colete||' \'||col.id_colet||'-'||col.continut_colet AS VARCHAR(500))
FROM col
INNER JOIN expeditori e ON col.id_expeditor=e.id_expeditor
INNER JOIN c_e ON col.id_expeditor=c_e.id_expeditor
AND col.nr_crt=c_e.nr_crt+ 1),
col AS (
SELECT c.*, ROW_NUMBER()OVER(PARTITION BY id_expeditor ORDER BY id_colet) AS nr_crt
FROM colete c
WHERE EXTRACT (YEAR FROM data_expediere)=2018 AND 
EXTRACT (MONTH FROM data_expediere)=1)
SELECT id_expeditor,expeditor,MAX(nr_crt) AS numar_colete, MAX(lista_colete) AS lista_colete
FROM c_e
GROUP BY id_expeditor,expeditor
ORDER BY 1;

/*37. Lista coletelor expediate de către fiecare expeditor în parte pe parcursul anului 2018. Ce colete a expediat fiecare expeditor în parte pe parcursul anului 2018 ? */
WITH RECURSIVE c_e(id_expeditor, expeditor, id_colet, nr_crt, lista_colete)AS(
SELECT col.id_expeditor, CAST(CASE WHEN e.prenume IS NOT NULL THEN e.nume||' '||e.prenume||' ('||cp.judet||')' ELSE e.nume||' ('||cp.judet||')' END AS VARCHAR(500)), col.id_colet, 1, CAST(' \'||col.id_colet||': '||col.continut_colet AS VARCHAR(500))
FROM col 
INNER JOIN expeditori e ON col.id_expeditor=e.id_expeditor
INNER JOIN coduri_postale cp ON e.cod_postal=cp.cod_postal
WHERE nr_crt=1
UNION ALL
SELECT c_e.id_expeditor,c_e.expeditor,c_e.id_colet,c_e.nr_crt+1,CAST(c_e.lista_colete||' \'||col.id_colet||'-'||col.continut_colet AS VARCHAR(500))
FROM col
INNER JOIN expeditori e ON col.id_expeditor=e.id_expeditor
INNER JOIN c_e ON col.id_expeditor=c_e.id_expeditor
AND col.nr_crt=c_e.nr_crt+ 1),
col AS (
SELECT c.*, ROW_NUMBER()OVER(PARTITION BY id_expeditor ORDER BY id_colet) AS nr_crt
FROM colete c
WHERE EXTRACT (YEAR FROM data_expediere)=2018)
SELECT id_expeditor,expeditor,MAX(nr_crt) AS numar_colete, MAX(lista_colete) AS lista_colete
FROM c_e
GROUP BY id_expeditor,expeditor
ORDER BY 1;

/* 38.Lista coletelor cu o greutate mai mică de 1kg expediate de către fiecare expeditor,cu excepția celor din București pe parcursul anului 2018. Ce colete cu o greutate mai mică de 1kg a expediat fiecare expeditor,cu excepția celor din București pe parcursul anului 2018? */
WITH RECURSIVE c_e(id_expeditor, expeditor, id_colet, nr_crt, lista_colete)AS(
SELECT col.id_expeditor, CAST(CASE WHEN e.prenume IS NOT NULL THEN e.nume||' '||e.prenume||' ('||cp.judet||')' ELSE e.nume||' ('||cp.judet||')' END AS VARCHAR(500)), col.id_colet, 1, CAST(' \'||col.id_colet||': '||col.continut_colet AS VARCHAR(500))
FROM col 
INNER JOIN expeditori e ON col.id_expeditor=e.id_expeditor
INNER JOIN coduri_postale cp ON e.cod_postal=cp.cod_postal
WHERE nr_crt=1
UNION ALL
SELECT c_e.id_expeditor,c_e.expeditor,c_e.id_colet,c_e.nr_crt+1,CAST(c_e.lista_colete||' \'||col.id_colet||'-'||col.continut_colet AS VARCHAR(500))
FROM col
INNER JOIN expeditori e ON col.id_expeditor=e.id_expeditor
INNER JOIN c_e ON col.id_expeditor=c_e.id_expeditor
AND col.nr_crt=c_e.nr_crt+ 1),
col AS (
SELECT c.*, ROW_NUMBER()OVER(PARTITION BY c.id_expeditor ORDER BY id_colet) AS nr_crt
FROM colete c
INNER JOIN expeditori e ON c.id_expeditor=e.id_expeditor
INNER JOIN coduri_postale cp ON e.cod_postal=cp.cod_postal
WHERE EXTRACT (YEAR FROM data_expediere)=2018 AND greutate<=1 AND judet!='București')
SELECT id_expeditor,expeditor,MAX(nr_crt) AS numar_colete, MAX(lista_colete) AS lista_colete
FROM c_e
GROUP BY id_expeditor,expeditor
ORDER BY 1;

/* 39.Lista coletelor primite de către fiecare destinatar în lunile Februarie, Martie și Aprilie ale anului 2018. Ce colete a primit fiecare destinatar în lunile Februarie, Martie și Aprilie ale anului 2018?*/
WITH RECURSIVE c_d(id_destinatar, destinatar, id_colet, nr_crt, lista_colete)AS(
SELECT col.id_destinatar, CAST(CASE WHEN d.prenume IS NOT NULL THEN d.nume||' '||d.prenume ELSE d.nume END AS VARCHAR(500)), col.id_colet, 1, CAST(' \'||col.id_colet||': '||col.continut_colet AS VARCHAR(500))
FROM col 
INNER JOIN destinatari d ON col.id_destinatar=d.id_destinatar
WHERE nr_crt=1
UNION ALL
SELECT c_d.id_destinatar,c_d.destinatar,c_d.id_colet,c_d.nr_crt+1,CAST(c_d.lista_colete||' \'||col.id_colet||'-'||col.continut_colet AS VARCHAR(500))
FROM col
INNER JOIN destinatari d ON col.id_destinatar=d.id_destinatar
INNER JOIN c_d ON col.id_destinatar=c_d.id_destinatar
AND col.nr_crt=c_d.nr_crt+ 1),
col AS (
SELECT c.*, ROW_NUMBER()OVER(PARTITION BY id_destinatar ORDER BY id_colet) AS nr_crt
FROM colete c
WHERE EXTRACT (YEAR FROM data_primire_colet)=2018 AND 
EXTRACT (MONTH FROM data_primire_colet)BETWEEN 2 AND 4)
SELECT id_destinatar,destinatar,MAX(nr_crt) AS numar_colete, MAX(lista_colete) AS lista_colete
FROM c_d
GROUP BY id_destinatar,destinatar
ORDER BY 1;

/* 40.Lista coletelor primite de către fiecare destinatar persoană juridică în anul 2018. Ce colete a primit fiecare destinatar persoană juridică în anul 2018? */
WITH RECURSIVE c_d(id_destinatar, destinatar, id_colet, nr_crt, lista_colete)AS(
SELECT col.id_destinatar, CAST(CASE WHEN d.prenume IS NOT NULL THEN d.nume||' '||d.prenume ELSE d.nume END AS VARCHAR(500)), col.id_colet, 1, CAST(' \'||col.id_colet||': '||col.continut_colet AS VARCHAR(500))
FROM col 
INNER JOIN destinatari d ON col.id_destinatar=d.id_destinatar
WHERE nr_crt=1
UNION ALL
SELECT c_d.id_destinatar,c_d.destinatar,c_d.id_colet,c_d.nr_crt+1,CAST(c_d.lista_colete||' \'||col.id_colet||'-'||col.continut_colet AS VARCHAR(500))
FROM col
INNER JOIN destinatari d ON col.id_destinatar=d.id_destinatar
INNER JOIN c_d ON col.id_destinatar=c_d.id_destinatar
AND col.nr_crt=c_d.nr_crt+ 1),
col AS (
SELECT c.*, ROW_NUMBER()OVER(PARTITION BY c.id_destinatar ORDER BY id_colet) AS nr_crt
FROM colete c
INNER JOIN destinatari d ON c.id_destinatar=d.id_destinatar
WHERE EXTRACT (YEAR FROM data_primire_colet)=2018 AND d.prenume IS NULL)
SELECT id_destinatar,destinatar,MAX(nr_crt) AS numar_colete, MAX(lista_colete) AS lista_colete
FROM c_d
GROUP BY id_destinatar,destinatar
ORDER BY 1;

/* 41.Lista coletelor primite de către fiecare destinatar persoană juridică, cu excepția celor din județul Iași în anul 2018. Ce colete a primit fiecare destinatar persoană juridică Lista coletelor primite de către fiecare destinatar persoană juridică, cu excepția celor din județul Iași în anul 2018? */
WITH RECURSIVE c_d(id_destinatar, destinatar, id_colet, nr_crt, lista_colete)AS(
SELECT col.id_destinatar, CAST(CASE WHEN d.prenume IS NOT NULL THEN d.nume||' '||d.prenume ELSE d.nume END AS VARCHAR(500)), col.id_colet, 1, CAST(' \'||col.id_colet||': '||col.continut_colet AS VARCHAR(500))
FROM col 
INNER JOIN destinatari d ON col.id_destinatar=d.id_destinatar
WHERE nr_crt=1
UNION ALL
SELECT c_d.id_destinatar,c_d.destinatar,c_d.id_colet,c_d.nr_crt+1,CAST(c_d.lista_colete||' \'||col.id_colet||'-'||col.continut_colet AS VARCHAR(500))
FROM col
INNER JOIN destinatari d ON col.id_destinatar=d.id_destinatar
INNER JOIN c_d ON col.id_destinatar=c_d.id_destinatar
AND col.nr_crt=c_d.nr_crt+ 1),
col AS (
SELECT c.*,cp.judet, ROW_NUMBER()OVER(PARTITION BY c.id_destinatar ORDER BY id_colet) AS nr_crt
FROM colete c
INNER JOIN destinatari d ON c.id_destinatar=d.id_destinatar
INNER JOIN coduri_postale cp ON d.cod_postal=cp.cod_postal
WHERE EXTRACT (YEAR FROM data_primire_colet)=2018 AND d.prenume IS NULL AND judet!='Iași')
SELECT id_destinatar,destinatar,MAX(nr_crt) AS numar_colete, MAX(lista_colete) AS lista_colete
FROM c_d
GROUP BY id_destinatar,destinatar
ORDER BY 1;

/* 42.Lista coletelor ce depășesc valoarea de 2500 de lei și au plecat din fiecare depozit în luna Octombrie 2018. Ce colete ce depășesc valoare de 2500 de lei au plecat din fiecare depozit în luna Octombrie 2018? */
WITH RECURSIVE col_dep(id_depozit_1,depozit, nr_crt, colete) AS(
SELECT col.id_depozit_1,CAST('Depozitul '||col.id_depozit_1||' ('||d.adresa||')' AS VARCHAR(500)),1, CAST ('\1: ' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN depozite d ON col.id_depozit_1=d.id_depozit
WHERE nr_crt=1
UNION ALL
SELECT col.id_depozit_1,col_dep.depozit,col_dep.nr_crt + 1,CAST (col_dep.colete || ' \' || col_dep.nr_crt + 1 || ': ' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN depozite d ON col.id_depozit_1=d.id_depozit
INNER JOIN col_dep ON col.id_depozit_1=col_dep.id_depozit_1
AND col.nr_crt = col_dep.nr_crt+ 1),
col AS (
SELECT t.*, d_t.id_colet,c.valoare,ROW_NUMBER()OVER(PARTITION BY id_depozit_1 ORDER BY d_t.id_colet) AS nr_crt 
FROM transporturi t 
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport 
INNER JOIN colete c ON d_t.id_colet=c.id_colet 
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND
EXTRACT (MONTH FROM data_plecare)=10 AND valoare>2500)
SELECT depozit,MAX(nr_crt) AS nr_colete, MAX(colete) AS lista_colete
FROM col_dep
GROUP BY id_depozit_1,depozit
ORDER BY id_depozit_1;

/* 43.Lista coletelor ce au plecat din fiecare depozit în luna Octombrie 2018 cu un vehicul de tip Mercedes Sprinter. Ce colete au plecat din fiecare depozit în luna Octombrie 2018 cu un vehicul de tip Mercedes Sprinter.*/
WITH RECURSIVE col_dep(id_depozit_1,depozit, nr_crt, colete) AS(
SELECT col.id_depozit_1,CAST('Depozitul '||col.id_depozit_1||' ('||d.adresa||')' AS VARCHAR(500)),1, CAST ('\1: ' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN depozite d ON col.id_depozit_1=d.id_depozit
WHERE nr_crt=1
UNION ALL
SELECT col.id_depozit_1,col_dep.depozit,col_dep.nr_crt + 1,CAST (col_dep.colete || ' \' || col_dep.nr_crt + 1 || ': ' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN depozite d ON col.id_depozit_1=d.id_depozit
INNER JOIN col_dep ON col.id_depozit_1=col_dep.id_depozit_1
AND col.nr_crt = col_dep.nr_crt+ 1),
col AS (
SELECT t.*, d_t.id_colet,v.model,ROW_NUMBER()OVER(PARTITION BY id_depozit_1 ORDER BY id_colet) AS nr_crt 
FROM transporturi t 
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport 
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND
EXTRACT (MONTH FROM data_plecare)=10 AND v.model='Mercedes Sprinter')
SELECT depozit,MAX(nr_crt) AS nr_colete, MAX(colete) AS lista_colete
FROM col_dep
GROUP BY id_depozit_1,depozit
ORDER BY id_depozit_1;

/*44.Lista coletelor ce au plecat din fiecare depozit în anul 2018 cu un vehicul de tip Mercedes Sprinter mai nou de anul 2014. Ce colete au plecat din fiecare depozit în anul 2018 cu un vehicul de tip Mercedes Sprinter mai nou de anul 2014?.*/
WITH RECURSIVE col_dep(id_depozit_1,depozit, nr_crt, colete) AS(
SELECT col.id_depozit_1,CAST('Depozitul '||col.id_depozit_1||' ('||d.adresa||')' AS VARCHAR(500)),1, CAST ('\1: ' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN depozite d ON col.id_depozit_1=d.id_depozit
WHERE nr_crt=1
UNION ALL
SELECT col.id_depozit_1,col_dep.depozit,col_dep.nr_crt + 1,CAST (col_dep.colete || ' \' || col_dep.nr_crt + 1 || ': ' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN depozite d ON col.id_depozit_1=d.id_depozit
INNER JOIN col_dep ON col.id_depozit_1=col_dep.id_depozit_1
AND col.nr_crt = col_dep.nr_crt+ 1),
col AS (
SELECT t.*, d_t.id_colet,v.model,v.an_fabricatie,ROW_NUMBER()OVER(PARTITION BY id_depozit_1 ORDER BY id_colet) AS nr_crt 
FROM transporturi t 
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport 
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND v.model='Mercedes Sprinter' AND v.an_fabricatie>2014)
SELECT depozit,MAX(nr_crt) AS nr_colete, MAX(colete) AS lista_colete
FROM col_dep
GROUP BY id_depozit_1,depozit
ORDER BY id_depozit_1;

/* 45.Lista coletelor ce au plecat din fiecare depozit în anul 2018 cu un vehicul de tip Mercedes Sprinter mai nou de anul 2014 și care au fost conduse de către un șofer ce a efectuat minim 3000 de km. Ce colete au plecat din fiecare depozit în anul 20188 cu un vehicul de tip Mercedes Sprinter mai nou de anul 2005 și care au fost conduse de către un șofer ce a efectuat minim 3000 de km. */
WITH RECURSIVE col_dep(id_depozit_1,depozit, nr_crt, colete) AS(
SELECT col.id_depozit_1,CAST('Depozitul '||col.id_depozit_1||' ('||d.adresa||')' AS VARCHAR(500)),1, CAST ('\1: ' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN depozite d ON col.id_depozit_1=d.id_depozit
WHERE nr_crt=1
UNION ALL
SELECT col.id_depozit_1,col_dep.depozit,col_dep.nr_crt + 1,CAST (col_dep.colete || ' \' || col_dep.nr_crt + 1 || ': ' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN depozite d ON col.id_depozit_1=d.id_depozit
INNER JOIN col_dep ON col.id_depozit_1=col_dep.id_depozit_1
AND col.nr_crt = col_dep.nr_crt+ 1),
col AS (
SELECT t.*, d_t.id_colet,v.model,v.an_fabricatie,s.km_parcursi,ROW_NUMBER()OVER(PARTITION BY id_depozit_1 ORDER BY id_colet) AS nr_crt 
FROM transporturi t 
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul
INNER JOIN soferi s ON t.id_sofer=s.id_sofer
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport 
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND v.model='Mercedes Sprinter' AND v.an_fabricatie>2014 AND s.km_parcursi>3000)
SELECT depozit,MAX(nr_crt) AS nr_colete, MAX(colete) AS lista_colete
FROM col_dep
GROUP BY id_depozit_1,depozit
ORDER BY id_depozit_1;

/* 46.Lista coletelor ce au plecat din fiecare depozit în anul 2018 cu un vehicul de tip Mercedes Sprinter mai nou de anul 2014 și care au fost conduse de către un șofer ce a efectuat minim 3000 de km și nu este din județul Iași. Ce colete au plecat din fiecare depozit în anul 20188 cu un vehicul de tip Mercedes Sprinter mai nou de anul 2005 și care au fost conduse de către un șofer ce a efectuat minim 3000 de km și nu este din județul Iași?*/
WITH RECURSIVE col_dep(id_depozit_1,depozit, nr_crt, colete) AS(
SELECT col.id_depozit_1,CAST('Depozitul '||col.id_depozit_1||' ('||d.adresa||')' AS VARCHAR(500)),1, CAST ('\1: ' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN depozite d ON col.id_depozit_1=d.id_depozit
WHERE nr_crt=1
UNION ALL
SELECT col.id_depozit_1,col_dep.depozit,col_dep.nr_crt + 1,CAST (col_dep.colete || ' \' || col_dep.nr_crt + 1 || ': ' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN depozite d ON col.id_depozit_1=d.id_depozit
INNER JOIN col_dep ON col.id_depozit_1=col_dep.id_depozit_1
AND col.nr_crt = col_dep.nr_crt+ 1),
col AS (
SELECT t.*, d_t.id_colet,v.model,v.an_fabricatie,s.km_parcursi,cp.judet,ROW_NUMBER()OVER(PARTITION BY id_depozit_1 ORDER BY id_colet) AS nr_crt 
FROM transporturi t 
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul
INNER JOIN soferi s ON t.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
INNER JOIN coduri_postale cp ON a.cod_postal=cp.cod_postal
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport 
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND v.model='Mercedes Sprinter' AND v.an_fabricatie>2014 AND s.km_parcursi>3000 AND judet!='Iași')
SELECT depozit,MAX(nr_crt) AS nr_colete, MAX(colete) AS lista_colete
FROM col_dep
GROUP BY id_depozit_1,depozit
ORDER BY id_depozit_1;

/* 47.Lista coletelor transportate din depozit în depozit în luna Iunie 2018. Ce colete au fost transportate din depozit în depozit în luna Iunie 2018? */
WITH RECURSIVE col_dep2(id_depozit_1,id_depozit_2,depozite, nr_crt, colete) AS(
SELECT col.id_depozit_1,col.id_depozit_2,CAST('Depozitul '||col.id_depozit_1||' -> '||'Depozitul '||col.id_depozit_2 AS VARCHAR(500)),1, CAST ('\1-' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
WHERE nr_crt=1
UNION ALL
SELECT col.id_depozit_1,col.id_depozit_2,col_dep2.depozite,col_dep2.nr_crt + 1,CAST (col_dep2.colete || ' \' || col_dep2.nr_crt + 1 || '-' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN col_dep2 ON col.id_depozit_1=col_dep2.id_depozit_1 AND col.id_depozit_2=col_dep2.id_depozit_2
AND col.nr_crt = col_dep2.nr_crt+ 1),
col AS (
SELECT t.*,d_t.id_colet,ROW_NUMBER()OVER(PARTITION BY id_depozit_1,id_depozit_2 ORDER BY id_colet) AS nr_crt
FROM transporturi t 
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport 
WHERE id_depozit_2 IS NOT NULL AND 
EXTRACT (YEAR FROM data_plecare)=2018 AND
EXTRACT (MONTH FROM data_plecare)=6)
SELECT depozite,MAX(nr_crt) AS nr_colete, MAX(colete) AS lista_colete
FROM col_dep2
GROUP BY id_depozit_1,id_depozit_2,depozite
ORDER BY id_depozit_1;

/* 48.Lista coletelor transportate din depozit în depozit în luna Iunie 2018 cu vehicule inmatriculate în anii 2015+. Ce colete au fost transportate din depozit în depozit în luna Iunie 2018 cu vehicule inmatriculate în anii 2015+ ? */
WITH RECURSIVE col_dep2(id_depozit_1,id_depozit_2,depozite, nr_crt, colete) AS(
SELECT col.id_depozit_1,col.id_depozit_2,CAST('Depozitul '||col.id_depozit_1||' -> '||'Depozitul '||col.id_depozit_2 AS VARCHAR(500)),1, CAST ('\1-' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
WHERE nr_crt=1
UNION ALL
SELECT col.id_depozit_1,col.id_depozit_2,col_dep2.depozite,col_dep2.nr_crt + 1,CAST (col_dep2.colete || ' \' || col_dep2.nr_crt + 1 || '-' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN col_dep2 ON col.id_depozit_1=col_dep2.id_depozit_1 AND col.id_depozit_2=col_dep2.id_depozit_2
AND col.nr_crt = col_dep2.nr_crt+ 1),
col AS (
SELECT t.*,d_t.id_colet,v.data_inmatriculare,ROW_NUMBER()OVER(PARTITION BY id_depozit_1,id_depozit_2 ORDER BY id_colet) AS nr_crt
FROM transporturi t 
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport 
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul
WHERE id_depozit_2 IS NOT NULL AND 
EXTRACT (YEAR FROM data_plecare)=2018 AND
EXTRACT (MONTH FROM data_plecare)=6 AND EXTRACT(YEAR FROM data_inmatriculare)>2015)
SELECT depozite,MAX(nr_crt) AS nr_colete, MAX(colete) AS lista_colete
FROM col_dep2
GROUP BY id_depozit_1,id_depozit_2,depozite
ORDER BY id_depozit_1;

/*49.Lista coletelor transportate din depozit în depozit în anul 2018 cu vehicule de tip Dacia Dokker Van, inmatriculate în anii 2015+. Ce colete au fost transportate din depozit în depozit în anul 2018 cu vehicule de tip Dacia Dokker Van, inmatriculate în anii 2015+ ? */
WITH RECURSIVE col_dep2(id_depozit_1,id_depozit_2,depozite, nr_crt, colete) AS(
SELECT col.id_depozit_1,col.id_depozit_2,CAST('Depozitul '||col.id_depozit_1||' -> '||'Depozitul '||col.id_depozit_2 AS VARCHAR(500)),1, CAST ('\1-' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
WHERE nr_crt=1
UNION ALL
SELECT col.id_depozit_1,col.id_depozit_2,col_dep2.depozite,col_dep2.nr_crt + 1,CAST (col_dep2.colete || ' \' || col_dep2.nr_crt + 1 || '-' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN col_dep2 ON col.id_depozit_1=col_dep2.id_depozit_1 AND col.id_depozit_2=col_dep2.id_depozit_2
AND col.nr_crt = col_dep2.nr_crt+ 1),
col AS (
SELECT t.*,d_t.id_colet,v.data_inmatriculare,ROW_NUMBER()OVER(PARTITION BY id_depozit_1,id_depozit_2 ORDER BY id_colet) AS nr_crt
FROM transporturi t 
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport 
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul
WHERE id_depozit_2 IS NOT NULL AND 
EXTRACT (YEAR FROM data_plecare)=2018 AND EXTRACT(YEAR FROM data_inmatriculare)>2015 AND v.model='Dacia Dokker Van' )
SELECT depozite,MAX(nr_crt) AS nr_colete, MAX(colete) AS lista_colete
FROM col_dep2
GROUP BY id_depozit_1,id_depozit_2,depozite
ORDER BY id_depozit_1;

/*50.Lista coletelor transportate din depozit în depozit în anul 2018 cu vehicule de tip Dacia Dokker Van, inmatriculate în anii 2015+ și conduse de către șoferi care nu sunt din județul Cluj. Ce colete au fost transportate din depozit în depozit în anul 2018 cu vehicule de tip Dacia Dokker Van, inmatriculate în anii 2015+ și conduse de către șoferi care nu sunt din județul Cluj? */
WITH RECURSIVE col_dep2(id_depozit_1,id_depozit_2,depozite, nr_crt, colete) AS(
SELECT col.id_depozit_1,col.id_depozit_2,CAST('Depozitul '||col.id_depozit_1||' -> '||'Depozitul '||col.id_depozit_2 AS VARCHAR(500)),1, CAST ('\1-' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
WHERE nr_crt=1
UNION ALL
SELECT col.id_depozit_1,col.id_depozit_2,col_dep2.depozite,col_dep2.nr_crt + 1,CAST (col_dep2.colete || ' \' || col_dep2.nr_crt + 1 || '-' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN col_dep2 ON col.id_depozit_1=col_dep2.id_depozit_1 AND col.id_depozit_2=col_dep2.id_depozit_2
AND col.nr_crt = col_dep2.nr_crt+ 1),
col AS (
SELECT t.*,d_t.id_colet,v.data_inmatriculare,ROW_NUMBER()OVER(PARTITION BY id_depozit_1,id_depozit_2 ORDER BY id_colet) AS nr_crt
FROM transporturi t 
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport 
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul
INNER JOIN soferi s ON t.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
INNER JOIN coduri_postale cp ON a.cod_postal=cp.cod_postal
WHERE id_depozit_2 IS NOT NULL AND 
EXTRACT (YEAR FROM data_plecare)=2018 AND EXTRACT(YEAR FROM data_inmatriculare)>2015 AND v.model='Dacia Dokker Van' AND judet!='Cluj' )
SELECT depozite,MAX(nr_crt) AS nr_colete, MAX(colete) AS lista_colete
FROM col_dep2
GROUP BY id_depozit_1,id_depozit_2,depozite
ORDER BY id_depozit_1;

/* 51.Lista transporturilor efectuate de catre feicare șofer în parte pe parcursul anului 2018. Ce transporturi a efectuat fiecare șofer pe parcursul anului 2018? */
WITH RECURSIVE tr_s(id_sofer, id_angajat, id_transport,sofer, nr_crt, transporturi) AS(
SELECT tran.id_sofer, a.id_angajat,tran.id_transport, CAST(a.nume||' '||a.prenume AS VARCHAR(500)), 1, CAST('\1: '||tran.id_transport||'('||tran.data_plecare||')' AS VARCHAR(500))
FROM tran 
INNER JOIN soferi s ON tran.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
WHERE nr_crt=1
UNION ALL
SELECT tr_s.id_sofer,tr_s.id_angajat,tr_s.id_transport,tr_s.sofer,tr_s.nr_crt + 1,CAST (tr_s.transporturi|| ' \' || tr_s.nr_crt + 1 ||tran.id_transport||'('||tran.data_plecare||')' AS VARCHAR(500))
FROM tran 
INNER JOIN soferi s ON tran.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
INNER JOIN tr_s ON tran.id_sofer=tr_s.id_sofer
AND tran.nr_crt = tr_s.nr_crt+ 1),
tran AS (
SELECT t.*,ROW_NUMBER()OVER(PARTITION BY id_sofer ORDER BY id_transport) AS nr_crt
FROM transporturi t
WHERE EXTRACT (YEAR FROM data_plecare)=2018)
SELECT id_sofer,sofer,MAX(nr_crt) AS nr_transporturi, MAX(transporturi) AS lista_transporturi
FROM tr_s
GROUP BY id_sofer,sofer
ORDER BY id_sofer;

/* 52.Lista transporturilor efectuate de catre fiecare șofer care nu este din județul Iași pe parcursul anului 2018. Ce transporturi a efectuat fiecare șofer care nu este din județul Iași pe parcursul anului 2018? */
WITH RECURSIVE tr_s(id_sofer, id_angajat, id_transport,sofer, nr_crt, transporturi) AS(
SELECT tran.id_sofer, a.id_angajat,tran.id_transport, CAST(a.nume||' '||a.prenume AS VARCHAR(500)), 1, CAST('\1: '||tran.id_transport||'('||tran.data_plecare||')' AS VARCHAR(500))
FROM tran 
INNER JOIN soferi s ON tran.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
WHERE nr_crt=1
UNION ALL
SELECT tr_s.id_sofer,tr_s.id_angajat,tr_s.id_transport,tr_s.sofer,tr_s.nr_crt + 1,CAST (tr_s.transporturi|| ' \' || tr_s.nr_crt + 1 ||tran.id_transport||'('||tran.data_plecare||')' AS VARCHAR(500))
FROM tran 
INNER JOIN soferi s ON tran.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
INNER JOIN tr_s ON tran.id_sofer=tr_s.id_sofer
AND tran.nr_crt = tr_s.nr_crt+ 1),
tran AS (
SELECT t.*,cp.judet,ROW_NUMBER()OVER(PARTITION BY t.id_sofer ORDER BY id_transport) AS nr_crt
FROM transporturi t
INNER JOIN soferi s ON t.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
INNER JOIN coduri_postale cp ON a.cod_postal=cp.cod_postal
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND judet!='Iași')
SELECT id_sofer,sofer,MAX(nr_crt) AS nr_transporturi, MAX(transporturi) AS lista_transporturi
FROM tr_s
GROUP BY id_sofer,sofer
ORDER BY id_sofer;

/* 53.Lista transporturilor efectuate cu fiecare mijloc de transport în anul 2018. Ce transporturi au fost efectuate cu fiecare mijloc de transport în anul 2018? */
WITH RECURSIVE tr_v(id_vehicul, model, id_transport,linie, transporturi) AS(
SELECT tran.id_vehicul, CAST(v.model||' ('||v.nr_inmatriculare||')' AS VARCHAR(500)),tran.id_transport,1,CAST('\1: '||tran.id_transport||'('||tran.data_plecare||')' AS VARCHAR(500))
FROM tran 
INNER JOIN vehicule v ON tran.id_vehicul=v.id_vehicul
WHERE linie=1
UNION ALL
SELECT tr_v.id_vehicul,tr_v.model,tr_v.id_transport,tr_v.linie + 1,CAST (tr_v.transporturi|| ' \' || tr_v.linie + 1 ||tran.id_transport||'('||tran.data_plecare||')' AS VARCHAR(500))
FROM tran 
INNER JOIN vehicule v ON tran.id_vehicul=v.id_vehicul
INNER JOIN tr_v ON tran.id_vehicul=tr_v.id_vehicul
AND tran.linie = tr_v.linie+ 1),
tran AS (
SELECT t.*,ROW_NUMBER()OVER(PARTITION BY id_vehicul ORDER BY id_transport) AS linie
FROM transporturi t
WHERE EXTRACT (YEAR FROM data_plecare)=2018)
SELECT id_vehicul,model,MAX(linie) AS nr_transporturi, MAX(transporturi) AS lista_transporturi
FROM tr_v
GROUP BY id_vehicul,model
ORDER BY id_vehicul;

/* 54.Lista transporturilor efectuate cu fiecare mijloc de transport în anul 2018 cu vehicule ce au fost inmatriculate cel putin în anul 2015. Ce transporturi au fost efectuate cu fiecare mijloc de transport în anul 2018 cu vehicule ce au fost inmatriculate cel putin în anul 2015 */
WITH RECURSIVE tr_v(id_vehicul, model, id_transport,linie, transporturi) AS(
SELECT tran.id_vehicul, CAST(v.model||' ('||v.nr_inmatriculare||')' AS VARCHAR(500)),tran.id_transport,1,CAST('\1: '||tran.id_transport||'('||tran.data_plecare||')' AS VARCHAR(500))
FROM tran 
INNER JOIN vehicule v ON tran.id_vehicul=v.id_vehicul
WHERE linie=1
UNION ALL
SELECT tr_v.id_vehicul,tr_v.model,tr_v.id_transport,tr_v.linie + 1,CAST (tr_v.transporturi|| ' \' || tr_v.linie + 1 ||tran.id_transport||'('||tran.data_plecare||')' AS VARCHAR(500))
FROM tran 
INNER JOIN vehicule v ON tran.id_vehicul=v.id_vehicul
INNER JOIN tr_v ON tran.id_vehicul=tr_v.id_vehicul
AND tran.linie = tr_v.linie+ 1),
tran AS (
SELECT t.*,data_inmatriculare,ROW_NUMBER()OVER(PARTITION BY t.id_vehicul ORDER BY id_transport) AS linie
FROM transporturi t
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND EXTRACT(YEAR FROM data_inmatriculare)>=2015)
SELECT id_vehicul,model,MAX(linie) AS nr_transporturi, MAX(transporturi) AS lista_transporturi
FROM tr_v
GROUP BY id_vehicul,model
ORDER BY id_vehicul;

/* 55.Lista transporturilor ce au plecat din fiecare depozit în luna Martie 2018. Ce transporturi au plecat din fiecare depozit în luna Martie 2018? */
WITH RECURSIVE tr_dep(id_depozit, depozit, id_transport,linie, transporturi) AS(
SELECT tran.id_depozit_1, CAST('Depozitul ' ||tran.id_depozit_1||'('||adresa||', '||c_p.judet||')' AS VARCHAR(500)) AS depozit,tran.id_transport,1,CAST('\1: '||tran.id_transport||'('||tran.data_plecare||')' AS VARCHAR(500))
FROM tran
INNER JOIN depozite d ON tran.id_depozit_1=d.id_depozit
INNER JOIN coduri_postale c_p ON d.cod_postal=c_p.cod_postal
WHERE linie=1
UNION ALL
SELECT tr_dep.id_depozit,tr_dep.depozit,tr_dep.id_transport,tr_dep.linie + 1,CAST (tr_dep.transporturi|| ' \' || tr_dep.linie + 1 ||tran.id_transport||'('||tran.data_plecare||')' AS VARCHAR(500))
FROM tran
INNER JOIN depozite d ON tran.id_depozit_1=d.id_depozit
INNER JOIN coduri_postale c_p ON d.cod_postal=c_p.cod_postal
INNER JOIN tr_dep ON tran.id_depozit_1=tr_dep.id_depozit
AND tran.linie = tr_dep.linie+ 1),
tran AS (
SELECT t.*,ROW_NUMBER()OVER(PARTITION BY id_depozit_1 ORDER BY id_transport) AS linie
FROM transporturi t
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND 
EXTRACT (MONTH FROM data_plecare)=3)
SELECT id_depozit,depozit,MAX(linie) AS nr_transporturi, MAX(transporturi) AS lista_transporturi
FROM tr_dep
GROUP BY id_depozit,depozit
ORDER BY id_depozit;

/* 56.Lista transporturilor ce au plecat din fiecare depozit în anul 2018. Ce transporturi au plecat din fiecare depozit în anul 2018?*/
WITH RECURSIVE tr_dep(id_depozit, depozit, id_transport,linie, transporturi) AS(
SELECT tran.id_depozit_1, CAST('Depozitul ' ||tran.id_depozit_1||'('||adresa||', '||c_p.judet||')' AS VARCHAR(500)) AS depozit,tran.id_transport,1,CAST('\1: '||tran.id_transport||'('||tran.data_plecare||')' AS VARCHAR(500))
FROM tran
INNER JOIN depozite d ON tran.id_depozit_1=d.id_depozit
INNER JOIN coduri_postale c_p ON d.cod_postal=c_p.cod_postal
WHERE linie=1
UNION ALL
SELECT tr_dep.id_depozit,tr_dep.depozit,tr_dep.id_transport,tr_dep.linie + 1,CAST (tr_dep.transporturi|| ' \' || tr_dep.linie + 1 ||tran.id_transport||'('||tran.data_plecare||')' AS VARCHAR(500))
FROM tran
INNER JOIN depozite d ON tran.id_depozit_1=d.id_depozit
INNER JOIN coduri_postale c_p ON d.cod_postal=c_p.cod_postal
INNER JOIN tr_dep ON tran.id_depozit_1=tr_dep.id_depozit
AND tran.linie = tr_dep.linie+ 1),
tran AS (
SELECT t.*,ROW_NUMBER()OVER(PARTITION BY id_depozit_1 ORDER BY id_transport) AS linie
FROM transporturi t
WHERE EXTRACT (YEAR FROM data_plecare)=2018)
SELECT id_depozit,depozit,MAX(linie) AS nr_transporturi, MAX(transporturi) AS lista_transporturi
FROM tr_dep
GROUP BY id_depozit,depozit
ORDER BY id_depozit;

/* 57.Lista transporturilor ce au plecat din fiecare depozit în anul 2018 cu vehicule de tip Ford Transit ce au fost înmatriculate începând cu anul 2015. Ce transporturi au plecat din fiecare depozit în anul 2018 cu vehicule de tip Ford Transit ce au fost înmatriculate începând cu anul 2015 ?*/
WITH RECURSIVE tr_dep(id_depozit, depozit, id_transport,linie, transporturi) AS(
SELECT tran.id_depozit_1, CAST('Depozitul ' ||tran.id_depozit_1||'('||adresa||', '||c_p.judet||')' AS VARCHAR(500)) AS depozit,tran.id_transport,1,CAST('\1: '||tran.id_transport||'('||tran.data_plecare||')' AS VARCHAR(500))
FROM tran
INNER JOIN depozite d ON tran.id_depozit_1=d.id_depozit
INNER JOIN coduri_postale c_p ON d.cod_postal=c_p.cod_postal
WHERE linie=1
UNION ALL
SELECT tr_dep.id_depozit,tr_dep.depozit,tr_dep.id_transport,tr_dep.linie + 1,CAST (tr_dep.transporturi|| ' \' || tr_dep.linie + 1 ||tran.id_transport||'('||tran.data_plecare||')' AS VARCHAR(500))
FROM tran
INNER JOIN depozite d ON tran.id_depozit_1=d.id_depozit
INNER JOIN coduri_postale c_p ON d.cod_postal=c_p.cod_postal
INNER JOIN tr_dep ON tran.id_depozit_1=tr_dep.id_depozit
AND tran.linie = tr_dep.linie+ 1),
tran AS (
SELECT t.*,v.model,data_inmatriculare,ROW_NUMBER()OVER(PARTITION BY id_depozit_1 ORDER BY id_transport) AS linie
FROM transporturi t
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND EXTRACT(YEAR FROM data_inmatriculare)>=2015 AND v.model='Ford Transit')
SELECT id_depozit,depozit,MAX(linie) AS nr_transporturi, MAX(transporturi) AS lista_transporturi
FROM tr_dep
GROUP BY id_depozit,depozit
ORDER BY id_depozit;

/* 58.Lista transporturilor ce au plecat din fiecare depozit în anul 2018 cu vehicule de tip Ford Transit ce au fost înmatriculate începând cu anul 2015 și care au fost conduse de către șoferi care nu sunt din județul Bihor. Ce transporturi au plecat din fiecare depozit în anul 2018 cu vehicule de tip Ford Transit ce au fost înmatriculate începând cu anul 2015 și care au fost conduse de către șoferi care nu sunt din județul Bihor. ?*/
WITH RECURSIVE tr_dep(id_depozit, depozit, id_transport,linie, transporturi) AS(
SELECT tran.id_depozit_1, CAST('Depozitul ' ||tran.id_depozit_1||'('||adresa||', '||c_p.judet||')' AS VARCHAR(500)) AS depozit,tran.id_transport,1,CAST('\1: '||tran.id_transport||'('||tran.data_plecare||')' AS VARCHAR(500))
FROM tran
INNER JOIN depozite d ON tran.id_depozit_1=d.id_depozit
INNER JOIN coduri_postale c_p ON d.cod_postal=c_p.cod_postal
WHERE linie=1
UNION ALL
SELECT tr_dep.id_depozit,tr_dep.depozit,tr_dep.id_transport,tr_dep.linie + 1,CAST (tr_dep.transporturi|| ' \' || tr_dep.linie + 1 ||tran.id_transport||'('||tran.data_plecare||')' AS VARCHAR(500))
FROM tran
INNER JOIN depozite d ON tran.id_depozit_1=d.id_depozit
INNER JOIN coduri_postale c_p ON d.cod_postal=c_p.cod_postal
INNER JOIN tr_dep ON tran.id_depozit_1=tr_dep.id_depozit
AND tran.linie = tr_dep.linie+ 1),
tran AS (
SELECT t.*,v.model,data_inmatriculare,judet,ROW_NUMBER()OVER(PARTITION BY id_depozit_1 ORDER BY id_transport) AS linie
FROM transporturi t
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul
INNER JOIN soferi s ON t.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
INNER JOIN coduri_postale c_p ON a.cod_postal=c_p.cod_postal
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND EXTRACT(YEAR FROM data_inmatriculare)>=2015 AND v.model='Ford Transit' AND judet!='Bihor')
SELECT id_depozit,depozit,MAX(linie) AS nr_transporturi, MAX(transporturi) AS lista_transporturi
FROM tr_dep
GROUP BY id_depozit,depozit
ORDER BY id_depozit;

/* 59.Lista coletelor returnate în anul 2018. Ce colete au fost returnate în anul 2018? */
WITH RECURSIVE col_ret(id_retur,retur,nr_factura, nr_crt, id_colet, lista_colete)  AS(
SELECT ret.id_retur,CAST(ret.id_retur||' ('||ret.motiv_retur||')' AS VARCHAR(500)),ret.nr_factura, 1, ret.id_colet, CAST(' \1-'||c.continut_colet AS VARCHAR(500))
FROM ret
INNER JOIN colete c ON ret.id_colet=c.id_colet
WHERE nr_crt=1
UNION ALL
SELECT col_ret.id_retur,col_ret.retur,col_ret.nr_factura,col_ret.nr_crt+1,col_ret.id_colet,CAST (col_ret.lista_colete|| ' \' || col_ret.nr_crt + 1 ||'-'||c.continut_colet AS VARCHAR(500))
FROM ret
INNER JOIN colete c ON ret.id_colet=c.id_colet
INNER JOIN col_ret ON ret.id_retur=col_ret.id_retur
AND ret.nr_crt = col_ret.nr_crt+ 1),
ret AS (
SELECT r.*,d_f.id_colet,ROW_NUMBER()OVER(PARTITION BY id_retur ORDER BY d_f.id_colet) AS nr_crt
FROM retururi r
INNER JOIN detalii_facturi d_f ON r.nr_factura=d_f.nr_factura
WHERE EXTRACT (YEAR FROM data_retur)=2018)
SELECT retur,MAX(nr_crt) AS numar_colete, MAX(lista_colete) AS lista_colete
FROM col_ret
GROUP BY id_retur,retur
ORDER BY id_retur;

/* 60.Lista coletelor returnate în anul 2018 de către destinatarii care nu sunt din județul Mureș. Ce colete au fost returnate în anul 2018 de către destinatarii care nu sunt din județul Mureș */
WITH RECURSIVE col_ret(id_retur,retur,nr_factura, nr_crt, id_colet, lista_colete)  AS(
SELECT ret.id_retur,CAST(ret.id_retur||' ('||ret.motiv_retur||')' AS VARCHAR(500)),ret.nr_factura, 1, ret.id_colet, CAST(' \1-'||c.continut_colet AS VARCHAR(500))
FROM ret
INNER JOIN colete c ON ret.id_colet=c.id_colet
WHERE nr_crt=1
UNION ALL
SELECT col_ret.id_retur,col_ret.retur,col_ret.nr_factura,col_ret.nr_crt+1,col_ret.id_colet,CAST (col_ret.lista_colete|| ' \' || col_ret.nr_crt + 1 ||'-'||c.continut_colet AS VARCHAR(500))
FROM ret
INNER JOIN colete c ON ret.id_colet=c.id_colet
INNER JOIN col_ret ON ret.id_retur=col_ret.id_retur
AND ret.nr_crt = col_ret.nr_crt+ 1),
ret AS (
SELECT r.*,d_f.id_colet,judet,ROW_NUMBER()OVER(PARTITION BY id_retur ORDER BY d_f.id_colet) AS nr_crt
FROM retururi r
INNER JOIN detalii_facturi d_f ON r.nr_factura=d_f.nr_factura
INNER JOIN facturi f ON d_f.nr_factura=f.nr_factura
INNER JOIN destinatari d ON f.id_destinatar=d.id_destinatar
INNER JOIN coduri_postale c_p ON d.cod_postal=c_p.cod_postal
WHERE EXTRACT (YEAR FROM data_retur)=2018 AND judet!='Mureș')
SELECT retur,MAX(nr_crt) AS numar_colete, MAX(lista_colete) AS lista_colete
FROM col_ret
GROUP BY id_retur,retur
ORDER BY id_retur;

/* 61.Lista transporturilor din depozit în depozit efectuate anul 2018. Ce transporturi din depozit în depozit au fost efectuate în anul 2018? */
WITH RECURSIVE tr_dep2(id_depozit_1,id_depozit_2, depozite, id_transport,linie, transporturi) AS(
SELECT tran.id_depozit_1,tran.id_depozit_2, CAST('Depozitul '||tran.id_depozit_1||' -> '||'Depozitul '||tran.id_depozit_2 AS VARCHAR(500)),tran.id_transport,1,CAST('\1: '||tran.id_transport||'('||tran.data_plecare||')' AS VARCHAR(500))
FROM tran
WHERE linie=1
UNION ALL
SELECT tr_dep2.id_depozit_1,tr_dep2.id_depozit_2,tr_dep2.depozite,tr_dep2.id_transport,tr_dep2.linie + 1,CAST (tr_dep2.transporturi|| ' \' || tr_dep2.linie + 1 ||tran.id_transport||'('||tran.data_plecare||')' AS VARCHAR(500))
FROM tran
INNER JOIN tr_dep2 ON tran.id_depozit_1=tr_dep2.id_depozit_1 AND tran.id_depozit_2=tr_dep2.id_depozit_2
AND tran.linie = tr_dep2.linie+ 1),
tran AS (
SELECT t.*,ROW_NUMBER()OVER(PARTITION BY id_depozit_1,id_depozit_2 ORDER BY id_transport) AS linie
FROM transporturi t
WHERE id_depozit_2 IS NOT NULL AND 
EXTRACT (YEAR FROM data_plecare)=2018)
SELECT depozite,MAX(linie) AS nr_transporturi, MAX(transporturi) AS lista_transporturi
FROM tr_dep2
GROUP BY id_depozit_1,depozite
ORDER BY id_depozit_1;

/* 62.Lista transporturilor din depozit în depozit efectuate anul 2018 cu un vehicul de tip Iveco Daily. Ce transporturi din depozit în depozit au fost efectuate în anul 2018 cu un vehicul de tip Iveco Daily? */
WITH RECURSIVE tr_dep2(id_depozit_1,id_depozit_2, depozite, id_transport,linie, transporturi) AS(
SELECT tran.id_depozit_1,tran.id_depozit_2, CAST('Depozitul '||tran.id_depozit_1||' -> '||'Depozitul '||tran.id_depozit_2 AS VARCHAR(500)),tran.id_transport,1,CAST('\1: '||tran.id_transport||'('||tran.data_plecare||')' AS VARCHAR(500))
FROM tran
WHERE linie=1
UNION ALL
SELECT tr_dep2.id_depozit_1,tr_dep2.id_depozit_2,tr_dep2.depozite,tr_dep2.id_transport,tr_dep2.linie + 1,CAST (tr_dep2.transporturi|| ' \' || tr_dep2.linie + 1 ||tran.id_transport||'('||tran.data_plecare||')' AS VARCHAR(500))
FROM tran
INNER JOIN tr_dep2 ON tran.id_depozit_1=tr_dep2.id_depozit_1 AND tran.id_depozit_2=tr_dep2.id_depozit_2
AND tran.linie = tr_dep2.linie+ 1),
tran AS (
SELECT t.*,v.model,ROW_NUMBER()OVER(PARTITION BY id_depozit_1,id_depozit_2 ORDER BY id_transport) AS linie
FROM transporturi t
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul
WHERE id_depozit_2 IS NOT NULL AND 
EXTRACT (YEAR FROM data_plecare)=2018 AND v.model='Iveco Daily' )
SELECT depozite,MAX(linie) AS nr_transporturi, MAX(transporturi) AS lista_transporturi
FROM tr_dep2
GROUP BY id_depozit_1,depozite
ORDER BY id_depozit_1;

/* 63.Lista transporturilor din depozit în depozit efectuate anul 2018 cu un vehicul de tip Iveco Daily care au fost înmatriculate începând cu anul 2010. Ce transporturi din depozit în depozit au fost efectuate în anul 2018 cu un vehicul de tip Iveco Daily care au fost înmatriculate începând cu anul 2010? */
WITH RECURSIVE tr_dep2(id_depozit_1,id_depozit_2, depozite, id_transport,linie, transporturi) AS(
SELECT tran.id_depozit_1,tran.id_depozit_2, CAST('Depozitul '||tran.id_depozit_1||' -> '||'Depozitul '||tran.id_depozit_2 AS VARCHAR(500)),tran.id_transport,1,CAST('\1: '||tran.id_transport||'('||tran.data_plecare||')' AS VARCHAR(500))
FROM tran
WHERE linie=1
UNION ALL
SELECT tr_dep2.id_depozit_1,tr_dep2.id_depozit_2,tr_dep2.depozite,tr_dep2.id_transport,tr_dep2.linie + 1,CAST (tr_dep2.transporturi|| ' \' || tr_dep2.linie + 1 ||tran.id_transport||'('||tran.data_plecare||')' AS VARCHAR(500))
FROM tran
INNER JOIN tr_dep2 ON tran.id_depozit_1=tr_dep2.id_depozit_1 AND tran.id_depozit_2=tr_dep2.id_depozit_2
AND tran.linie = tr_dep2.linie+ 1),
tran AS (
SELECT t.*,v.model,v.data_inmatriculare,ROW_NUMBER()OVER(PARTITION BY id_depozit_1,id_depozit_2 ORDER BY id_transport) AS linie
FROM transporturi t
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul
WHERE id_depozit_2 IS NOT NULL AND 
EXTRACT (YEAR FROM data_plecare)=2018 AND v.model='Iveco Daily' AND EXTRACT(YEAR FROM data_inmatriculare)>=2010)
SELECT depozite,MAX(linie) AS nr_transporturi, MAX(transporturi) AS lista_transporturi
FROM tr_dep2
GROUP BY id_depozit_1,depozite
ORDER BY id_depozit_1;

/* 64.Lista transporturilor din depozit în depozit efectuate anul 2018 cu un vehicul de tip Iveco Daily și care au fost conduse de șoferi din fiecare județ, cu excepția celor din Vaslui. Ce transporturi din depozit în depozit au fost efectuate în anul 2018 vehicul de tip Iveco Daily și care au fost conduse de șoferi din fiecare județ, cu excepția celor din Vaslui? */
WITH RECURSIVE tr_dep2(id_depozit_1,id_depozit_2, depozite, id_transport,linie, transporturi) AS(
SELECT tran.id_depozit_1,tran.id_depozit_2, CAST('Depozitul '||tran.id_depozit_1||' -> '||'Depozitul '||tran.id_depozit_2 AS VARCHAR(500)),tran.id_transport,1,CAST('\1: '||tran.id_transport||'('||tran.data_plecare||')' AS VARCHAR(500))
FROM tran
WHERE linie=1
UNION ALL
SELECT tr_dep2.id_depozit_1,tr_dep2.id_depozit_2,tr_dep2.depozite,tr_dep2.id_transport,tr_dep2.linie + 1,CAST (tr_dep2.transporturi|| ' \' || tr_dep2.linie + 1 ||tran.id_transport||'('||tran.data_plecare||')' AS VARCHAR(500))
FROM tran
INNER JOIN tr_dep2 ON tran.id_depozit_1=tr_dep2.id_depozit_1 AND tran.id_depozit_2=tr_dep2.id_depozit_2
AND tran.linie = tr_dep2.linie+ 1),
tran AS (
SELECT t.*,v.model,c_p.judet,ROW_NUMBER()OVER(PARTITION BY id_depozit_1,id_depozit_2 ORDER BY id_transport) AS linie
FROM transporturi t
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul
INNER JOIN soferi s ON t.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
INNER JOIN coduri_postale c_p ON a.cod_postal=c_p.cod_postal
WHERE id_depozit_2 IS NOT NULL AND 
EXTRACT (YEAR FROM data_plecare)=2018 AND v.model='Iveco Daily' AND judet!='Vaslui')
SELECT depozite,MAX(linie) AS nr_transporturi, MAX(transporturi) AS lista_transporturi
FROM tr_dep2
GROUP BY id_depozit_1,depozite
ORDER BY id_depozit_1;

/* 65.Lista tuturor coletelor expediată de fiecare persoană jurdică(firmă) din județul Suceava în anul 2018. Ce colete a expediat fiecare persoană juridică(firmă) din județul Suceava în anul 2018? */
WITH RECURSIVE e_jur(id_expeditor, expeditor,id_colet, nr_crt, lista_colete)AS(
SELECT col.id_expeditor, e.nume, col.id_colet, 1, CAST(' \'||col.id_colet||': '||col.continut_colet AS VARCHAR(500))
FROM col 
INNER JOIN expeditori e ON col.id_expeditor=e.id_expeditor
WHERE nr_crt=1
UNION ALL
SELECT e_jur.id_expeditor,e_jur.expeditor,e_jur.id_colet,e_jur.nr_crt+1,CAST(e_jur.lista_colete||' \'||col.id_colet||'-'||col.continut_colet AS VARCHAR(500))
FROM col
INNER JOIN expeditori e ON col.id_expeditor=e.id_expeditor
INNER JOIN e_jur ON col.id_expeditor=e_jur.id_expeditor
AND col.nr_crt=e_jur.nr_crt+ 1),
col AS (
SELECT c.*,e.prenume,c_p.judet, ROW_NUMBER()OVER(PARTITION BY c.id_expeditor ORDER BY id_colet) AS nr_crt
FROM colete c
INNER JOIN expeditori e ON c.id_expeditor=e.id_expeditor
INNER JOIN coduri_postale c_p ON e.cod_postal=c_p.cod_postal
WHERE e.prenume IS NULL AND c_p.judet='Suceava' AND
EXTRACT (YEAR FROM data_expediere)=2018)
SELECT id_expeditor,expeditor,MAX(nr_crt) AS numar_colete, MAX(lista_colete) AS lista_colete
FROM e_jur
GROUP BY id_expeditor,expeditor
ORDER BY 1;

/* 66.Lista tuturor coletelor expediată de fiecare persoană jurdică(firmă) în anul 2018. Ce colete a expediat fiecare persoană juridică(firmă) în anul 2018? */
WITH RECURSIVE e_jur(id_expeditor, expeditor,id_colet, nr_crt, lista_colete)AS(
SELECT col.id_expeditor, e.nume, col.id_colet, 1, CAST(' \'||col.id_colet||': '||col.continut_colet AS VARCHAR(500))
FROM col 
INNER JOIN expeditori e ON col.id_expeditor=e.id_expeditor
WHERE nr_crt=1
UNION ALL
SELECT e_jur.id_expeditor,e_jur.expeditor,e_jur.id_colet,e_jur.nr_crt+1,CAST(e_jur.lista_colete||' \'||col.id_colet||'-'||col.continut_colet AS VARCHAR(500))
FROM col
INNER JOIN expeditori e ON col.id_expeditor=e.id_expeditor
INNER JOIN e_jur ON col.id_expeditor=e_jur.id_expeditor
AND col.nr_crt=e_jur.nr_crt+ 1),
col AS (
SELECT c.*,e.prenume, ROW_NUMBER()OVER(PARTITION BY c.id_expeditor ORDER BY id_colet) AS nr_crt
FROM colete c
INNER JOIN expeditori e ON c.id_expeditor=e.id_expeditor
WHERE e.prenume IS NULL AND
EXTRACT (YEAR FROM data_expediere)=2018)
SELECT id_expeditor,expeditor,MAX(nr_crt) AS numar_colete, MAX(lista_colete) AS lista_colete
FROM e_jur
GROUP BY id_expeditor,expeditor
ORDER BY 1;

/* 67.Lista tuturor coletelor expediată de fiecare persoană jurdică(firmă) în perioada xx.01-xx.04. Ce colete a expediat fiecare persoană juridică(firmă) în perioada xx.01-xx.04.2018? */
WITH RECURSIVE e_jur(id_expeditor, expeditor,id_colet, nr_crt, lista_colete)AS(
SELECT col.id_expeditor, e.nume, col.id_colet, 1, CAST(' \'||col.id_colet||': '||col.continut_colet AS VARCHAR(500))
FROM col 
INNER JOIN expeditori e ON col.id_expeditor=e.id_expeditor
WHERE nr_crt=1
UNION ALL
SELECT e_jur.id_expeditor,e_jur.expeditor,e_jur.id_colet,e_jur.nr_crt+1,CAST(e_jur.lista_colete||' \'||col.id_colet||'-'||col.continut_colet AS VARCHAR(500))
FROM col
INNER JOIN expeditori e ON col.id_expeditor=e.id_expeditor
INNER JOIN e_jur ON col.id_expeditor=e_jur.id_expeditor
AND col.nr_crt=e_jur.nr_crt+ 1),
col AS (
SELECT c.*,e.prenume, ROW_NUMBER()OVER(PARTITION BY c.id_expeditor ORDER BY id_colet) AS nr_crt
FROM colete c
INNER JOIN expeditori e ON c.id_expeditor=e.id_expeditor
WHERE e.prenume IS NULL AND 
EXTRACT (YEAR FROM data_expediere)=2018 AND 
EXTRACT (MONTH FROM data_expediere) BETWEEN 1 AND 4)
SELECT id_expeditor,expeditor,MAX(nr_crt) AS numar_colete, MAX(lista_colete) AS lista_colete
FROM e_jur
GROUP BY id_expeditor,expeditor
ORDER BY 1;

/* 68.Lista tututor incidentelor raportate de fiecare șofer în parte în anul 2018. Ce incidente a raportat fiecare șofer în parte în anul 2018? */
WITH RECURSIVE i_s(id_sofer, sofer, id_incident, nr_crt, lista_incidente)AS(
SELECT i.id_sofer, CAST(a.nume||' '||a.prenume AS VARCHAR(500)), i.id_incident, 1, CAST('\1: '||i.tip_incident||'('||i.data_raportarii||')' AS VARCHAR(500))
FROM incidente i
INNER JOIN soferi s ON i.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
WHERE EXTRACT (YEAR FROM data_raportarii)=2018
UNION ALL
SELECT i_s.id_sofer, sofer, i.id_incident, i_s.nr_crt+1, CAST(i_s.lista_incidente||' \'||i_s.nr_crt+1||': '||i.tip_incident||'('||i.data_raportarii||')' AS VARCHAR(500)) 
FROM incidente i
INNER JOIN soferi s ON i.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
INNER JOIN i_s ON i.id_sofer=i_s.id_sofer AND i.id_incident=
(SELECT MIN(id_incident)
FROM incidente
WHERE id_sofer=i_s.id_sofer AND id_incident>i_s.id_incident)
WHERE EXTRACT (YEAR FROM data_raportarii)=2018)
SELECT id_sofer, sofer,nr_crt AS numar_incidente, lista_incidente
FROM i_s
WHERE (id_sofer, sofer, nr_crt) IN
(SELECT id_sofer, sofer, max(nr_crt)
FROM i_s
GROUP BY id_sofer, sofer)
ORDER BY 1;

/* 69.Lista tututor incidentelor raportate de fiecare șofer din județul Iași în anul 2018. Ce incidente a raportat fiecare șofer din județul Iași în anul 2018? */
WITH RECURSIVE i_s(id_sofer, sofer, judet,id_incident, nr_crt, lista_incidente)AS(
SELECT i.id_sofer, CAST(a.nume||' '||a.prenume AS VARCHAR(500)),c_p.judet, i.id_incident, 1, CAST('\1: '||i.tip_incident||'('||i.data_raportarii||')' AS VARCHAR(500))
FROM incidente i
INNER JOIN soferi s ON i.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
INNER JOIN coduri_postale c_p ON a.cod_postal=c_p.cod_postal
WHERE EXTRACT (YEAR FROM data_raportarii)=2018 AND c_p.judet='Iași'
UNION ALL
SELECT i_s.id_sofer, sofer,i_s.judet,i.id_incident, i_s.nr_crt+1, CAST(i_s.lista_incidente||' \'||i_s.nr_crt+1||': '||i.tip_incident||'('||i.data_raportarii||')' AS VARCHAR(500)) 
FROM incidente i
INNER JOIN soferi s ON i.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
INNER JOIN coduri_postale c_p ON a.cod_postal=c_p.cod_postal
INNER JOIN i_s ON i.id_sofer=i_s.id_sofer AND i.id_incident=
(SELECT MIN(id_incident)
FROM incidente
WHERE id_sofer=i_s.id_sofer AND id_incident>i_s.id_incident)
WHERE EXTRACT (YEAR FROM data_raportarii)=2018 AND c_p.judet='Iași')
SELECT id_sofer, sofer,nr_crt AS numar_incidente, lista_incidente
FROM i_s
WHERE (id_sofer, sofer, nr_crt) IN
(SELECT id_sofer, sofer, max(nr_crt)
FROM i_s
GROUP BY id_sofer, sofer)
ORDER BY 1;

/* 70.Lista coletelor primite de către fiecare persoană juridică(firmă) în perioada xx.02-xx.06.2018. Ce colete a primit fiecare persoană juridică(firmă) în perioadă xx.02-xx.06.2018? */
WITH RECURSIVE d_jur(id_destinatar, destinatar,id_colet, nr_crt, lista_colete)AS(
SELECT col.id_destinatar, d.nume, col.id_colet, 1, CAST(' \'||col.id_colet||': '||col.continut_colet AS VARCHAR(500))
FROM col 
INNER JOIN destinatari d ON col.id_destinatar=d.id_destinatar
WHERE nr_crt=1
UNION ALL
SELECT d_jur.id_destinatar,d_jur.destinatar,d_jur.id_colet,d_jur.nr_crt+1,CAST(d_jur.lista_colete||' \'||col.id_colet||'-'||col.continut_colet AS VARCHAR(500))
FROM col
INNER JOIN destinatari d ON col.id_destinatar=d.id_destinatar
INNER JOIN d_jur ON col.id_destinatar=d_jur.id_destinatar
AND col.nr_crt=d_jur.nr_crt+ 1),
col AS (
SELECT c.*,d.prenume, ROW_NUMBER()OVER(PARTITION BY c.id_destinatar ORDER BY id_colet) AS nr_crt
FROM colete c
INNER JOIN destinatari d ON c.id_destinatar=d.id_destinatar
WHERE d.prenume IS NULL AND 
EXTRACT (YEAR FROM data_expediere)=2018 AND 
EXTRACT (MONTH FROM data_expediere) BETWEEN 2 AND 6)
SELECT id_destinatar,destinatar,MAX(nr_crt) AS numar_colete, MAX(lista_colete) AS lista_colete
FROM d_jur
GROUP BY id_destinatar,destinatar
ORDER BY 1;

/* 71.Lista coletelor primite de către fiecare persoană juridică(firmă) în anul 2018. Ce colete a primit fiecare persoană juridică(firmă) în anul 2018?*/
WITH RECURSIVE d_jur(id_destinatar, destinatar,id_colet, nr_crt, lista_colete)AS(
SELECT col.id_destinatar, d.nume, col.id_colet, 1, CAST(' \'||col.id_colet||': '||col.continut_colet AS VARCHAR(500))
FROM col 
INNER JOIN destinatari d ON col.id_destinatar=d.id_destinatar
WHERE nr_crt=1
UNION ALL
SELECT d_jur.id_destinatar,d_jur.destinatar,d_jur.id_colet,d_jur.nr_crt+1,CAST(d_jur.lista_colete||' \'||col.id_colet||'-'||col.continut_colet AS VARCHAR(500))
FROM col
INNER JOIN destinatari d ON col.id_destinatar=d.id_destinatar
INNER JOIN d_jur ON col.id_destinatar=d_jur.id_destinatar
AND col.nr_crt=d_jur.nr_crt+ 1),
col AS (
SELECT c.*,d.prenume, ROW_NUMBER()OVER(PARTITION BY c.id_destinatar ORDER BY id_colet) AS nr_crt
FROM colete c
INNER JOIN destinatari d ON c.id_destinatar=d.id_destinatar
WHERE d.prenume IS NULL AND 
EXTRACT (YEAR FROM data_expediere)=2018)
SELECT id_destinatar,destinatar,MAX(nr_crt) AS numar_colete, MAX(lista_colete) AS lista_colete
FROM d_jur
GROUP BY id_destinatar,destinatar
ORDER BY 1;

/* 72.Lista coletelor primite de către fiecare persoană juridică(firmă) în anul 2018 ce au fost expediate din fiecare județ, cu excepția municipiului București. Ce colete a primit fiecare persoană juridică(firmă) în anul 2018 expediate din fiecare județ, cu excepția municipiului București?*/
WITH RECURSIVE d_jur(id_destinatar, destinatar,id_colet, nr_crt, lista_colete)AS(
SELECT col.id_destinatar, d.nume, col.id_colet, 1, CAST(' \'||col.id_colet||': '||col.continut_colet AS VARCHAR(500))
FROM col 
INNER JOIN destinatari d ON col.id_destinatar=d.id_destinatar
WHERE nr_crt=1
UNION ALL
SELECT d_jur.id_destinatar,d_jur.destinatar,d_jur.id_colet,d_jur.nr_crt+1,CAST(d_jur.lista_colete||' \'||col.id_colet||'-'||col.continut_colet AS VARCHAR(500))
FROM col
INNER JOIN destinatari d ON col.id_destinatar=d.id_destinatar
INNER JOIN d_jur ON col.id_destinatar=d_jur.id_destinatar
AND col.nr_crt=d_jur.nr_crt+ 1),
col AS (
SELECT c.*,d.prenume,c_p.judet,ROW_NUMBER()OVER(PARTITION BY c.id_destinatar ORDER BY id_colet) AS nr_crt
FROM colete c
INNER JOIN destinatari d ON c.id_destinatar=d.id_destinatar
INNER JOIN expeditori e ON c.id_expeditor=e.id_expeditor
INNER JOIN coduri_postale c_p ON e.cod_postal=c_p.cod_postal
WHERE d.prenume IS NULL AND 
EXTRACT (YEAR FROM data_expediere)=2018 AND c_p.judet!='București' )
SELECT id_destinatar,destinatar,MAX(nr_crt) AS numar_colete, MAX(lista_colete) AS lista_colete
FROM d_jur
GROUP BY id_destinatar,destinatar
ORDER BY 1;

/* 73.Lista coletelor livrate de către fiecare șofer ce a depășit pragul de 25.000km în luna Iulie. Ce colete au fost livtrate de către fiecare șofer în luna Iulie?*/
WITH RECURSIVE col_sof(id_sofer,sofer, nr_crt, colete) AS(
SELECT col.id_sofer,CAST(a.nume||' '||a.prenume AS VARCHAR(500)),1, CAST ('\1: ' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN soferi s ON col.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
WHERE nr_crt=1
UNION ALL
SELECT col.id_sofer,col_sof.sofer,col_sof.nr_crt + 1,CAST (col_sof.colete || ' \' || col_sof.nr_crt + 1 || ' :' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN soferi s ON col.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
INNER JOIN col_sof ON col.id_sofer=col_sof.id_sofer
AND col.nr_crt = col_sof.nr_crt+ 1),
col AS (
SELECT t.*, d_t.id_colet,ROW_NUMBER()OVER(PARTITION BY t.id_sofer ORDER BY id_colet) AS nr_crt 
FROM transporturi t 
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport 
INNER JOIN soferi s ON t.id_sofer=s.id_sofer
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND
EXTRACT (MONTH FROM data_plecare)=7 AND km_parcursi>25000)
SELECT id_sofer,sofer,MAX(nr_crt) AS nr_colete, MAX(colete) AS lista_colete
FROM col_sof
GROUP BY id_sofer,sofer
ORDER BY 1;

/* 74.Lista coletelor livrate de către fiecare șofer în luna Iulie cu vehicule ce au fost înmatriculate începând cu anul 2012. Ce colete au fost livtrate de către fiecare șofer în luna Iulie cu vehicule ce au fost înmatriculate începând cu anul 2012?*/
WITH RECURSIVE col_sof(id_sofer,sofer, nr_crt, colete) AS(
SELECT col.id_sofer,CAST(a.nume||' '||a.prenume AS VARCHAR(500)),1, CAST ('\1: ' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN soferi s ON col.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
WHERE nr_crt=1
UNION ALL
SELECT col.id_sofer,col_sof.sofer,col_sof.nr_crt + 1,CAST (col_sof.colete || ' \' || col_sof.nr_crt + 1 || ' :' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN soferi s ON col.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
INNER JOIN col_sof ON col.id_sofer=col_sof.id_sofer
AND col.nr_crt = col_sof.nr_crt+ 1),
col AS (
SELECT t.*, d_t.id_colet,v.data_inmatriculare,ROW_NUMBER()OVER(PARTITION BY id_sofer ORDER BY id_colet) AS nr_crt 
FROM transporturi t 
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport 
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul 
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND
EXTRACT (MONTH FROM data_plecare)=7 AND EXTRACT(YEAR FROM data_inmatriculare)>=2012)
SELECT id_sofer,sofer,MAX(nr_crt) AS nr_colete, MAX(colete) AS lista_colete
FROM col_sof
GROUP BY id_sofer,sofer
ORDER BY 1;

/* 75.Lista coletelor livrate de către fiecare șofer din afara județul Iași în luna Iulie cu vehicule ce au fost înmatriculate începând cu anul 2012. Ce colete au fost livtrate de către fiecare șofer din afara județul Iași în luna Iulie cu vehicule ce au fost înmatriculate începând cu anul 2012?*/
WITH RECURSIVE col_sof(id_sofer,sofer, nr_crt, colete) AS(
SELECT col.id_sofer,CAST(a.nume||' '||a.prenume AS VARCHAR(500)),1, CAST ('\1: ' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN soferi s ON col.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
WHERE nr_crt=1
UNION ALL
SELECT col.id_sofer,col_sof.sofer,col_sof.nr_crt + 1,CAST (col_sof.colete || ' \' || col_sof.nr_crt + 1 || ' :' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN soferi s ON col.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
INNER JOIN col_sof ON col.id_sofer=col_sof.id_sofer
AND col.nr_crt = col_sof.nr_crt+ 1),
col AS (
SELECT t.*, d_t.id_colet,v.data_inmatriculare,c_p.judet,ROW_NUMBER()OVER(PARTITION BY t.id_sofer ORDER BY id_colet) AS nr_crt 
FROM transporturi t 
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport 
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul 
INNER JOIN soferi s ON t.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
INNER JOIN coduri_postale c_p ON a.cod_postal=c_p.cod_postal
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND
EXTRACT (MONTH FROM data_plecare)=7 AND EXTRACT(YEAR FROM data_inmatriculare)>=2012 AND judet!='Iași')
SELECT id_sofer,sofer,MAX(nr_crt) AS nr_colete, MAX(colete) AS lista_colete
FROM col_sof
GROUP BY id_sofer,sofer
ORDER BY 1;

/* 76.Lista facturilor primite de către destinatarii cu un id<=1.000.000 în anul 2018. Ce facturi au primit destinatarii cu un id<=1.000.000 în anul 2018? */
WITH RECURSIVE f_d(id_destinatar,destinatar, nr_crt, facturi) AS(
SELECT fact.id_destinatar,CAST(CASE WHEN d.prenume IS NOT NULL THEN d.nume||' '||d.prenume ELSE d.nume END AS VARCHAR(500)),1, CAST ('\' ||fact.nr_factura||'('||fact.data||')' AS VARCHAR(1000))
FROM fact 
INNER JOIN destinatari d ON fact.id_destinatar=d.id_destinatar
WHERE nr_crt=1
UNION ALL
SELECT f_d.id_destinatar,f_d.destinatar,f_d.nr_crt + 1,CAST (f_d.facturi || ' \' ||fact.nr_factura||'('||fact.data||')'  AS VARCHAR(1000))
FROM fact 
INNER JOIN destinatari d ON fact.id_destinatar=d.id_destinatar
INNER JOIN f_d ON fact.id_destinatar=f_d.id_destinatar
AND fact.nr_crt = f_d.nr_crt+ 1),
fact AS (
SELECT f.*,ROW_NUMBER()OVER(PARTITION BY id_destinatar ORDER BY nr_factura) AS nr_crt
FROM facturi f
WHERE EXTRACT (YEAR FROM data)=2018 AND id_destinatar<=100000)
SELECT id_destinatar,destinatar,MAX(nr_crt) AS nr_facturi, MAX(facturi) AS lista_facturi
FROM f_d
GROUP BY id_destinatar,destinatar
ORDER BY 1;

/* 77.Lista facturilor primite de către destinatarii cu un id<=1.000.000 care nu sunt din județul Prahova în anul 2018. Ce facturi au primit destinatarii cu un id<=100.000.0 care nu sunt din județul Prahova în anul 2018? */
WITH RECURSIVE f_d(id_destinatar,destinatar, nr_crt, facturi) AS(
SELECT fact.id_destinatar,CAST(CASE WHEN d.prenume IS NOT NULL THEN d.nume||' '||d.prenume ELSE d.nume END AS VARCHAR(500)),1, CAST ('\' ||fact.nr_factura||'('||fact.data||')' AS VARCHAR(1000))
FROM fact 
INNER JOIN destinatari d ON fact.id_destinatar=d.id_destinatar
WHERE nr_crt=1
UNION ALL
SELECT f_d.id_destinatar,f_d.destinatar,f_d.nr_crt + 1,CAST (f_d.facturi || ' \' ||fact.nr_factura||'('||fact.data||')'  AS VARCHAR(1000))
FROM fact 
INNER JOIN destinatari d ON fact.id_destinatar=d.id_destinatar
INNER JOIN f_d ON fact.id_destinatar=f_d.id_destinatar
AND fact.nr_crt = f_d.nr_crt+ 1),
fact AS (
SELECT f.*,ROW_NUMBER()OVER(PARTITION BY f.id_destinatar ORDER BY nr_factura) AS nr_crt
FROM facturi f
INNER JOIN destinatari d ON f.id_destinatar=d.id_destinatar
INNER JOIN coduri_postale c_p ON d.cod_postal=c_p.cod_postal
WHERE EXTRACT (YEAR FROM data)=2018 AND f.id_destinatar<=100000 AND judet!='Prahova')
SELECT id_destinatar,destinatar,MAX(nr_crt) AS nr_facturi, MAX(facturi) AS lista_facturi
FROM f_d
GROUP BY id_destinatar,destinatar
ORDER BY 1;

/* 78.Lista coletelor transportate cu fiecare vehicul în luna Mai 2018. Ce colete au fost transportate prin intermediul fiecărui vehicul în luna Mai 2018? */
WITH RECURSIVE col_veh(id_vehicul,model, nr_crt, colete) AS(
SELECT col.id_vehicul,CAST(v.model||' ('||v.nr_inmatriculare||')' AS VARCHAR(500)),1, CAST ('\1: ' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN vehicule v ON col.id_vehicul=v.id_vehicul
WHERE nr_crt=1
UNION ALL
SELECT col.id_vehicul,col_veh.model,col_veh.nr_crt + 1,CAST (col_veh.colete || ' \' || col_veh.nr_crt + 1 || ' :' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN vehicule v ON col.id_vehicul=v.id_vehicul
INNER JOIN col_veh ON col.id_vehicul=col_veh.id_vehicul
AND col.nr_crt = col_veh.nr_crt+ 1),
col AS (
SELECT t.*, d_t.id_colet,ROW_NUMBER()OVER(PARTITION BY id_vehicul ORDER BY id_colet) AS nr_crt 
FROM transporturi t 
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport 
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND
EXTRACT (MONTH FROM data_plecare)=5)
SELECT id_vehicul,model,MAX(nr_crt) AS nr_colete, MAX(colete) AS lista_colete
FROM col_veh
GROUP BY id_vehicul,model
ORDER BY id_vehicul;

/* 79.Lista coletelor transportate în anul 2018 cu vehicule conduse de către șoferi ce au depășit pragul de 25000 de km. Ce colete au fost transportate în anul 2018 cu vehicule conduse de către șoferi ce au depășit pragul de 25000 de km */
WITH RECURSIVE col_veh(id_vehicul,model, nr_crt, colete) AS(
SELECT col.id_vehicul,CAST(v.model||' ('||v.nr_inmatriculare||')' AS VARCHAR(500)),1, CAST ('\1: ' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN vehicule v ON col.id_vehicul=v.id_vehicul
WHERE nr_crt=1
UNION ALL
SELECT col.id_vehicul,col_veh.model,col_veh.nr_crt + 1,CAST (col_veh.colete || ' \' || col_veh.nr_crt + 1 || ' :' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN vehicule v ON col.id_vehicul=v.id_vehicul
INNER JOIN col_veh ON col.id_vehicul=col_veh.id_vehicul
AND col.nr_crt = col_veh.nr_crt+ 1),
col AS (
SELECT t.*, d_t.id_colet,km_parcursi,ROW_NUMBER()OVER(PARTITION BY id_vehicul ORDER BY id_colet) AS nr_crt 
FROM transporturi t 
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport 
INNER JOIN soferi s ON t.id_sofer=s.id_sofer
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND km_parcursi>25000)
SELECT id_vehicul,model,MAX(nr_crt) AS nr_colete, MAX(colete) AS lista_colete
FROM col_veh
GROUP BY id_vehicul,model
ORDER BY id_vehicul;

/* 80.Lista coletelor transportate în anul 2018 cu vehicule conduse de către șoferi ce au depășit pragul de 25000 de km și care nu sunt din județul Iași. Ce colete au fost transportate în anul 2018 cu vehicule conduse de către șoferi ce au depășit pragul de 25000 de km și care nu sunt din județul Iași?*/
WITH RECURSIVE col_veh(id_vehicul,model, nr_crt, colete) AS(
SELECT col.id_vehicul,CAST(v.model||' ('||v.nr_inmatriculare||')' AS VARCHAR(500)),1, CAST ('\1: ' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN vehicule v ON col.id_vehicul=v.id_vehicul
WHERE nr_crt=1
UNION ALL
SELECT col.id_vehicul,col_veh.model,col_veh.nr_crt + 1,CAST (col_veh.colete || ' \' || col_veh.nr_crt + 1 || ' :' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN vehicule v ON col.id_vehicul=v.id_vehicul
INNER JOIN col_veh ON col.id_vehicul=col_veh.id_vehicul
AND col.nr_crt = col_veh.nr_crt+ 1),
col AS (
SELECT t.*, d_t.id_colet,km_parcursi,judet,ROW_NUMBER()OVER(PARTITION BY id_vehicul ORDER BY id_colet) AS nr_crt 
FROM transporturi t 
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport 
INNER JOIN soferi s ON t.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
INNER JOIN coduri_postale c_p ON a.cod_postal=c_p.cod_postal
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND km_parcursi>25000 AND judet!='Iași')
SELECT id_vehicul,model,MAX(nr_crt) AS nr_colete, MAX(colete) AS lista_colete
FROM col_veh
GROUP BY id_vehicul,model
ORDER BY id_vehicul;

/* 81.Lista coletelor transportate în anul 2018 din primele 3 depozite cu vehicule conduse de către șoferi ce au depășit pragul de 25000 de km și care nu sunt din județul Iași. Ce colete au fost transportate în anul 2018 din primele 3 depozite ecu vehicule conduse de către șoferi ce au depășit pragul de 25000 de km și care nu sunt din județul Iași?*/
WITH RECURSIVE col_veh(id_vehicul,model, nr_crt, colete) AS(
SELECT col.id_vehicul,CAST(v.model||' ('||v.nr_inmatriculare||')' AS VARCHAR(500)),1, CAST ('\1: ' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN vehicule v ON col.id_vehicul=v.id_vehicul
WHERE nr_crt=1
UNION ALL
SELECT col.id_vehicul,col_veh.model,col_veh.nr_crt + 1,CAST (col_veh.colete || ' \' || col_veh.nr_crt + 1 || ' :' || c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN vehicule v ON col.id_vehicul=v.id_vehicul
INNER JOIN col_veh ON col.id_vehicul=col_veh.id_vehicul
AND col.nr_crt = col_veh.nr_crt+ 1),
col AS (
SELECT t.*, d_t.id_colet,km_parcursi,judet,ROW_NUMBER()OVER(PARTITION BY id_vehicul ORDER BY id_colet) AS nr_crt 
FROM transporturi t 
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport 
INNER JOIN soferi s ON t.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
INNER JOIN coduri_postale c_p ON a.cod_postal=c_p.cod_postal
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND km_parcursi>25000 AND judet!='Iași' AND id_depozit_1 BETWEEN 1 AND 3)
SELECT id_vehicul,model,MAX(nr_crt) AS nr_colete, MAX(colete) AS lista_colete
FROM col_veh
GROUP BY id_vehicul,model
ORDER BY id_vehicul;

/* 82.Lista coletelor primite de către destinatarii din fiecare județ în luna luna Aprilie 2018. Ce colete au fost livrate către fiecare județ în luna luna Aprilie 2018? */
WITH RECURSIVE col_djud(judet, nr_crt, colete) AS(
SELECT col.judet,1, CAST ('\1: ' ||c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
WHERE nr_crt=1
UNION ALL
SELECT col.judet,col_djud.nr_crt + 1,CAST (col_djud.colete || ' \' || col_djud.nr_crt + 1 || ': ' ||c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN col_djud ON col.judet=col_djud.judet
AND col.nr_crt = col_djud.nr_crt+ 1),
col AS (
SELECT cp.*, c.id_colet,ROW_NUMBER()OVER(PARTITION BY cp.judet ORDER BY id_colet) AS nr_crt 
FROM coduri_postale cp
INNER JOIN destinatari d ON cp.cod_postal=d.cod_postal
INNER JOIN colete c ON d.id_destinatar=c.id_destinatar
WHERE EXTRACT (YEAR FROM data_primire_colet)=2018 AND
EXTRACT (MONTH FROM data_primire_colet)=4)
SELECT judet,MAX(nr_crt) AS nr_colete, MAX(colete) AS lista_colete
FROM col_djud
GROUP BY judet
ORDER BY judet;

/* 83.Lista coletelor cu o valoare mai mare de 1000 de lei și o greutate minimă de 5 kg primite de către destinatarii din fiecare județ în luna luna Aprilie 2018. Ce colete au fost livrate către fiecare județ în luna luna Aprilie 2018? */
WITH RECURSIVE col_djud(judet, nr_crt, colete) AS(
SELECT col.judet,1, CAST ('\1: ' ||c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
WHERE nr_crt=1
UNION ALL
SELECT col.judet,col_djud.nr_crt + 1,CAST (col_djud.colete || ' \' || col_djud.nr_crt + 1 || ': ' ||c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN col_djud ON col.judet=col_djud.judet
AND col.nr_crt = col_djud.nr_crt+ 1),
col AS (
SELECT cp.*, c.id_colet,ROW_NUMBER()OVER(PARTITION BY cp.judet ORDER BY id_colet) AS nr_crt 
FROM coduri_postale cp
INNER JOIN destinatari d ON cp.cod_postal=d.cod_postal
INNER JOIN colete c ON d.id_destinatar=c.id_destinatar
WHERE EXTRACT (YEAR FROM data_primire_colet)=2018 AND
EXTRACT (MONTH FROM data_primire_colet)=4 AND valoare>1000 AND greutate>=5)
SELECT judet,MAX(nr_crt) AS nr_colete, MAX(colete) AS lista_colete
FROM col_djud
GROUP BY judet
ORDER BY judet;

/* 84.Lista coletelor trimise de expeditorii din fiecare județ în luna August. Ce colete au fost trimise(expediate) din fiecare județ în luna August ?*/
WITH RECURSIVE col_ejud(judet, nr_crt, colete) AS(
SELECT col.judet,1, CAST ('\1: ' ||c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
WHERE nr_crt=1
UNION ALL
SELECT col.judet,col_ejud.nr_crt + 1,CAST (col_ejud.colete || ' \' || col_ejud.nr_crt + 1 || ': ' ||c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN col_ejud ON col.judet=col_ejud.judet
AND col.nr_crt = col_ejud.nr_crt+ 1),
col AS (
SELECT cp.*, c.id_colet,ROW_NUMBER()OVER(PARTITION BY cp.judet ORDER BY id_colet) AS nr_crt 
FROM coduri_postale cp
INNER JOIN expeditori e ON cp.cod_postal=e.cod_postal
INNER JOIN colete c ON e.id_expeditor=c.id_expeditor
WHERE EXTRACT (YEAR FROM data_expediere)=2018 AND
EXTRACT (MONTH FROM data_expediere)=8)
SELECT judet,MAX(nr_crt) AS nr_colete, MAX(colete) AS lista_colete
FROM col_ejud
GROUP BY judet
ORDER BY judet;

/* 85.Lista coletelor cu o greutate mai mică de 5 kg și o valoare mai mică de 500 de lei trimise de expeditorii din fiecare județ în luna August. Ce colete au fost trimise(expediate) din fiecare județ în luna August ?*/
WITH RECURSIVE col_ejud(judet, nr_crt, colete) AS(
SELECT col.judet,1, CAST ('\1: ' ||c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
WHERE nr_crt=1
UNION ALL
SELECT col.judet,col_ejud.nr_crt + 1,CAST (col_ejud.colete || ' \' || col_ejud.nr_crt + 1 || ': ' ||c.continut_colet AS VARCHAR(1000))
FROM col 
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN col_ejud ON col.judet=col_ejud.judet
AND col.nr_crt = col_ejud.nr_crt+ 1),
col AS (
SELECT cp.*, c.id_colet,ROW_NUMBER()OVER(PARTITION BY cp.judet ORDER BY id_colet) AS nr_crt 
FROM coduri_postale cp
INNER JOIN expeditori e ON cp.cod_postal=e.cod_postal
INNER JOIN colete c ON e.id_expeditor=c.id_expeditor
WHERE EXTRACT (YEAR FROM data_expediere)=2018 AND
EXTRACT (MONTH FROM data_expediere)=8 AND valoare<500 AND greutate<5)
SELECT judet,MAX(nr_crt) AS nr_colete, MAX(colete) AS lista_colete
FROM col_ejud
GROUP BY judet
ORDER BY judet;

/* 86.Lista coletelor pentru transporturile efectuate în anul 2018 cu un vehicul, model Ford Transit, ce au fost conduse de către șoferi ce au depășit pragul de 5000 de km, afișându-se și tracking number-ul fiecărui colet. Ce colete au fost transportate în anul 2018, cu un vehicul, model Ford Transit, ce au fost conduse de către șoferi ce au depășit pragul de 5000 de km, afișându-se și tracking number-ul fiecărui colet ? */
WITH RECURSIVE i_t(id_transport,depozit,sofer,id_colet, nr_crt,lista_colete) AS(
SELECT col.id_transport,CAST('Depozitul '||t.id_depozit_1||' ('||d.adresa||')' AS VARCHAR(500)), CAST(a.nume||' '||a.prenume AS VARCHAR(500)),col.id_colet,1, CAST('\1-'||continut_colet||'('||u_c.trackingnumber||')' AS VARCHAR(500))
FROM col
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN soferi s ON col.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
INNER JOIN transporturi t ON col.id_transport=t.id_transport
INNER JOIN depozite d ON t.id_depozit_1=d.id_depozit
INNER JOIN urmarire_colete u_c ON col.id_colet=u_c.id_colet
WHERE nr_crt=1
UNION ALL
SELECT i_t.id_transport,i_t.depozit,i_t.sofer,i_t.id_colet,i_t.nr_crt + 1,CAST(i_t.lista_colete||' \'||i_t.nr_crt+1||'-'||c.continut_colet||' ('||u_c.trackingnumber||')' AS VARCHAR(500))
FROM col
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN soferi s ON col.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
INNER JOIN transporturi t ON col.id_transport=t.id_transport
INNER JOIN depozite d ON t.id_depozit_1=d.id_depozit
INNER JOIN urmarire_colete u_c ON col.id_colet=u_c.id_colet
INNER JOIN i_t ON col.id_transport=i_t.id_transport
AND col.nr_crt = i_t.nr_crt+ 1),
col AS (
SELECT d_t.*,t.id_vehicul,t.id_sofer,t.data_plecare,ROW_NUMBER()OVER(PARTITION BY d_t.id_transport ORDER BY d_t.id_colet) AS nr_crt
FROM detalii_transport d_t 
INNER JOIN transporturi t ON d_t.id_transport=t.id_transport
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul
INNER JOIN soferi s ON t.id_sofer=s.id_sofer 
WHERE model='Ford Transit' AND km_parcursi>=5000 AND EXTRACT(YEAR FROM data_plecare)=2018)
SELECT id_transport,depozit,sofer,MAX(nr_crt) AS numar_colete, MAX(lista_colete) AS lista_colete
FROM i_t
GROUP BY id_transport,depozit,sofer
ORDER BY 1;

/* 87.Lista coletelor organizată pe facturi întocmite în anul 2018 de către primii 90.000 de expeditori, afișandu-se și tracking number-ul. Ce colete conține fiecare factură întocmită în anul 2018 de către primii 90.000 de expeditori?*/
WITH RECURSIVE l_f(nr_factura,expeditor, nr_crt, id_colet, lista_colete) AS(
SELECT col.nr_factura,CAST(CASE WHEN e.prenume IS NOT NULL THEN e.nume||' '||e.prenume ELSE e.nume END AS VARCHAR(500)),1,col.id_colet, CAST('\1-'||continut_colet||'('||u_c.trackingnumber||')' AS VARCHAR(500))
FROM col
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN expeditori e ON col.id_expeditor=e.id_expeditor
INNER JOIN urmarire_colete u_c ON col.id_colet=u_c.id_colet
WHERE nr_crt=1
UNION ALL
SELECT l_f.nr_factura,l_f.expeditor,l_f.nr_crt + 1,l_f.id_colet,CAST(l_f.lista_colete||' \'||l_f.nr_crt+1||'-'||c.continut_colet AS VARCHAR(500))
FROM col
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN expeditori e ON col.id_expeditor=e.id_expeditor
INNER JOIN urmarire_colete u_c ON col.id_colet=u_c.id_colet
INNER JOIN l_f ON col.nr_factura=l_f.nr_factura
AND col.nr_crt=l_f.nr_crt+ 1),
col AS (
SELECT d_f.*,f.data,f.id_expeditor,ROW_NUMBER()OVER(PARTITION BY d_f.nr_factura ORDER BY d_f.id_colet) AS nr_crt
FROM detalii_facturi d_f
INNER JOIN facturi f ON d_f.nr_factura=f.nr_factura
WHERE EXTRACT (YEAR FROM data)=2018 AND id_expeditor BETWEEN 1 AND 90000)
SELECT nr_factura,expeditor,MAX(nr_crt) AS numar_colete, MAX(lista_colete) AS lista_colete
FROM l_f
GROUP BY nr_factura,expeditor
ORDER BY 1;

/* 88.Lista coletelor pentru primele 2000 de transporturi, afișandu-se și tracking-ul numberul fiecaruia. Ce colete conțin -primele 2000 de transporturi? */
WITH RECURSIVE i_t(id_transport,id_colet, nr_crt,lista_colete) AS(
SELECT col.id_transport,col.id_colet,1, CAST('\1-'||continut_colet||'('||u_c.trackingnumber||')' AS VARCHAR(500))
FROM col
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN urmarire_colete u_c ON col.id_colet=u_c.id_colet
WHERE nr_crt=1
UNION ALL
SELECT i_t.id_transport,i_t.id_colet,i_t.nr_crt + 1,CAST(i_t.lista_colete||' \'||i_t.nr_crt+1||'-'||c.continut_colet||'('||u_c.trackingnumber||')' AS VARCHAR(500))
FROM col
INNER JOIN colete c ON col.id_colet=c.id_colet
INNER JOIN urmarire_colete u_c ON col.id_colet=u_c.id_colet
INNER JOIN i_t ON col.id_transport=i_t.id_transport
AND col.nr_crt = i_t.nr_crt+ 1),
col AS (
SELECT d_t.*,ROW_NUMBER()OVER(PARTITION BY d_t.id_transport ORDER BY d_t.id_colet) AS nr_crt
FROM detalii_transport d_t 
WHERE d_t.id_transport<=2000)
SELECT id_transport,MAX(nr_crt) AS numar_colete, MAX(lista_colete) AS lista_colete
FROM i_t
GROUP BY id_transport
ORDER BY 1;
