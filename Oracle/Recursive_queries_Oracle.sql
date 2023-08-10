/* 1.Lista coletelor organizată pe transporturi efectuate în perioada 1.01-15.01.2018. Ce colete conține fiecare transport efectuat în data de 1.01-15.01.2018 */
WITH i_t AS (
SELECT d_t.*,t.data_plecare,ROW_NUMBER()OVER(PARTITION BY d_t.id_transport ORDER BY d_t.id_colet) AS nr_crt
FROM detalii_transport d_t 
INNER JOIN transporturi t ON d_t.id_transport=t.id_transport
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND 
EXTRACT (MONTH FROM data_plecare)=1 AND
EXTRACT (DAY FROM data_plecare) BETWEEN 1 AND 15)
SELECT i_t.id_transport,MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||'-'||c.continut_colet,' \')) AS lista_colete
FROM i_t
INNER JOIN colete c ON i_t.id_colet=c.id_colet
START WITH nr_crt=1
CONNECT BY PRIOR i_t.id_transport=i_t.id_transport AND PRIOR nr_crt=nr_crt-1
GROUP BY i_t.id_transport
ORDER BY 1

/* 2.Lista coletelor organizată pe facturi întocmite în perioada 01.01-30.01.2018. Ce colete conține fiecare factură întocmită în perioada 01.01-30.01.2018? */
WITH l_f AS(
SELECT d_f.*,f.data,ROW_NUMBER()OVER(PARTITION BY d_f.nr_factura ORDER BY d_f.id_colet) AS nr_crt
FROM detalii_facturi d_f
INNER JOIN facturi f ON d_f.nr_factura=f.nr_factura
WHERE EXTRACT (YEAR FROM data)=2018 AND 
EXTRACT (MONTH FROM data)=1 AND
EXTRACT (DAY FROM data) BETWEEN 1 AND 30)
SELECT nr_factura,MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||'-'||c.continut_colet,' \')) AS lista_colete
FROM l_f
INNER JOIN colete c ON l_f.id_colet=c.id_colet
START WITH nr_crt=1
CONNECT BY PRIOR l_f.nr_factura=l_f.nr_factura AND PRIOR nr_crt=nr_crt-1
GROUP BY nr_factura
ORDER BY nr_factura

/* 3.Lista coletelor expediate de către fiecare expeditor în parte în perioada 01.01-08.01.2018. Ce colete a expediat fiecare expeditor în parte în perioada 01.01-08.01.2018? */
WITH c_e AS(
SELECT c.*, ROW_NUMBER()OVER(PARTITION BY id_expeditor ORDER BY id_colet) AS nr_crt
FROM colete c
WHERE EXTRACT (YEAR FROM data_expediere)=2018 AND 
EXTRACT (MONTH FROM data_expediere)=1 AND
EXTRACT (DAY FROM data_expediere) BETWEEN 1 AND 8)
SELECT c.id_expeditor,CAST(e.nume||' '||e.prenume AS VARCHAR(500)) AS expeditor,MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(c.id_colet||': '||c.continut_colet,' \')) AS lista_colete
FROM c_e
INNER JOIN colete c ON c_e.id_colet=c.id_colet
INNER JOIN expeditori e ON c_e.id_expeditor=e.id_expeditor
START WITH nr_crt=1
CONNECT BY PRIOR c_e.id_expeditor=c_e.id_expeditor AND PRIOR nr_crt=nr_crt-1
GROUP BY c.id_expeditor,e.nume,e.prenume
ORDER BY id_expeditor

/* 4.Lista coletelor primite de către fiecare destinatar în perioada 15.02-30.02.2018. Ce colete a primit fiecare destinatar în perioada 15.02-30.02.2018 */
WITH c_d AS(
SELECT c.*, ROW_NUMBER()OVER(PARTITION BY id_destinatar ORDER BY id_colet) AS nr_crt
FROM colete c
WHERE EXTRACT (YEAR FROM data_primire_colet)=2018 AND 
EXTRACT (MONTH FROM data_primire_colet)=2 AND
EXTRACT (DAY FROM data_primire_colet) BETWEEN 15 AND 30)
SELECT c.id_destinatar,CAST(d.nume||' '||d.prenume AS VARCHAR(500)) AS destinatar,MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(c.id_colet||': '||c.continut_colet,' \')) AS lista_colete
FROM c_d
INNER JOIN colete c ON c_d.id_colet=c.id_colet
INNER JOIN destinatari d ON c_d.id_destinatar=d.id_destinatar
START WITH nr_crt=1
CONNECT BY PRIOR c_d.id_destinatar=c_d.id_destinatar AND PRIOR nr_crt=nr_crt-1
GROUP BY c.id_destinatar,d.nume,d.prenume
ORDER BY id_destinatar

/* 5.Lista coletelor ce au plecat din fiecare depozit în perioada 01.10-06.10.2018. Ce colete au plecat din fiecare depozit în perioada 01.10-06.10.2018 */
WITH col_dep AS(
SELECT t.id_depozit_1, d_t.id_colet,ROW_NUMBER()OVER(PARTITION BY id_depozit_1 ORDER BY id_colet) AS nr_crt
FROM transporturi t
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND
EXTRACT (MONTH FROM data_plecare)=10 AND
EXTRACT (DAY FROM data_plecare) BETWEEN 1 AND 6)
SELECT CAST('Depozitul '||col_dep.id_depozit_1||' ('||d.adresa||')' AS VARCHAR(500)) AS Depozit, MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||': '||c.continut_colet,' \')) AS lista_colete
FROM col_dep
INNER JOIN colete c ON col_dep.id_colet=c.id_colet
INNER JOIN depozite d ON col_dep.id_depozit_1=d.id_depozit
START WITH nr_crt=1
CONNECT BY PRIOR col_dep.id_depozit_1=col_dep.id_depozit_1 AND PRIOR nr_crt=nr_crt-1
GROUP BY col_dep.id_depozit_1,d.adresa
ORDER BY id_depozit_1

/* 6.Lista coletelor transportate din depozit în depozit în perioada 15.06-18.06.2018. Ce colete au fost transportate din depozit în depozit în perioada 15.06-20.06.2018 */
WITH col_dep2 AS(
SELECT t.*,CAST('Depozitul '||t.id_depozit_1||' -> '||'Depozitul '||t.id_depozit_2 AS VARCHAR(500)) AS depozite,d_t.id_colet, ROW_NUMBER()OVER(PARTITION BY id_depozit_1,id_depozit_2 ORDER BY id_colet) AS nr_crt
FROM transporturi t 
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport 
WHERE id_depozit_2 IS NOT NULL AND 
EXTRACT (YEAR FROM data_plecare)=2018 AND
EXTRACT (MONTH FROM data_plecare)=06 AND
EXTRACT (DAY FROM data_plecare) BETWEEN 15 AND 20)
SELECT col_dep2.depozite, MAX(nr_crt) AS nr_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||': '||c.continut_colet,' \')) AS lista_colete
FROM col_dep2
INNER JOIN colete c ON col_dep2.id_colet=c.id_colet
START WITH nr_crt=1
CONNECT BY PRIOR col_dep2.depozite=col_dep2.depozite AND PRIOR nr_crt=nr_crt-1
GROUP BY col_dep2.depozite,col_dep2.id_depozit_1
ORDER BY col_dep2.id_depozit_1

/* 7.Lista transporturilor efectuate de catre feicare șofer în parte în lunile Ianuarie și Februarie 2018. Ce transporturi a efectuat fiecare șofer în lunile Ianuarie și Februarie 2018? */
WITH tr_s AS(
SELECT t.*,ROW_NUMBER()OVER(PARTITION BY id_sofer ORDER BY id_transport) AS nr_crt
FROM transporturi t
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND 
EXTRACT (MONTH FROM data_plecare) BETWEEN 1 AND 2)
SELECT tr_s.id_sofer, CAST(nume||' '||prenume AS VARCHAR(4000))AS sofer, MAX(nr_crt) AS nr_transporturi,MAX(SYS_CONNECT_BY_PATH(nr_crt||': '||id_transport||'('||data_plecare||')'||' ','\')) AS Transporturi 
FROM tr_s
INNER JOIN soferi s ON tr_s.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
START WITH nr_crt=1
CONNECT BY PRIOR tr_s.id_sofer=tr_s.id_sofer AND PRIOR nr_crt=nr_crt-1
GROUP BY tr_s.id_sofer, nume, prenume
ORDER BY 1

/* 8.Lista transporturilor efectuate cu fiecare mijloc de transport în perioada xx.01-xx.04.2018. Ce transporturi au fost efectuate cu fiecare mijloc de transport în perioada xx.01-xx.04.2018? */
WITH tr_v AS(
SELECT t.*,ROW_NUMBER()OVER(PARTITION BY id_vehicul ORDER BY id_transport) AS linie
FROM transporturi t
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND 
EXTRACT (MONTH FROM data_plecare) BETWEEN 1 AND 4)
SELECT tr_v.id_vehicul, CAST(v.model||' ('||v.nr_inmatriculare||')' AS VARCHAR(500)) AS model,MAX(linie) AS nr_transporturi,MAX(SYS_CONNECT_BY_PATH(linie||': '||id_transport||'('||data_plecare||') ','\')) AS Lista_transporturi
FROM tr_v
INNER JOIN vehicule v ON tr_v.id_vehicul=v.id_vehicul
START WITH linie=1
CONNECT BY PRIOR tr_v.id_vehicul=tr_v.id_vehicul AND PRIOR linie=linie-1
GROUP BY tr_v.id_vehicul, model, nr_inmatriculare
ORDER BY 1

/* 9.Lista transporturilor ce au plecat din fiecare depozit în săptămâna 13-19.03.2018. Ce transporturi au plecat din fiecare depozit în săptamana 13-19.03.2018? */
WITH tr_dep AS(
SELECT t.*,ROW_NUMBER()OVER(PARTITION BY id_depozit_1 ORDER BY id_transport) AS linie
FROM transporturi t
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND 
EXTRACT (MONTH FROM data_plecare)=3 AND
EXTRACT (DAY FROM data_plecare) BETWEEN 13 AND 19)
SELECT CAST('Depozitul ' ||tr_dep.id_depozit_1||'('||adresa||', '||c_p.judet||')' AS VARCHAR(500)) AS depozit, MAX(linie) AS nr_transporturi, MAX(SYS_CONNECT_BY_PATH(linie||': '||id_transport||' ('||data_plecare||')',' \')) AS Lista_transporturi
FROM tr_dep
INNER JOIN depozite d ON tr_dep.id_depozit_1=d.id_depozit
INNER JOIN coduri_postale c_p ON d.cod_postal=c_p.cod_postal
START WITH linie=1 
CONNECT BY PRIOR tr_dep.id_depozit_1=tr_dep.id_depozit_1 AND PRIOR linie=linie-1
GROUP BY id_depozit_1, adresa,judet
ORDER BY id_depozit_1

/* 10.Lista coletelor returnate în lunile Ianuarie-Mai 2018. Ce colete au fost returnate în lunile Ianuarie-Mari 2018? */
WITH col_ret AS(
SELECT r.*,d_f.id_colet,ROW_NUMBER()OVER(PARTITION BY id_retur ORDER BY d_f.id_colet) AS nr_crt
FROM retururi r
INNER JOIN detalii_facturi d_f ON r.nr_factura=d_f.nr_factura
WHERE EXTRACT (YEAR FROM data_retur)=2018 AND 
EXTRACT (MONTH FROM data_retur) BETWEEN 1 AND 5)
SELECT CAST(col_ret.id_retur||' ('||col_ret.motiv_retur||')' AS VARCHAR(500)) AS retur, MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||'-'||c.continut_colet,' \')) AS lista_colete
FROM col_ret
INNER JOIN colete c ON col_ret.id_colet=c.id_colet
START WITH nr_crt=1
CONNECT BY PRIOR col_ret.id_retur=col_ret.id_retur AND PRIOR nr_crt=nr_crt-1
GROUP BY col_ret.id_retur,col_ret.motiv_retur
ORDER BY id_retur

/* 11.Lista transporturilor din depozit în depozit efectuate în luna Februarie 2018. Ce transporturi din depozit în depozit au fost efectuate în luna Februarie 2018? */
WITH tr_dep2 AS(
SELECT t.*,CAST('Depozitul '||t.id_depozit_1||' -> '||'Depozitul '||t.id_depozit_2 AS VARCHAR(500)) AS depozite,ROW_NUMBER()OVER(PARTITION BY id_depozit_1,id_depozit_2 ORDER BY id_transport) AS linie
FROM transporturi t
WHERE id_depozit_2 IS NOT NULL AND 
EXTRACT (YEAR FROM data_plecare)=2018 AND 
EXTRACT (MONTH FROM data_plecare)=2)
SELECT tr_dep2.depozite, MAX(linie) AS nr_transporturi, MAX(SYS_CONNECT_BY_PATH(linie||': '||id_transport||' ('||data_plecare||')',' \')) AS Lista_transporturi
FROM tr_dep2
START WITH linie=1 
CONNECT BY PRIOR tr_dep2.depozite=tr_dep2.depozite AND PRIOR linie=linie-1
GROUP BY tr_dep2.depozite,id_depozit_1
ORDER BY id_depozit_1

/* 12.Lista tuturor coletelor expediată de fiecare persoană jurdică(firmă) din județul Suceava în perioada xx.01-xx.04. Ce colete a expediat fiecare persoană juridică(firmă) din județul Suceava în perioada xx.01-xx.04.2018? */
WITH e_jur AS(
SELECT c.*, ROW_NUMBER()OVER(PARTITION BY id_expeditor ORDER BY id_colet) AS nr_crt
FROM colete c
WHERE EXTRACT (YEAR FROM data_expediere)=2018 AND 
EXTRACT (MONTH FROM data_expediere) BETWEEN 1 AND 4)
SELECT e_jur.id_expeditor, CAST(nume AS VARCHAR(500)) AS expeditor, MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(id_colet||': '||continut_colet,' \')) AS lista_colete
FROM e_jur
INNER JOIN expeditori e ON e_jur.id_expeditor=e.id_expeditor
INNER JOIN coduri_postale c_p ON e.cod_postal=c_p.cod_postal
WHERE e.prenume IS NULL AND c_p.judet='Suceava' AND
EXTRACT (YEAR FROM data_expediere)=2018 AND 
EXTRACT (MONTH FROM data_expediere) BETWEEN 1 AND 4
START WITH nr_crt=1
CONNECT BY PRIOR e_jur.id_expeditor=e_jur.id_expeditor AND PRIOR nr_crt=nr_crt-1
GROUP BY e_jur.id_expeditor, nume
ORDER BY 1

/* 13.Lista tututor incidentelor raportate de fiecare șofer în parte în perioada xx.01-xx.03.2018. Ce incidente a raportat fiecare șofer în parte în perioada xx.01-xx.03.2018? */
WITH i_s AS(
SELECT i.*, ROW_NUMBER()OVER(PARTITION BY id_sofer ORDER BY id_incident) AS nr_crt
FROM incidente i
WHERE EXTRACT (YEAR FROM data_raportarii)=2018 AND 
EXTRACT (MONTH FROM data_raportarii) BETWEEN 1 AND 3)
SELECT i_s.id_sofer, CAST(a.nume||' '||a.prenume AS VARCHAR(500)) AS sofer, MAX(nr_crt) AS numar_incidente, MAX(SYS_CONNECT_BY_PATH(nr_crt||': '||tip_incident||'('||data_raportarii||')',' \')) AS lista_incidente
FROM i_s
INNER JOIN soferi s ON i_s.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
WHERE EXTRACT (YEAR FROM data_raportarii)=2018 AND 
EXTRACT (MONTH FROM data_raportarii) BETWEEN 1 AND 3
START WITH nr_crt=1
CONNECT BY PRIOR i_s.id_sofer=i_s.id_sofer AND PRIOR nr_crt=nr_crt-1
GROUP BY i_s.id_sofer, nume, prenume
ORDER BY 1

/* 14.Lista coletelor primite de către fiecare persoană juridică(firmă) din județul Iași în perioada xx.02-xx.06.2018. Ce colete a primit fiecare persoană juridică(firmă) din județul Iași în perioadă xx.02-xx.06.2018? */
WITH d_jur AS(
SELECT c.*, ROW_NUMBER()OVER(PARTITION BY id_destinatar ORDER BY id_colet) AS nr_crt
FROM colete c
WHERE EXTRACT (YEAR FROM data_expediere)=2018 AND 
EXTRACT (MONTH FROM data_expediere) BETWEEN 2 AND 6)
SELECT d_jur.id_destinatar, CAST(nume AS VARCHAR(500)) AS destinatar, MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(id_colet||': '||continut_colet,' \')) AS lista_colete
FROM d_jur
INNER JOIN destinatari d ON d_jur.id_destinatar=d.id_destinatar
INNER JOIN coduri_postale c_p ON d.cod_postal=c_p.cod_postal
WHERE d.prenume IS NULL AND c_p.judet='Iași' AND
EXTRACT (YEAR FROM data_expediere)=2018 AND 
EXTRACT (MONTH FROM data_expediere) BETWEEN 2 AND 6
START WITH nr_crt=1
CONNECT BY PRIOR d_jur.id_destinatar=d_jur.id_destinatar AND PRIOR nr_crt=nr_crt-1
GROUP BY d_jur.id_destinatar, nume
ORDER BY 1

/* 15.Lista coletelor livrate de către fiecare șofer în perioada 21.07-23.07.2018. Ce colete au fost livtrate de către fiecare șofer în perioada 21.07-23.07.2018? */
WITH col_sof AS(
SELECT t.id_sofer, d_t.id_colet,ROW_NUMBER()OVER(PARTITION BY id_sofer ORDER BY id_colet) AS nr_crt
FROM transporturi t
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND
EXTRACT (MONTH FROM data_plecare)=7 AND 
EXTRACT (DAY FROM data_plecare) BETWEEN 21 AND 23)
SELECT col_sof.id_sofer,CAST(a.nume||' '||a.prenume AS VARCHAR(500)) AS sofer, MAX(nr_crt) AS nr_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||': '||c.continut_colet,' \')) AS lista_colete
FROM col_sof
INNER JOIN colete c ON col_sof.id_colet=c.id_colet
INNER JOIN soferi s ON col_sof.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
START WITH nr_crt=1
CONNECT BY PRIOR col_sof.id_sofer=col_sof.id_sofer AND PRIOR nr_crt=nr_crt-1
GROUP BY col_sof.id_sofer,a.nume,a.prenume
ORDER BY 1

/* 16.Lista facturilor primite de către fiecare destinatar în parte în perioada xx.03-xx.06.2018. Ce facturi a primit fiecare destinatar în perioada xx.03-xx.06.2018? */
WITH f_d AS(
SELECT f.*, ROW_NUMBER()OVER(PARTITION BY id_destinatar ORDER BY nr_factura) AS nr_crt
FROM facturi f
WHERE EXTRACT (YEAR FROM data)=2018 AND 
EXTRACT (MONTH FROM data) BETWEEN 3 AND 6)
SELECT f_d.id_destinatar, CAST(nume||' '||prenume AS VARCHAR(500))AS destinatar, MAX(nr_crt) AS nr_facturi, MAX(SYS_CONNECT_BY_PATH(nr_factura||'('||data||')',' \')) AS lista_facturi
FROM f_d
INNER JOIN destinatari d ON f_d.id_destinatar=d.id_destinatar
WHERE EXTRACT (YEAR FROM data)=2018 AND 
EXTRACT (MONTH FROM data) BETWEEN 3 AND 6
START WITH nr_crt=1
CONNECT BY PRIOR f_d.id_destinatar=f_d.id_destinatar AND PRIOR nr_crt=nr_crt-1
GROUP BY f_d.id_destinatar,nume,prenume
ORDER BY 1

/* 17.Lista facturilor întocmite de către fiecare expeditor în perioada xx.04-xx.08.2018. Ce facturi a întocmit fiecare expeditor în perioada xx.04-xx.08.2018? */
WITH f_e AS(
SELECT f.*, ROW_NUMBER()OVER(PARTITION BY id_expeditor ORDER BY nr_factura) AS nr_crt
FROM facturi f
WHERE EXTRACT (YEAR FROM data)=2018 AND 
EXTRACT (MONTH FROM data) BETWEEN 4 AND 8)
SELECT f_e.id_expeditor, CAST(nume||' '||prenume AS VARCHAR(500))AS expeditor, MAX(nr_crt) AS nr_facturi, MAX(SYS_CONNECT_BY_PATH(nr_factura||'('||data||')',' \')) AS lista_facturi
FROM f_e
INNER JOIN expeditori e ON f_e.id_expeditor=e.id_expeditor
START WITH nr_crt=1
CONNECT BY PRIOR f_e.id_expeditor=f_e.id_expeditor AND PRIOR nr_crt=nr_crt-1
GROUP BY f_e.id_expeditor,nume,prenume
ORDER BY 1

/* 18.Lista expeditorilor organizată pe coduri poștale pentru primii 10.000 de expeditori. Pentru primii 10.000 de expeditori să se alcătuiască lista acestora organizată pe coduri poștale. */
WITH cp_exp AS(
SELECT e.*, ROW_NUMBER()OVER(PARTITION BY cod_postal ORDER BY id_expeditor) AS nr_crt
FROM expeditori e
WHERE id_expeditor<=10000)
SELECT cp_exp.cod_postal, MAX(nr_crt) AS nr_expeditori, MAX(SYS_CONNECT_BY_PATH(nr_crt||': '||nume||' '||prenume,' \')) AS lista_expeditori
FROM cp_exp
START WITH nr_crt=1
CONNECT BY PRIOR cp_exp.cod_postal=cp_exp.cod_postal AND PRIOR nr_crt=nr_crt-1
GROUP BY cp_exp.cod_postal
ORDER BY 1

/* 19.Lista destinatarilor organizată pe coduri poștale pentru destinatarii al caror id este cuprins între 60000 și 70000. Să se afișeze lista destinatarilor oganizată pe coduri poștale pentru destinatarii cuprinși între 60.000 și 70.000 */
WITH cp_dest AS(
SELECT d.*, ROW_NUMBER()OVER(PARTITION BY cod_postal ORDER BY id_destinatar) AS nr_crt
FROM destinatari d
WHERE id_destinatar>=60000 AND id_destinatar<=70000)
SELECT cp_dest.cod_postal, MAX(nr_crt) AS nr_destinatari, MAX(SYS_CONNECT_BY_PATH(nr_crt||': '||nume||' '||prenume,' \')) AS lista_expeditori
FROM cp_dest
START WITH nr_crt=1
CONNECT BY PRIOR cp_dest.cod_postal=cp_dest.cod_postal AND PRIOR nr_crt=nr_crt-1
GROUP BY cp_dest.cod_postal
ORDER BY 1

/* 20.Lista angajaților organizată pe coduri poștale */
WITH cp_ang AS(
SELECT a.*, ROW_NUMBER()OVER(PARTITION BY cod_postal ORDER BY id_angajat) AS nr_crt
FROM angajati a)
SELECT cp_ang.cod_postal, MAX(nr_crt) AS nr_expeditori, MAX(SYS_CONNECT_BY_PATH(nr_crt||': '||nume||' '||prenume,' \')) AS lista_expeditori
FROM cp_ang
START WITH nr_crt=1
CONNECT BY PRIOR cp_ang.cod_postal=cp_ang.cod_postal AND PRIOR nr_crt=nr_crt-1
GROUP BY cp_ang.cod_postal
ORDER BY 1


/* 21.Lista chitanțelor achitare de către fiecare destinatar în parte in perioada xx.05-xx.07.2018.*/
WITH ch_d AS(
SELECT f.*,nr_chitanta, ROW_NUMBER()OVER(PARTITION BY id_destinatar ORDER BY nr_chitanta) AS nr_crt
FROM facturi f
INNER JOIN chitante ch ON f.nr_factura=ch.nr_factura)
SELECT ch_d.id_destinatar,CAST(d.nume||' '||d.prenume AS VARCHAR(500)) AS destinatar, MAX(nr_crt) AS nr_chitante, MAX(SYS_CONNECT_BY_PATH(ch.nr_chitanta||'('||ch.data_achitare||')',' \')) AS lista_expeditori
FROM ch_d
INNER JOIN chitante ch ON ch_d.nr_chitanta=ch.nr_chitanta
INNER JOIN destinatari d ON ch_d.id_destinatar=d.id_destinatar
WHERE EXTRACT(YEAR FROM data_achitare)=2018 AND
EXTRACT (MONTH FROM data_achitare) BETWEEN 5 AND 7
START WITH nr_crt=1
CONNECT BY PRIOR ch_d.id_destinatar=ch_d.id_destinatar AND PRIOR nr_crt=nr_crt-1
GROUP BY ch_d.id_destinatar,nume,prenume
ORDER BY 1

/* 22.Lista coletelor transportate cu fiecare vehicul în perioada 01.05-06.05.2018. Ce colete au fost transportate prin intermediul fiecărui vehicul în perioada 01.05-06.05.2018? */
WITH col_veh AS(
SELECT t.*, d_t.id_colet,ROW_NUMBER()OVER(PARTITION BY id_vehicul ORDER BY id_colet) AS nr_crt
FROM transporturi t
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND
EXTRACT (MONTH FROM data_plecare)=5 AND 
EXTRACT (DAY FROM data_plecare) BETWEEN 1 AND 6)
SELECT col_veh.id_vehicul,CAST(v.model||' ('||v.nr_inmatriculare||')' AS VARCHAR(500)), MAX(nr_crt) AS nr_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||': '||c.continut_colet,' \')) AS lista_colete
FROM col_veh
INNER JOIN colete c ON col_veh.id_colet=c.id_colet
INNER JOIN vehicule v ON col_veh.id_vehicul=v.id_vehicul
START WITH nr_crt=1
CONNECT BY PRIOR col_veh.id_vehicul=col_veh.id_vehicul AND PRIOR nr_crt=nr_crt-1
GROUP BY col_veh.id_vehicul,model,nr_inmatriculare
ORDER BY 1

/* 23.Lista coletelor primite de către destinatarii din fiecare județ în perioada 04.04-06.04.2018. Ce colete au fost livrate către fiecare județ în perioada 04.04-06.04.2018? */
WITH col_djud AS(
SELECT cp.*, c.id_colet,ROW_NUMBER()OVER(PARTITION BY cp.judet ORDER BY id_colet) AS nr_crt 
FROM coduri_postale cp
INNER JOIN destinatari d ON cp.cod_postal=d.cod_postal
INNER JOIN colete c ON d.id_destinatar=c.id_destinatar
WHERE EXTRACT (YEAR FROM data_primire_colet)=2018 AND
EXTRACT (MONTH FROM data_primire_colet)=4 AND 
EXTRACT (DAY FROM data_primire_colet) BETWEEN 4 AND 6)
SELECT col_djud.judet, MAX(nr_crt) AS nr_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||': '||c.continut_colet,' \')) AS lista_colete
FROM col_djud
INNER JOIN colete c ON col_djud.id_colet=c.id_colet
START WITH nr_crt=1
CONNECT BY PRIOR col_djud.judet=col_djud.judet AND PRIOR nr_crt=nr_crt-1
GROUP BY judet
ORDER BY judet

/* 24.Lista coletelor trimise de expeditorii din fiecare județ în perioada 20.08-24.08.2018. Ce colete au fost trimise(expediate) din fiecare județ în perioada 20.08-24.08.2018? */
WITH col_ejud AS(
SELECT cp.*, c.id_colet,ROW_NUMBER()OVER(PARTITION BY cp.judet ORDER BY id_colet) AS nr_crt 
FROM coduri_postale cp
INNER JOIN expeditori e ON cp.cod_postal=e.cod_postal
INNER JOIN colete c ON e.id_expeditor=c.id_expeditor
WHERE EXTRACT (YEAR FROM data_expediere)=2018 AND
EXTRACT (MONTH FROM data_expediere)=8 AND 
EXTRACT (DAY FROM data_expediere) BETWEEN 20 AND 24)
SELECT col_ejud.judet, MAX(nr_crt) AS nr_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||': '||c.continut_colet,' \')) AS lista_colete
FROM col_ejud
INNER JOIN colete c ON col_ejud.id_colet=c.id_colet
START WITH nr_crt=1
CONNECT BY PRIOR col_ejud.judet=col_ejud.judet AND PRIOR nr_crt=nr_crt-1
GROUP BY judet
ORDER BY judet

/* 25.Lista transporturilor pentru fiecare colet expediat în luna Iulie a anului 2018. Pentru fiecare colet expediat în luna Iulie 2018, să se afișeze lista transporturilor. */
WITH col_tran AS(
SELECT d_t.*,t.data_plecare,ROW_NUMBER()OVER(PARTITION BY id_colet ORDER BY d_t.id_transport) AS nr_crt
FROM detalii_transport d_t 
INNER JOIN transporturi t ON d_t.id_transport=t.id_transport
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND
EXTRACT (MONTH FROM data_plecare)=7)
SELECT col_tran.id_colet,c.continut_colet, MAX(nr_crt) AS nr_transporturi, MAX(SYS_CONNECT_BY_PATH(nr_crt||': '||col_tran.id_transport||'('||col_tran.data_plecare||')',' \')) AS lista_transporturi
FROM col_tran
INNER JOIN colete c ON col_tran.id_colet=c.id_colet
START WITH nr_crt=1
CONNECT BY PRIOR col_tran.id_colet=col_tran.id_colet AND PRIOR nr_crt=nr_crt-1
GROUP BY col_tran.id_colet,c.continut_colet
ORDER BY 1

/* 26.Lista coletelor organizată pe transporturi efectuate în luna Ianuarie. Ce colete conține fiecare transport efectuat în luna Ianuarie? */
WITH i_t AS (
SELECT d_t.*,t.data_plecare,ROW_NUMBER()OVER(PARTITION BY d_t.id_transport ORDER BY d_t.id_colet) AS nr_crt
FROM detalii_transport d_t 
INNER JOIN transporturi t ON d_t.id_transport=t.id_transport
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND 
EXTRACT (MONTH FROM data_plecare)=1)
SELECT i_t.id_transport,MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||'-'||c.continut_colet,' \')) AS lista_colete
FROM i_t
INNER JOIN colete c ON i_t.id_colet=c.id_colet
START WITH nr_crt=1
CONNECT BY PRIOR i_t.id_transport=i_t.id_transport AND PRIOR nr_crt=nr_crt-1
GROUP BY i_t.id_transport
ORDER BY 1

/* 27.Lista coletelor organizată pe transporturi efectuate în anul 2018, ce au avut ca și oră de plecare, ora 15:00. Ce colete conține fiecare transport efectuat în anul 2018, ce au avut ca și oră de plecare, ora 15:00? */
WITH i_t AS (
SELECT d_t.*,t.data_plecare,ROW_NUMBER()OVER(PARTITION BY d_t.id_transport ORDER BY d_t.id_colet) AS nr_crt
FROM detalii_transport d_t 
INNER JOIN transporturi t ON d_t.id_transport=t.id_transport
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND
ora_plecare='15:00')
SELECT i_t.id_transport,MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||'-'||c.continut_colet,' \')) AS lista_colete
FROM i_t
INNER JOIN colete c ON i_t.id_colet=c.id_colet
START WITH nr_crt=1
CONNECT BY PRIOR i_t.id_transport=i_t.id_transport AND PRIOR nr_crt=nr_crt-1
GROUP BY i_t.id_transport
ORDER BY 1

/* 28.Lista coletelor pentru primele 2000 de transporturi. Ce colete conțin primele 2000 de transporturi? */
WITH i_t AS (
SELECT d_t.*,t.data_plecare,ROW_NUMBER()OVER(PARTITION BY d_t.id_transport ORDER BY d_t.id_colet) AS nr_crt
FROM detalii_transport d_t 
INNER JOIN transporturi t ON d_t.id_transport=t.id_transport
WHERE d_t.id_transport<=2000)
SELECT i_t.id_transport,MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||'-'||c.continut_colet,' \')) AS lista_colete
FROM i_t
INNER JOIN colete c ON i_t.id_colet=c.id_colet
START WITH nr_crt=1
CONNECT BY PRIOR i_t.id_transport=i_t.id_transport AND PRIOR nr_crt=nr_crt-1
GROUP BY i_t.id_transport
ORDER BY 1

/* 29.Lista coletelor pentru transporturile efectuate în anul 2018 cu un vehicul, model Ford Transit. Ce colete au fost transportate în anul 2018, cu un vehicul, model Ford Transit? */
WITH i_t AS (
SELECT d_t.*,t.data_plecare,ROW_NUMBER()OVER(PARTITION BY d_t.id_transport ORDER BY d_t.id_colet) AS nr_crt
FROM detalii_transport d_t 
INNER JOIN colete c ON d_t.id_colet=c.id_colet
INNER JOIN transporturi t ON d_t.id_transport=t.id_transport
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul
WHERE v.model='Ford Transit' AND EXTRACT(YEAR FROM data_plecare)=2018)
SELECT i_t.id_transport,MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||'-'||c.continut_colet,' \')) AS lista_colete
FROM i_t
INNER JOIN colete c ON i_t.id_colet=c.id_colet
START WITH nr_crt=1
CONNECT BY PRIOR i_t.id_transport=i_t.id_transport AND PRIOR nr_crt=nr_crt-1
GROUP BY i_t.id_transport
ORDER BY 1

/* 30.Lista coletelor pentru transporturile efectuate în anul 2018 cu un vehicul, model Ford Transit, ce au fost conduse de către șoferi ce au depășit pragul de 5000 de km. Ce colete au fost transportate în anul 2018, cu un vehicul, model Ford Transit, ce au fost conduse de către șoferi ce au depășit pragul de 5000 de km  ? */
WITH i_t AS (
SELECT d_t.*,t.data_plecare,ROW_NUMBER()OVER(PARTITION BY d_t.id_transport ORDER BY d_t.id_colet) AS nr_crt
FROM detalii_transport d_t 
INNER JOIN transporturi t ON d_t.id_transport=t.id_transport
INNER JOIN colete c ON d_t.id_colet=c.id_colet
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul
INNER JOIN soferi s ON t.id_sofer=s.id_sofer 
WHERE model='Ford Transit' AND km_parcursi>=5000 AND EXTRACT(YEAR FROM data_plecare)=2018) 
SELECT i_t.id_transport,MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||'-'||c.continut_colet,' \')) AS lista_colete
FROM i_t
INNER JOIN colete c ON i_t.id_colet=c.id_colet
START WITH nr_crt=1
CONNECT BY PRIOR i_t.id_transport=i_t.id_transport AND PRIOR nr_crt=nr_crt-1
GROUP BY i_t.id_transport
ORDER BY 1

/* 31.Lista coletelor pentru transporturile efectuate în anul 2018 cu un vehicul, model Ford Transit, ce au fost conduse de către șoferi ce au depășit pragul de 5000 de km. Ce colete au fost transportate în anul 2018, cu un vehicul, model Ford Transit, ce au fost conduse de către șoferi ce au depășit pragul de 5000 de km  ? */
WITH i_t AS (
SELECT d_t.*,t.id_vehicul,t.id_sofer,t.data_plecare,ROW_NUMBER()OVER(PARTITION BY d_t.id_transport ORDER BY d_t.id_colet) AS nr_crt
FROM detalii_transport d_t 
INNER JOIN transporturi t ON d_t.id_transport=t.id_transport
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul
INNER JOIN soferi s ON t.id_sofer=s.id_sofer 
WHERE model='Ford Transit' AND km_parcursi>=5000 AND EXTRACT(YEAR FROM data_plecare)=2018)
SELECT i_t.id_transport,CAST('Depozitul '||t.id_depozit_1||' ('||d.adresa||')' AS VARCHAR(500)) AS depozit, CAST(a.nume||' '||a.prenume AS VARCHAR(500)) AS sofer,MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||'-'||c.continut_colet,' \')) AS lista_colete
FROM i_t
INNER JOIN colete c ON i_t.id_colet=c.id_colet
INNER JOIN soferi s ON i_t.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
INNER JOIN transporturi t ON i_t.id_transport=t.id_transport
INNER JOIN depozite d ON t.id_depozit_1=d.id_depozit
START WITH nr_crt=1
CONNECT BY PRIOR i_t.id_transport=i_t.id_transport AND PRIOR nr_crt=nr_crt-1
GROUP BY i_t.id_transport,t.id_depozit_1,d.adresa,a.nume,a.prenume
ORDER BY 1

/* 32.Lista coletelor organizată pe facturi întocmite în perioada 01.01-30.01.2018. Ce colete conține fiecare factură întocmită în perioada 01.01-30.01.2018? */
WITH l_f AS(
SELECT d_f.*,f.data,ROW_NUMBER()OVER(PARTITION BY d_f.nr_factura ORDER BY d_f.id_colet) AS nr_crt
FROM detalii_facturi d_f
INNER JOIN facturi f ON d_f.nr_factura=f.nr_factura
WHERE EXTRACT (YEAR FROM data)=2018)
SELECT nr_factura,MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||'-'||c.continut_colet,' \')) AS lista_colete
FROM l_f
INNER JOIN colete c ON l_f.id_colet=c.id_colet
START WITH nr_crt=1
CONNECT BY PRIOR l_f.nr_factura=l_f.nr_factura AND PRIOR nr_crt=nr_crt-1
GROUP BY nr_factura
ORDER BY nr_factura

/* 33.Lista coletelor organizată pe facturi întocmite în anul 2018 de către primii 90.000 de expeditori. Ce colete conține fiecare factură întocmită în anul 2018 de către primii 90.000 de expeditori?*/
WITH l_f AS(
SELECT d_f.*,f.data,ROW_NUMBER()OVER(PARTITION BY d_f.nr_factura ORDER BY d_f.id_colet) AS nr_crt
FROM detalii_facturi d_f
INNER JOIN facturi f ON d_f.nr_factura=f.nr_factura
WHERE EXTRACT (YEAR FROM data)=2018 AND f.id_expeditor BETWEEN 1 AND 90000)
SELECT nr_factura,MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||'-'||c.continut_colet,' \')) AS lista_colete
FROM l_f
INNER JOIN colete c ON l_f.id_colet=c.id_colet
START WITH nr_crt=1
CONNECT BY PRIOR l_f.nr_factura=l_f.nr_factura AND PRIOR nr_crt=nr_crt-1
GROUP BY nr_factura
ORDER BY nr_factura

/* 34.Lista coletelor ce depășesc valoarea de 1500lei și greutatea de 15kg organizată pe facturi primite în anul 2018 de către primii 85.000 de destinatari. Ce colete ce depășesc valoarea de 1500lei și greutatea de 15kg conține fiecare factură primită în anul 2018 de către primii 85.000 de destinatari?*/
WITH l_f AS(
SELECT d_f.*,c.valoare,c.greutate,CAST(d.nume||' '||d.prenume AS VARCHAR(500)) AS destinatar,f.data,ROW_NUMBER()OVER(PARTITION BY d_f.nr_factura ORDER BY d_f.id_colet) AS nr_crt
FROM detalii_facturi d_f
INNER JOIN facturi f ON d_f.nr_factura=f.nr_factura
INNER JOIN destinatari d ON f.id_destinatar=d.id_destinatar
INNER JOIN colete c ON d_f.id_colet=c.id_colet
WHERE EXTRACT (YEAR FROM data)=2018 AND f.id_destinatar BETWEEN 1 AND 85000 AND 
valoare>1500 AND greutate<15)
SELECT nr_factura,destinatar,MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||'-'||c.continut_colet,' \')) AS lista_colete
FROM l_f
INNER JOIN colete c ON l_f.id_colet=c.id_colet
START WITH nr_crt=1
CONNECT BY PRIOR l_f.nr_factura=l_f.nr_factura AND PRIOR nr_crt=nr_crt-1
GROUP BY nr_factura,destinatar
ORDER BY nr_factura

/* 35.Lista coletelor ce depășesc valoarea de 500lei și greutatea de 5kg organizată pe facturi primite în anul 2018 de către primii 95.000 de destinatari din județul Iași. Ce colete ce depășesc valoarea de 1500lei și greutatea de 15kg conține fiecare factură primită în anul 2018 de către primii 95.000 de destinatari din judetul Iași?*/
WITH l_f AS(
SELECT d_f.*,c.valoare,c.greutate,CAST(d.nume||' '||d.prenume AS VARCHAR(500)) AS destinatar,f.data,ROW_NUMBER()OVER(PARTITION BY d_f.nr_factura ORDER BY d_f.id_colet) AS nr_crt
FROM detalii_facturi d_f
INNER JOIN facturi f ON d_f.nr_factura=f.nr_factura
INNER JOIN destinatari d ON f.id_destinatar=d.id_destinatar
INNER JOIN coduri_postale c_p ON d.cod_postal=c_p.cod_postal
INNER JOIN colete c ON d_f.id_colet=c.id_colet
WHERE EXTRACT (YEAR FROM data)=2018 AND f.id_destinatar BETWEEN 1 AND 95000 AND 
valoare>500 AND greutate<5 AND judet='Iași')
SELECT nr_factura,destinatar,MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||'-'||c.continut_colet,' \')) AS lista_colete
FROM l_f
INNER JOIN colete c ON l_f.id_colet=c.id_colet
START WITH nr_crt=1
CONNECT BY PRIOR l_f.nr_factura=l_f.nr_factura AND PRIOR nr_crt=nr_crt-1
GROUP BY nr_factura,destinatar
ORDER BY nr_factura

/* 36.Lista coletelor expediate de către fiecare expeditor în parte în prima luna a anului 2018. Ce colete a expediat fiecare expeditor în parte în prima luna a anului 2018? */
WITH c_e AS(
SELECT c.*, ROW_NUMBER()OVER(PARTITION BY id_expeditor ORDER BY id_colet) AS nr_crt
FROM colete c
WHERE EXTRACT (YEAR FROM data_expediere)=2018 AND 
EXTRACT (MONTH FROM data_expediere)=1)
SELECT c.id_expeditor,CAST(e.nume||' '||e.prenume AS VARCHAR(500)) AS expeditor,MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(c.id_colet||': '||c.continut_colet,' \')) AS lista_colete
FROM c_e
INNER JOIN colete c ON c_e.id_colet=c.id_colet
INNER JOIN expeditori e ON c_e.id_expeditor=e.id_expeditor
START WITH nr_crt=1
CONNECT BY PRIOR c_e.id_expeditor=c_e.id_expeditor AND PRIOR nr_crt=nr_crt-1
GROUP BY c.id_expeditor,e.nume,e.prenume
ORDER BY id_expeditor

/* 37.Lista coletelor expediate de către fiecare expeditor în parte pe parcursul anului 2018. Ce colete a expediat fiecare expeditor în parte pe parcursul anului 2018 ? */
WITH c_e AS(
SELECT c.*, ROW_NUMBER()OVER(PARTITION BY id_expeditor ORDER BY id_colet) AS nr_crt
FROM colete c
WHERE EXTRACT (YEAR FROM data_expediere)=2018)
SELECT c.id_expeditor,CAST(e.nume||' '||e.prenume||' ('||cp.judet||')' AS VARCHAR(500)) AS expeditor,MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(c.id_colet||': '||c.continut_colet,' \')) AS lista_colete
FROM c_e
INNER JOIN colete c ON c_e.id_colet=c.id_colet
INNER JOIN expeditori e ON c_e.id_expeditor=e.id_expeditor
INNER JOIN coduri_postale cp ON e.cod_postal=cp.cod_postal
START WITH nr_crt=1
CONNECT BY PRIOR c_e.id_expeditor=c_e.id_expeditor AND PRIOR nr_crt=nr_crt-1
GROUP BY c.id_expeditor,e.nume,e.prenume,cp.judet
ORDER BY id_expeditor

/* 38.Lista coletelor cu o greutate mai mică de 1kg expediate de către fiecare expeditor,cu excepția celor din București pe parcursul anului 2018. Ce colete cu o greutate mai mică de 1kg a expediat fiecare expeditor,cu excepția celor din București pe parcursul anului 2018? */
WITH c_e AS(
SELECT c.*,cp.judet, ROW_NUMBER()OVER(PARTITION BY c.id_expeditor ORDER BY id_colet) AS nr_crt
FROM colete c
INNER JOIN expeditori e ON c.id_expeditor=e.id_expeditor
INNER JOIN coduri_postale cp ON e.cod_postal=cp.cod_postal
WHERE EXTRACT (YEAR FROM data_expediere)=2018 AND greutate<=1 AND judet!='București')
SELECT c.id_expeditor,CAST(e.nume||' '||e.prenume||' ('||cp.judet||')' AS VARCHAR(500)) AS expeditor,MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(c.id_colet||': '||c.continut_colet,' \')) AS lista_colete
FROM c_e
INNER JOIN colete c ON c_e.id_colet=c.id_colet
INNER JOIN expeditori e ON c_e.id_expeditor=e.id_expeditor
INNER JOIN coduri_postale cp ON e.cod_postal=cp.cod_postal
START WITH nr_crt=1
CONNECT BY PRIOR c_e.id_expeditor=c_e.id_expeditor AND PRIOR nr_crt=nr_crt-1
GROUP BY c.id_expeditor,e.nume,e.prenume,cp.judet
ORDER BY id_expeditor

/* 39.Lista coletelor primite de către fiecare destinatar în lunile Februarie, Martie și Aprilie ale anului 2018. Ce colete a primit fiecare destinatar în lunile Februarie, Martie și Aprilie ale anului 2018?*/
WITH c_d AS(
SELECT c.*, ROW_NUMBER()OVER(PARTITION BY id_destinatar ORDER BY id_colet) AS nr_crt
FROM colete c
WHERE EXTRACT (YEAR FROM data_primire_colet)=2018 AND 
EXTRACT (MONTH FROM data_primire_colet)BETWEEN 2 AND 4)
SELECT c.id_destinatar,CAST(d.nume||' '||d.prenume AS VARCHAR(500)) AS destinatar,MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(c.id_colet||': '||c.continut_colet,' \')) AS lista_colete
FROM c_d
INNER JOIN colete c ON c_d.id_colet=c.id_colet
INNER JOIN destinatari d ON c_d.id_destinatar=d.id_destinatar
START WITH nr_crt=1
CONNECT BY PRIOR c_d.id_destinatar=c_d.id_destinatar AND PRIOR nr_crt=nr_crt-1
GROUP BY c.id_destinatar,d.nume,d.prenume
ORDER BY id_destinatar

/* 40.Lista coletelor primite de către fiecare destinatar persoană juridică în anul 2018. Ce colete a primit fiecare destinatar persoană juridică în anul 2018? */
WITH c_d AS(
SELECT c.*, ROW_NUMBER()OVER(PARTITION BY c.id_destinatar ORDER BY id_colet) AS nr_crt
FROM colete c
INNER JOIN destinatari d ON c.id_destinatar=d.id_destinatar
WHERE EXTRACT (YEAR FROM data_primire_colet)=2018 AND d.prenume IS NULL)
SELECT c.id_destinatar,CAST(d.nume||' '||d.prenume AS VARCHAR(500)) AS destinatar,MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(c.id_colet||': '||c.continut_colet,' \')) AS lista_colete
FROM c_d
INNER JOIN colete c ON c_d.id_colet=c.id_colet
INNER JOIN destinatari d ON c_d.id_destinatar=d.id_destinatar
START WITH nr_crt=1
CONNECT BY PRIOR c_d.id_destinatar=c_d.id_destinatar AND PRIOR nr_crt=nr_crt-1
GROUP BY c.id_destinatar,d.nume,d.prenume
ORDER BY id_destinatar

/* 41.Lista coletelor primite de către fiecare destinatar persoană juridică, cu excepția celor din județul Iași în anul 2018. Ce colete a primit fiecare destinatar persoană juridică Lista coletelor primite de către fiecare destinatar persoană juridică, cu excepția celor din județul Iași în anul 2018? */
WITH c_d AS(
SELECT c.*, ROW_NUMBER()OVER(PARTITION BY c.id_destinatar ORDER BY id_colet) AS nr_crt
FROM colete c
INNER JOIN destinatari d ON c.id_destinatar=d.id_destinatar
INNER JOIN coduri_postale cp ON d.cod_postal=cp.cod_postal
WHERE EXTRACT (YEAR FROM data_primire_colet)=2018 AND d.prenume IS NULL AND judet!='Iași')
SELECT c.id_destinatar,CAST(d.nume||' '||d.prenume AS VARCHAR(500)) AS destinatar,MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(c.id_colet||': '||c.continut_colet,' \')) AS lista_colete
FROM c_d
INNER JOIN colete c ON c_d.id_colet=c.id_colet
INNER JOIN destinatari d ON c_d.id_destinatar=d.id_destinatar
START WITH nr_crt=1
CONNECT BY PRIOR c_d.id_destinatar=c_d.id_destinatar AND PRIOR nr_crt=nr_crt-1
GROUP BY c.id_destinatar,d.nume,d.prenume
ORDER BY id_destinatar

/* 42.Lista coletelor ce depășesc valoarea de 2500 de lei și au plecat din fiecare depozit în luna Octombrie 2018. Ce colete ce depășesc valoare de 2500 de lei au plecat din fiecare depozit în luna Octombrie 2018? */
WITH col_dep AS(
SELECT t.id_depozit_1, d_t.id_colet,c.valoare,ROW_NUMBER()OVER(PARTITION BY id_depozit_1 ORDER BY d_t.id_colet) AS nr_crt
FROM transporturi t
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport
INNER JOIN colete c ON d_t.id_colet=c.id_colet
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND
EXTRACT (MONTH FROM data_plecare)=10 AND valoare>2500)
SELECT CAST('Depozitul '||col_dep.id_depozit_1||' ('||d.adresa||')' AS VARCHAR(500)) AS Depozit, MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||': '||c.continut_colet,' \')) AS lista_colete
FROM col_dep
INNER JOIN colete c ON col_dep.id_colet=c.id_colet
INNER JOIN depozite d ON col_dep.id_depozit_1=d.id_depozit
START WITH nr_crt=1
CONNECT BY PRIOR col_dep.id_depozit_1=col_dep.id_depozit_1 AND PRIOR nr_crt=nr_crt-1
GROUP BY col_dep.id_depozit_1,d.adresa
ORDER BY id_depozit_1

/* 43.Lista coletelor ce au plecat din fiecare depozit în luna Octombrie 2018 cu un vehicul de tip Mercedes Sprinter. Ce colete au plecat din fiecare depozit în luna Octombrie 2018 cu un vehicul de tip Mercedes Sprinter.*/
WITH col_dep AS(
SELECT t.id_depozit_1, d_t.id_colet,v.model,ROW_NUMBER()OVER(PARTITION BY id_depozit_1 ORDER BY id_colet) AS nr_crt
FROM transporturi t
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport 
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND
EXTRACT (MONTH FROM data_plecare)=10 AND v.model='Mercedes Sprinter')
SELECT CAST('Depozitul '||col_dep.id_depozit_1||' ('||d.adresa||')' AS VARCHAR(500)) AS Depozit, MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||': '||c.continut_colet,' \')) AS lista_colete
FROM col_dep
INNER JOIN colete c ON col_dep.id_colet=c.id_colet
INNER JOIN depozite d ON col_dep.id_depozit_1=d.id_depozit
START WITH nr_crt=1
CONNECT BY PRIOR col_dep.id_depozit_1=col_dep.id_depozit_1 AND PRIOR nr_crt=nr_crt-1
GROUP BY col_dep.id_depozit_1,d.adresa
ORDER BY id_depozit_1

/* 44.Lista coletelor ce au plecat din fiecare depozit în anul 2018 cu un vehicul de tip Mercedes Sprinter mai nou de anul 2014. Ce colete au plecat din fiecare depozit în anul 2018 cu un vehicul de tip Mercedes Sprinter mai nou de anul 2014?.*/
WITH col_dep AS(
SELECT t.id_depozit_1, d_t.id_colet,v.model,v.an_fabricatie,ROW_NUMBER()OVER(PARTITION BY id_depozit_1 ORDER BY id_colet) AS nr_crt
FROM transporturi t
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport 
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND v.model='Mercedes Sprinter' AND v.an_fabricatie>2014)
SELECT CAST('Depozitul '||col_dep.id_depozit_1||' ('||d.adresa||')' AS VARCHAR(500)) AS Depozit, MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||': '||c.continut_colet,' \')) AS lista_colete
FROM col_dep
INNER JOIN colete c ON col_dep.id_colet=c.id_colet
INNER JOIN depozite d ON col_dep.id_depozit_1=d.id_depozit
START WITH nr_crt=1
CONNECT BY PRIOR col_dep.id_depozit_1=col_dep.id_depozit_1 AND PRIOR nr_crt=nr_crt-1
GROUP BY col_dep.id_depozit_1,d.adresa
ORDER BY id_depozit_1

/* 45.Lista coletelor ce au plecat din fiecare depozit în anul 2018 cu un vehicul de tip Mercedes Sprinter mai nou de anul 2014 și care au fost conduse de către un șofer ce a efectuat minim 3000 de km. Ce colete au plecat din fiecare depozit în anul 20188 cu un vehicul de tip Mercedes Sprinter mai nou de anul 2005 și care au fost conduse de către un șofer ce a efectuat minim 3000 de km. */
WITH col_dep AS(
SELECT t.id_depozit_1, d_t.id_colet,v.model,v.an_fabricatie,ROW_NUMBER()OVER(PARTITION BY id_depozit_1 ORDER BY id_colet) AS nr_crt
FROM transporturi t
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul
INNER JOIN soferi s ON t.id_sofer=s.id_sofer
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport 
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND v.model='Mercedes Sprinter' AND v.an_fabricatie>2014 AND s.km_parcursi>3000)
SELECT CAST('Depozitul '||col_dep.id_depozit_1||' ('||d.adresa||')' AS VARCHAR(500)) AS Depozit, MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||': '||c.continut_colet,' \')) AS lista_colete
FROM col_dep
INNER JOIN colete c ON col_dep.id_colet=c.id_colet
INNER JOIN depozite d ON col_dep.id_depozit_1=d.id_depozit
START WITH nr_crt=1
CONNECT BY PRIOR col_dep.id_depozit_1=col_dep.id_depozit_1 AND PRIOR nr_crt=nr_crt-1
GROUP BY col_dep.id_depozit_1,d.adresa
ORDER BY id_depozit_1

/* 46.Lista coletelor ce au plecat din fiecare depozit în anul 2018 cu un vehicul de tip Mercedes Sprinter mai nou de anul 2005 și care au fost conduse de către un șofer ce a efectuat minim 3000 de km și nu este din județul Iași. Ce colete au plecat din fiecare depozit în anul 20188 cu un vehicul de tip Mercedes Sprinter mai nou de anul 2005 și care au fost conduse de către un șofer ce a efectuat minim 3000 de km și nu este din județul Iași?*/
WITH col_dep AS(
SELECT t.id_depozit_1, d_t.id_colet,v.model,v.an_fabricatie,ROW_NUMBER()OVER(PARTITION BY id_depozit_1 ORDER BY id_colet) AS nr_crt
FROM transporturi t
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul
INNER JOIN soferi s ON t.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
INNER JOIN coduri_postale cp ON a.cod_postal=cp.cod_postal
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport 
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND v.model='Mercedes Sprinter' AND v.an_fabricatie>2014 AND s.km_parcursi>3000 AND judet!='Iași')
SELECT CAST('Depozitul '||col_dep.id_depozit_1||' ('||d.adresa||')' AS VARCHAR(500)) AS Depozit, MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||': '||c.continut_colet,' \')) AS lista_colete
FROM col_dep
INNER JOIN colete c ON col_dep.id_colet=c.id_colet
INNER JOIN depozite d ON col_dep.id_depozit_1=d.id_depozit
START WITH nr_crt=1
CONNECT BY PRIOR col_dep.id_depozit_1=col_dep.id_depozit_1 AND PRIOR nr_crt=nr_crt-1
GROUP BY col_dep.id_depozit_1,d.adresa
ORDER BY id_depozit_1

/* 47.Lista coletelor transportate din depozit în depozit în luna Iunie 2018. Ce colete au fost transportate din depozit în depozit în luna Iunie 2018? */
WITH col_dep2 AS(
SELECT t.*,CAST('Depozitul '||t.id_depozit_1||' -> '||'Depozitul '||t.id_depozit_2 AS VARCHAR(500)) AS depozite,d_t.id_colet, ROW_NUMBER()OVER(PARTITION BY id_depozit_1,id_depozit_2 ORDER BY id_colet) AS nr_crt
FROM transporturi t 
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport 
WHERE id_depozit_2 IS NOT NULL AND 
EXTRACT (YEAR FROM data_plecare)=2018 AND
EXTRACT (MONTH FROM data_plecare)=6)
SELECT col_dep2.depozite, MAX(nr_crt) AS nr_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||': '||c.continut_colet,' \')) AS lista_colete
FROM col_dep2
INNER JOIN colete c ON col_dep2.id_colet=c.id_colet
START WITH nr_crt=1
CONNECT BY PRIOR col_dep2.depozite=col_dep2.depozite AND PRIOR nr_crt=nr_crt-1
GROUP BY col_dep2.depozite,col_dep2.id_depozit_1
ORDER BY col_dep2.id_depozit_1

/* 48.Lista coletelor transportate din depozit în depozit în luna Iunie 2018 cu vehicule inmatriculate în anii 2015+. Ce colete au fost transportate din depozit în depozit în luna Iunie 2018 cu vehicule inmatriculate în anii 2015+ ? */
WITH col_dep2 AS(
SELECT t.*,CAST('Depozitul '||t.id_depozit_1||' -> '||'Depozitul '||t.id_depozit_2 AS VARCHAR(500)) AS depozite,d_t.id_colet, ROW_NUMBER()OVER(PARTITION BY id_depozit_1,id_depozit_2 ORDER BY id_colet) AS nr_crt
FROM transporturi t 
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport 
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul
WHERE id_depozit_2 IS NOT NULL AND 
EXTRACT (YEAR FROM data_plecare)=2018 AND
EXTRACT (MONTH FROM data_plecare)=6 AND EXTRACT(YEAR FROM data_inmatriculare)>2015)
SELECT col_dep2.depozite, MAX(nr_crt) AS nr_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||': '||c.continut_colet,' \')) AS lista_colete
FROM col_dep2
INNER JOIN colete c ON col_dep2.id_colet=c.id_colet
START WITH nr_crt=1
CONNECT BY PRIOR col_dep2.depozite=col_dep2.depozite AND PRIOR nr_crt=nr_crt-1
GROUP BY col_dep2.depozite,col_dep2.id_depozit_1
ORDER BY col_dep2.id_depozit_1

/* 49.Lista coletelor transportate din depozit în depozit în anul 2018 cu vehicule de tip Dacia Dokker Van, inmatriculate în anii 2015+. Ce colete au fost transportate din depozit în depozit în anul 2018 cu vehicule de tip Dacia Dokker Van, inmatriculate în anii 2015+ ? */
WITH col_dep2 AS(
SELECT t.*,CAST('Depozitul '||t.id_depozit_1||' -> '||'Depozitul '||t.id_depozit_2 AS VARCHAR(500)) AS depozite,d_t.id_colet, ROW_NUMBER()OVER(PARTITION BY id_depozit_1,id_depozit_2 ORDER BY id_colet) AS nr_crt
FROM transporturi t 
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport 
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul
WHERE id_depozit_2 IS NOT NULL AND 
EXTRACT (YEAR FROM data_plecare)=2018 AND EXTRACT(YEAR FROM data_inmatriculare)>2015 AND v.model='Dacia Dokker Van')
SELECT col_dep2.depozite, MAX(nr_crt) AS nr_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||': '||c.continut_colet,' \')) AS lista_colete
FROM col_dep2
INNER JOIN colete c ON col_dep2.id_colet=c.id_colet
START WITH nr_crt=1
CONNECT BY PRIOR col_dep2.depozite=col_dep2.depozite AND PRIOR nr_crt=nr_crt-1
GROUP BY col_dep2.depozite,col_dep2.id_depozit_1
ORDER BY col_dep2.id_depozit_1

/* 50.Lista coletelor transportate din depozit în depozit în anul 2018 cu vehicule de tip Dacia Dokker Van, inmatriculate în anii 2015+ și conduse de către șoferi care nu sunt din județul Cluj. Ce colete au fost transportate din depozit în depozit în anul 2018 cu vehicule de tip Dacia Dokker Van, inmatriculate în anii 2015+ și conduse de către șoferi care nu sunt din județul Cluj? */
WITH col_dep2 AS(
SELECT t.*,v.data_inmatriculare,CAST('Depozitul '||t.id_depozit_1||' -> '||'Depozitul '||t.id_depozit_2 AS VARCHAR(500)) AS depozite,d_t.id_colet, ROW_NUMBER()OVER(PARTITION BY id_depozit_1,id_depozit_2 ORDER BY id_colet) AS nr_crt
FROM transporturi t 
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport 
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul
INNER JOIN soferi s ON t.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
INNER JOIN coduri_postale cp ON a.cod_postal=cp.cod_postal
WHERE id_depozit_2 IS NOT NULL AND 
EXTRACT (YEAR FROM data_plecare)=2018 AND EXTRACT(YEAR FROM data_inmatriculare)>2015 AND v.model='Dacia Dokker Van' AND judet!='Cluj')
SELECT col_dep2.depozite, MAX(nr_crt) AS nr_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||': '||c.continut_colet,' \')) AS lista_colete
FROM col_dep2
INNER JOIN colete c ON col_dep2.id_colet=c.id_colet
START WITH nr_crt=1
CONNECT BY PRIOR col_dep2.depozite=col_dep2.depozite AND PRIOR nr_crt=nr_crt-1
GROUP BY col_dep2.depozite,col_dep2.id_depozit_1
ORDER BY col_dep2.id_depozit_1

/* 51.Lista transporturilor efectuate de catre feicare șofer în parte pe parcursul anului 2018. Ce transporturi a efectuat fiecare șofer pe parcursul anului 2018? */
WITH tr_s AS(
SELECT t.*,ROW_NUMBER()OVER(PARTITION BY id_sofer ORDER BY id_transport) AS nr_crt
FROM transporturi t
WHERE EXTRACT (YEAR FROM data_plecare)=2018)
SELECT tr_s.id_sofer, CAST(nume||' '||prenume AS VARCHAR(4000))AS sofer, MAX(nr_crt) AS nr_transporturi,MAX(SYS_CONNECT_BY_PATH(nr_crt||': '||id_transport||'('||data_plecare||')'||' ','\')) AS Transporturi 
FROM tr_s
INNER JOIN soferi s ON tr_s.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
START WITH nr_crt=1
CONNECT BY PRIOR tr_s.id_sofer=tr_s.id_sofer AND PRIOR nr_crt=nr_crt-1
GROUP BY tr_s.id_sofer, nume, prenume
ORDER BY 1

/* 52.Lista transporturilor efectuate de catre fiecare șofer care nu este din județul Iași pe parcursul anului 2018. Ce transporturi a efectuat fiecare șofer care nu este din județul Iași pe parcursul anului 2018? */
WITH tr_s AS(
SELECT t.*,ROW_NUMBER()OVER(PARTITION BY t.id_sofer ORDER BY id_transport) AS nr_crt
FROM transporturi t
INNER JOIN soferi s ON t.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
INNER JOIN coduri_postale cp ON a.cod_postal=cp.cod_postal
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND judet!='Iași')
SELECT tr_s.id_sofer, CAST(nume||' '||prenume AS VARCHAR(4000))AS sofer, MAX(nr_crt) AS nr_transporturi,MAX(SYS_CONNECT_BY_PATH(nr_crt||': '||id_transport||'('||data_plecare||')'||' ','\')) AS Transporturi 
FROM tr_s
INNER JOIN soferi s ON tr_s.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
START WITH nr_crt=1
CONNECT BY PRIOR tr_s.id_sofer=tr_s.id_sofer AND PRIOR nr_crt=nr_crt-1
GROUP BY tr_s.id_sofer, nume, prenume
ORDER BY 1

/* 53.Lista transporturilor efectuate cu fiecare mijloc de transport în anul 2018. Ce transporturi au fost efectuate cu fiecare mijloc de transport în anul 2018? */
WITH tr_v AS(
SELECT t.*,ROW_NUMBER()OVER(PARTITION BY id_vehicul ORDER BY id_transport) AS linie
FROM transporturi t
WHERE EXTRACT (YEAR FROM data_plecare)=2018)
SELECT tr_v.id_vehicul, CAST(v.model||' ('||v.nr_inmatriculare||')' AS VARCHAR(500)) AS model,MAX(linie) AS nr_transporturi,MAX(SYS_CONNECT_BY_PATH(linie||': '||id_transport||'('||data_plecare||') ','\')) AS Lista_transporturi
FROM tr_v
INNER JOIN vehicule v ON tr_v.id_vehicul=v.id_vehicul
START WITH linie=1
CONNECT BY PRIOR tr_v.id_vehicul=tr_v.id_vehicul AND PRIOR linie=linie-1
GROUP BY tr_v.id_vehicul, model, nr_inmatriculare
ORDER BY 1

/* 54.Lista transporturilor efectuate cu fiecare mijloc de transport în anul 2018 cu vehicule ce au fost inmatriculate cel putin în anul 2015. Ce transporturi au fost efectuate cu fiecare mijloc de transport în anul 2018 cu vehicule ce au fost inmatriculate cel putin în anul 2015 */
WITH tr_v AS(
SELECT t.*,ROW_NUMBER()OVER(PARTITION BY t.id_vehicul ORDER BY id_transport) AS linie
FROM transporturi t
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND EXTRACT(YEAR FROM data_inmatriculare)>=2015)
SELECT tr_v.id_vehicul, CAST(v.model||' ('||v.nr_inmatriculare||')' AS VARCHAR(500)) AS model,MAX(linie) AS nr_transporturi,MAX(SYS_CONNECT_BY_PATH(linie||': '||id_transport||'('||data_plecare||') ','\')) AS Lista_transporturi
FROM tr_v
INNER JOIN vehicule v ON tr_v.id_vehicul=v.id_vehicul
START WITH linie=1
CONNECT BY PRIOR tr_v.id_vehicul=tr_v.id_vehicul AND PRIOR linie=linie-1
GROUP BY tr_v.id_vehicul, model, nr_inmatriculare
ORDER BY 1

/* 55.Lista transporturilor ce au plecat din fiecare depozit în luna Martie 2018. Ce transporturi au plecat din fiecare depozit în luna Martie 2018? */
WITH tr_dep AS(
SELECT t.*,ROW_NUMBER()OVER(PARTITION BY id_depozit_1 ORDER BY id_transport) AS linie
FROM transporturi t
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND 
EXTRACT (MONTH FROM data_plecare)=3)
SELECT CAST('Depozitul ' ||tr_dep.id_depozit_1||'('||adresa||', '||c_p.judet||')' AS VARCHAR(500)) AS depozit, MAX(linie) AS nr_transporturi, MAX(SYS_CONNECT_BY_PATH(linie||': '||id_transport||' ('||data_plecare||')',' \')) AS Lista_transporturi
FROM tr_dep
INNER JOIN depozite d ON tr_dep.id_depozit_1=d.id_depozit
INNER JOIN coduri_postale c_p ON d.cod_postal=c_p.cod_postal
START WITH linie=1 
CONNECT BY PRIOR tr_dep.id_depozit_1=tr_dep.id_depozit_1 AND PRIOR linie=linie-1
GROUP BY id_depozit_1, adresa,judet
ORDER BY id_depozit_1

/* 56.Lista transporturilor ce au plecat din fiecare depozit în anul 2018. Ce transporturi au plecat din fiecare depozit în anul 2018?*/
WITH tr_dep AS(
SELECT t.*,ROW_NUMBER()OVER(PARTITION BY id_depozit_1 ORDER BY id_transport) AS linie
FROM transporturi t
WHERE EXTRACT (YEAR FROM data_plecare)=2018)
SELECT CAST('Depozitul ' ||tr_dep.id_depozit_1||'('||adresa||', '||c_p.judet||')' AS VARCHAR(500)) AS depozit, MAX(linie) AS nr_transporturi, MAX(SYS_CONNECT_BY_PATH(linie||': '||id_transport||' ('||data_plecare||')',' \')) AS Lista_transporturi
FROM tr_dep
INNER JOIN depozite d ON tr_dep.id_depozit_1=d.id_depozit
INNER JOIN coduri_postale c_p ON d.cod_postal=c_p.cod_postal
START WITH linie=1 
CONNECT BY PRIOR tr_dep.id_depozit_1=tr_dep.id_depozit_1 AND PRIOR linie=linie-1
GROUP BY id_depozit_1, adresa,judet
ORDER BY id_depozit_1

/* 57.Lista transporturilor ce au plecat din fiecare depozit în anul 2018 cu vehicule de tip Ford Transit ce au fost înmatriculate începând cu anul 2015. Ce transporturi au plecat din fiecare depozit în anul 2018 cu vehicule de tip Ford Transit ce au fost înmatriculate începând cu anul 2015 ?*/
WITH tr_dep AS(
SELECT t.*,ROW_NUMBER()OVER(PARTITION BY id_depozit_1 ORDER BY id_transport) AS linie
FROM transporturi t
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND EXTRACT(YEAR FROM data_inmatriculare)>=2015 AND v.model='Ford Transit')
SELECT CAST('Depozitul ' ||tr_dep.id_depozit_1||'('||adresa||', '||c_p.judet||')' AS VARCHAR(500)) AS depozit, MAX(linie) AS nr_transporturi, MAX(SYS_CONNECT_BY_PATH(linie||': '||id_transport||' ('||data_plecare||')',' \')) AS Lista_transporturi
FROM tr_dep
INNER JOIN depozite d ON tr_dep.id_depozit_1=d.id_depozit
INNER JOIN coduri_postale c_p ON d.cod_postal=c_p.cod_postal
START WITH linie=1 
CONNECT BY PRIOR tr_dep.id_depozit_1=tr_dep.id_depozit_1 AND PRIOR linie=linie-1
GROUP BY id_depozit_1, adresa,judet
ORDER BY id_depozit_1

/* 58.Lista transporturilor ce au plecat din fiecare depozit în anul 2018 cu vehicule de tip Ford Transit ce au fost înmatriculate începând cu anul 2015 și care au fost conduse de către șoferi care nu sunt din județul Bihor. Ce transporturi au plecat din fiecare depozit în anul 2018 cu vehicule de tip Ford Transit ce au fost înmatriculate începând cu anul 2015 și care au fost conduse de către șoferi care nu sunt din județul Bihor. ?*/
WITH tr_dep AS(
SELECT t.*,ROW_NUMBER()OVER(PARTITION BY id_depozit_1 ORDER BY id_transport) AS linie
FROM transporturi t
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul
INNER JOIN soferi s ON t.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
INNER JOIN coduri_postale c_p ON a.cod_postal=c_p.cod_postal
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND EXTRACT(YEAR FROM data_inmatriculare)>=2015 AND v.model='Ford Transit' AND judet!='Bihor')
SELECT CAST('Depozitul ' ||tr_dep.id_depozit_1||'('||adresa||', '||c_p.judet||')' AS VARCHAR(500)) AS depozit, MAX(linie) AS nr_transporturi, MAX(SYS_CONNECT_BY_PATH(linie||': '||id_transport||' ('||data_plecare||')',' \')) AS Lista_transporturi
FROM tr_dep
INNER JOIN depozite d ON tr_dep.id_depozit_1=d.id_depozit
INNER JOIN coduri_postale c_p ON d.cod_postal=c_p.cod_postal
START WITH linie=1 
CONNECT BY PRIOR tr_dep.id_depozit_1=tr_dep.id_depozit_1 AND PRIOR linie=linie-1
GROUP BY id_depozit_1, adresa,judet
ORDER BY id_depozit_1

/* 59.Lista coletelor returnate în anul 2018. Ce colete au fost returnate în anul 2018? */
WITH col_ret AS(
SELECT r.*,d_f.id_colet,ROW_NUMBER()OVER(PARTITION BY id_retur ORDER BY d_f.id_colet) AS nr_crt
FROM retururi r
INNER JOIN detalii_facturi d_f ON r.nr_factura=d_f.nr_factura
WHERE EXTRACT (YEAR FROM data_retur)=2018)
SELECT CAST(col_ret.id_retur||' ('||col_ret.motiv_retur||')' AS VARCHAR(500)) AS retur, MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||'-'||c.continut_colet,' \')) AS lista_colete
FROM col_ret
INNER JOIN colete c ON col_ret.id_colet=c.id_colet
START WITH nr_crt=1
CONNECT BY PRIOR col_ret.id_retur=col_ret.id_retur AND PRIOR nr_crt=nr_crt-1
GROUP BY col_ret.id_retur,col_ret.motiv_retur
ORDER BY id_retur

/* 60.Lista coletelor returnate în anul 2018 de către destinatarii care nu sunt din județul Mureș. Ce colete au fost returnate în anul 2018 de către destinatarii care nu sunt din județul Mureș */
WITH col_ret AS(
SELECT r.*,d_f.id_colet,ROW_NUMBER()OVER(PARTITION BY id_retur ORDER BY d_f.id_colet) AS nr_crt
FROM retururi r
INNER JOIN detalii_facturi d_f ON r.nr_factura=d_f.nr_factura
INNER JOIN facturi f ON d_f.nr_factura=f.nr_factura
INNER JOIN destinatari d ON f.id_destinatar=d.id_destinatar
INNER JOIN coduri_postale c_p ON d.cod_postal=c_p.cod_postal
WHERE EXTRACT (YEAR FROM data_retur)=2018 AND judet!='Mureș')
SELECT CAST(col_ret.id_retur||' ('||col_ret.motiv_retur||')' AS VARCHAR(500)) AS retur, MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||'-'||c.continut_colet,' \')) AS lista_colete
FROM col_ret
INNER JOIN colete c ON col_ret.id_colet=c.id_colet
START WITH nr_crt=1
CONNECT BY PRIOR col_ret.id_retur=col_ret.id_retur AND PRIOR nr_crt=nr_crt-1
GROUP BY col_ret.id_retur,col_ret.motiv_retur
ORDER BY id_retur

/* 61.Lista transporturilor din depozit în depozit efectuate anul 2018. Ce transporturi din depozit în depozit au fost efectuate în anul 2018? */
WITH tr_dep2 AS(
SELECT t.*,CAST('Depozitul '||t.id_depozit_1||' -> '||'Depozitul '||t.id_depozit_2 AS VARCHAR(500)) AS depozite,ROW_NUMBER()OVER(PARTITION BY id_depozit_1,id_depozit_2 ORDER BY id_transport) AS linie
FROM transporturi t
WHERE id_depozit_2 IS NOT NULL AND 
EXTRACT (YEAR FROM data_plecare)=2018)
SELECT tr_dep2.depozite, MAX(linie) AS nr_transporturi, MAX(SYS_CONNECT_BY_PATH(linie||': '||id_transport||' ('||data_plecare||')',' \')) AS Lista_transporturi
FROM tr_dep2
START WITH linie=1 
CONNECT BY PRIOR tr_dep2.depozite=tr_dep2.depozite AND PRIOR linie=linie-1
GROUP BY tr_dep2.depozite,id_depozit_1
ORDER BY id_depozit_1

/* 62.Lista transporturilor din depozit în depozit efectuate anul 2018 cu un vehicul de tip Iveco Daily. Ce transporturi din depozit în depozit au fost efectuate în anul 2018 cu un vehicul de tip Iveco Daily? */
WITH tr_dep2 AS(
SELECT t.*,CAST('Depozitul '||t.id_depozit_1||' -> '||'Depozitul '||t.id_depozit_2 AS VARCHAR(500)) AS depozite,ROW_NUMBER()OVER(PARTITION BY id_depozit_1,id_depozit_2 ORDER BY id_transport) AS linie
FROM transporturi t
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul
WHERE id_depozit_2 IS NOT NULL AND 
EXTRACT (YEAR FROM data_plecare)=2018 AND v.model='Iveco Daily' )
SELECT tr_dep2.depozite, MAX(linie) AS nr_transporturi, MAX(SYS_CONNECT_BY_PATH(linie||': '||id_transport||' ('||data_plecare||')',' \')) AS Lista_transporturi
FROM tr_dep2
START WITH linie=1 
CONNECT BY PRIOR tr_dep2.depozite=tr_dep2.depozite AND PRIOR linie=linie-1
GROUP BY tr_dep2.depozite,id_depozit_1
ORDER BY id_depozit_1

/* 63.Lista transporturilor din depozit în depozit efectuate anul 2018 cu un vehicul de tip Iveco Daily care au fost înmatriculate începând cu anul 2010. Ce transporturi din depozit în depozit au fost efectuate în anul 2018 cu un vehicul de tip Iveco Daily care au fost înmatriculate începând cu anul 2010? */
WITH tr_dep2 AS(
SELECT t.*,v.model,v.data_inmatriculare,CAST('Depozitul '||t.id_depozit_1||' -> '||'Depozitul '||t.id_depozit_2 AS VARCHAR(500)) AS depozite,ROW_NUMBER()OVER(PARTITION BY id_depozit_1,id_depozit_2 ORDER BY id_transport) AS linie
FROM transporturi t
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul
WHERE id_depozit_2 IS NOT NULL AND 
EXTRACT (YEAR FROM data_plecare)=2018 AND v.model='Iveco Daily' AND EXTRACT(YEAR FROM data_inmatriculare)>=2010 )
SELECT tr_dep2.depozite, MAX(linie) AS nr_transporturi, MAX(SYS_CONNECT_BY_PATH(linie||': '||id_transport||' ('||data_plecare||')',' \')) AS Lista_transporturi
FROM tr_dep2
START WITH linie=1 
CONNECT BY PRIOR tr_dep2.depozite=tr_dep2.depozite AND PRIOR linie=linie-1
GROUP BY tr_dep2.depozite,id_depozit_1
ORDER BY id_depozit_1

/* 64.Lista transporturilor din depozit în depozit efectuate anul 2018 cu un vehicul de tip Iveco Daily și care au fost conduse de șoferi din fiecare județ, cu excepția celor din Vaslui. Ce transporturi din depozit în depozit au fost efectuate în anul 2018 vehicul de tip Iveco Daily și care au fost conduse de șoferi din fiecare județ, cu excepția celor din Vaslui? */
WITH tr_dep2 AS(
SELECT t.*,v.model,v.data_inmatriculare,CAST('Depozitul '||t.id_depozit_1||' -> '||'Depozitul '||t.id_depozit_2 AS VARCHAR(500)) AS depozite,ROW_NUMBER()OVER(PARTITION BY id_depozit_1,id_depozit_2 ORDER BY id_transport) AS linie
FROM transporturi t
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul
INNER JOIN soferi s ON t.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
INNER JOIN coduri_postale c_p ON a.cod_postal=c_p.cod_postal
WHERE id_depozit_2 IS NOT NULL AND 
EXTRACT (YEAR FROM data_plecare)=2018 AND v.model='Iveco Daily' AND judet!='Vaslui' )
SELECT tr_dep2.depozite, MAX(linie) AS nr_transporturi, MAX(SYS_CONNECT_BY_PATH(linie||': '||id_transport||' ('||data_plecare||')',' \')) AS Lista_transporturi
FROM tr_dep2
START WITH linie=1 
CONNECT BY PRIOR tr_dep2.depozite=tr_dep2.depozite AND PRIOR linie=linie-1
GROUP BY tr_dep2.depozite,id_depozit_1
ORDER BY id_depozit_1

/* 65.Lista tuturor coletelor expediată de fiecare persoană jurdică(firmă) din județul Suceava în anul 2018. Ce colete a expediat fiecare persoană juridică(firmă) din județul Suceava în anul 2018? */
WITH e_jur AS(
SELECT c.*, e.prenume,ROW_NUMBER()OVER(PARTITION BY c.id_expeditor ORDER BY id_colet) AS nr_crt
FROM colete c
INNER JOIN expeditori e ON c.id_expeditor=e.id_expeditor
INNER JOIN coduri_postale c_p ON e.cod_postal=c_p.cod_postal
WHERE e.prenume IS NULL AND c_p.judet='Suceava' AND
EXTRACT (YEAR FROM data_expediere)=2018)
SELECT e_jur.id_expeditor, CAST(nume AS VARCHAR(500)) AS expeditor, MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(id_colet||': '||continut_colet,' \')) AS lista_colete
FROM e_jur
INNER JOIN expeditori e ON e_jur.id_expeditor=e.id_expeditor
START WITH nr_crt=1
CONNECT BY PRIOR e_jur.id_expeditor=e_jur.id_expeditor AND PRIOR nr_crt=nr_crt-1
GROUP BY e_jur.id_expeditor, nume
ORDER BY 1

/* 66.Lista tuturor coletelor expediată de fiecare persoană jurdică(firmă) în anul 2018. Ce colete a expediat fiecare persoană juridică(firmă) în anul 2018? */
WITH e_jur AS(
SELECT c.*, e.prenume,ROW_NUMBER()OVER(PARTITION BY c.id_expeditor ORDER BY id_colet) AS nr_crt
FROM colete c
INNER JOIN expeditori e ON c.id_expeditor=e.id_expeditor
WHERE e.prenume IS NULL AND
EXTRACT (YEAR FROM data_expediere)=2018)
SELECT e_jur.id_expeditor, CAST(nume AS VARCHAR(500)) AS expeditor, MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(id_colet||': '||continut_colet,' \')) AS lista_colete
FROM e_jur
INNER JOIN expeditori e ON e_jur.id_expeditor=e.id_expeditor
START WITH nr_crt=1
CONNECT BY PRIOR e_jur.id_expeditor=e_jur.id_expeditor AND PRIOR nr_crt=nr_crt-1
GROUP BY e_jur.id_expeditor, nume
ORDER BY 1

/* 67.Lista tuturor coletelor expediată de fiecare persoană jurdică(firmă) în perioada xx.01-xx.04. Ce colete a expediat fiecare persoană juridică(firmă) în perioada xx.01-xx.04.2018? */
WITH e_jur AS(
SELECT c.*, e.prenume,ROW_NUMBER()OVER(PARTITION BY c.id_expeditor ORDER BY id_colet) AS nr_crt
FROM colete c
INNER JOIN expeditori e ON c.id_expeditor=e.id_expeditor
WHERE e.prenume IS NULL AND
EXTRACT (YEAR FROM data_expediere)=2018 AND 
EXTRACT (MONTH FROM data_expediere) BETWEEN 1 AND 4)
SELECT e_jur.id_expeditor, CAST(nume AS VARCHAR(500)) AS expeditor, MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(id_colet||': '||continut_colet,' \')) AS lista_colete
FROM e_jur
INNER JOIN expeditori e ON e_jur.id_expeditor=e.id_expeditor
START WITH nr_crt=1
CONNECT BY PRIOR e_jur.id_expeditor=e_jur.id_expeditor AND PRIOR nr_crt=nr_crt-1
GROUP BY e_jur.id_expeditor, nume
ORDER BY 1

/* 68.Lista tututor incidentelor raportate de fiecare șofer în parte în anul 2018. Ce incidente a raportat fiecare șofer în parte în anul 2018? */
WITH i_s AS(
SELECT i.*, ROW_NUMBER()OVER(PARTITION BY id_sofer ORDER BY id_incident) AS nr_crt
FROM incidente i
WHERE EXTRACT (YEAR FROM data_raportarii)=2018)
SELECT i_s.id_sofer, CAST(a.nume||' '||a.prenume AS VARCHAR(500)) AS sofer, MAX(nr_crt) AS numar_incidente, MAX(SYS_CONNECT_BY_PATH(nr_crt||': '||tip_incident||'('||data_raportarii||')',' \')) AS lista_incidente
FROM i_s
INNER JOIN soferi s ON i_s.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
START WITH nr_crt=1
CONNECT BY PRIOR i_s.id_sofer=i_s.id_sofer AND PRIOR nr_crt=nr_crt-1
GROUP BY i_s.id_sofer, nume, prenume
ORDER BY 1

/* 69.Lista tututor incidentelor raportate de fiecare șofer din județul Iași în anul 2018. Ce incidente a raportat fiecare șofer din județul Iași în anul 2018? */
WITH i_s AS(
SELECT i.*, ROW_NUMBER()OVER(PARTITION BY i.id_sofer ORDER BY id_incident) AS nr_crt
FROM incidente i
INNER JOIN soferi s ON i.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
INNER JOIN coduri_postale c_p ON a.cod_postal=c_p.cod_postal
WHERE EXTRACT (YEAR FROM data_raportarii)=2018 AND c_p.judet='Iași')
SELECT i_s.id_sofer, CAST(a.nume||' '||a.prenume AS VARCHAR(500)) AS sofer, MAX(nr_crt) AS numar_incidente, MAX(SYS_CONNECT_BY_PATH(nr_crt||': '||tip_incident||'('||data_raportarii||')',' \')) AS lista_incidente
FROM i_s
INNER JOIN soferi s ON i_s.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
START WITH nr_crt=1
CONNECT BY PRIOR i_s.id_sofer=i_s.id_sofer AND PRIOR nr_crt=nr_crt-1
GROUP BY i_s.id_sofer, nume, prenume
ORDER BY 1

/* 70.Lista coletelor primite de către fiecare persoană juridică(firmă) în perioada xx.02-xx.06.2018. Ce colete a primit fiecare persoană juridică(firmă) în perioadă xx.02-xx.06.2018? */
WITH d_jur AS(
SELECT c.*, ROW_NUMBER()OVER(PARTITION BY c.id_destinatar ORDER BY id_colet) AS nr_crt
FROM colete c
INNER JOIN destinatari d ON c.id_destinatar=d.id_destinatar
INNER JOIN coduri_postale c_p ON d.cod_postal=c_p.cod_postal
WHERE d.prenume IS NULL AND 
EXTRACT (YEAR FROM data_expediere)=2018 AND 
EXTRACT (MONTH FROM data_expediere) BETWEEN 2 AND 6)
SELECT d_jur.id_destinatar, CAST(nume AS VARCHAR(500)) AS destinatar, MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(id_colet||': '||continut_colet,' \')) AS lista_colete
FROM d_jur
INNER JOIN destinatari d ON d_jur.id_destinatar=d.id_destinatar
START WITH nr_crt=1
CONNECT BY PRIOR d_jur.id_destinatar=d_jur.id_destinatar AND PRIOR nr_crt=nr_crt-1
GROUP BY d_jur.id_destinatar, nume
ORDER BY 1

/* 71.Lista coletelor primite de către fiecare persoană juridică(firmă) în anul 2018. Ce colete a primit fiecare persoană juridică(firmă) în anul 2018?*/
WITH d_jur AS(
SELECT c.*, ROW_NUMBER()OVER(PARTITION BY c.id_destinatar ORDER BY id_colet) AS nr_crt
FROM colete c
INNER JOIN destinatari d ON c.id_destinatar=d.id_destinatar
INNER JOIN coduri_postale c_p ON d.cod_postal=c_p.cod_postal
WHERE d.prenume IS NULL AND 
EXTRACT (YEAR FROM data_expediere)=2018)
SELECT d_jur.id_destinatar, CAST(nume AS VARCHAR(500)) AS destinatar, MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(id_colet||': '||continut_colet,' \')) AS lista_colete
FROM d_jur
INNER JOIN destinatari d ON d_jur.id_destinatar=d.id_destinatar
START WITH nr_crt=1
CONNECT BY PRIOR d_jur.id_destinatar=d_jur.id_destinatar AND PRIOR nr_crt=nr_crt-1
GROUP BY d_jur.id_destinatar, nume
ORDER BY 1

/* 72.Lista coletelor primite de către fiecare persoană juridică(firmă) în anul 2018 ce au fost expediate din fiecare județ, cu excepția municipiului București. Ce colete a primit fiecare persoană juridică(firmă) în anul 2018 expediate din fiecare județ, cu excepția municipiului București?*/
WITH d_jur AS(
SELECT c.*, ROW_NUMBER()OVER(PARTITION BY c.id_destinatar ORDER BY id_colet) AS nr_crt
FROM colete c
INNER JOIN destinatari d ON c.id_destinatar=d.id_destinatar
INNER JOIN expeditori e ON c.id_expeditor=e.id_expeditor
INNER JOIN coduri_postale c_p ON e.cod_postal=c_p.cod_postal
WHERE d.prenume IS NULL AND 
EXTRACT (YEAR FROM data_expediere)=2018 AND c_p.judet!='București')
SELECT d_jur.id_destinatar, CAST(nume AS VARCHAR(500)) AS destinatar, MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(id_colet||': '||continut_colet,' \')) AS lista_colete
FROM d_jur
INNER JOIN destinatari d ON d_jur.id_destinatar=d.id_destinatar
START WITH nr_crt=1
CONNECT BY PRIOR d_jur.id_destinatar=d_jur.id_destinatar AND PRIOR nr_crt=nr_crt-1
GROUP BY d_jur.id_destinatar, nume
ORDER BY 1

/* 73.Lista coletelor livrate de către fiecare șofer ce a depășit pragul de 25.000km în luna Iulie. Ce colete au fost livtrate de către fiecare șofer în luna Iulie?*/
WITH col_sof AS(
SELECT t.id_sofer, d_t.id_colet,ROW_NUMBER()OVER(PARTITION BY t.id_sofer ORDER BY id_colet) AS nr_crt
FROM transporturi t
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport
INNER JOIN soferi s ON t.id_sofer=s.id_sofer
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND
EXTRACT (MONTH FROM data_plecare)=7 AND km_parcursi>25000)
SELECT col_sof.id_sofer,CAST(a.nume||' '||a.prenume AS VARCHAR(500)) AS sofer, MAX(nr_crt) AS nr_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||': '||c.continut_colet,' \')) AS lista_colete
FROM col_sof
INNER JOIN colete c ON col_sof.id_colet=c.id_colet
INNER JOIN soferi s ON col_sof.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
START WITH nr_crt=1
CONNECT BY PRIOR col_sof.id_sofer=col_sof.id_sofer AND PRIOR nr_crt=nr_crt-1
GROUP BY col_sof.id_sofer,a.nume,a.prenume
ORDER BY 1

/* 74.Lista coletelor livrate de către fiecare șofer în luna Iulie cu vehicule ce au fost înmatriculate începând cu anul 2012. Ce colete au fost livtrate de către fiecare șofer în luna Iulie cu vehicule ce au fost înmatriculate începând cu anul 2012?*/
WITH col_sof AS(
SELECT t.id_sofer, d_t.id_colet,ROW_NUMBER()OVER(PARTITION BY t.id_sofer ORDER BY id_colet) AS nr_crt
FROM transporturi t
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul 
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND
EXTRACT (MONTH FROM data_plecare)=7 AND EXTRACT(YEAR FROM data_inmatriculare)>=2012)
SELECT col_sof.id_sofer,CAST(a.nume||' '||a.prenume AS VARCHAR(500)) AS sofer, MAX(nr_crt) AS nr_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||': '||c.continut_colet,' \')) AS lista_colete
FROM col_sof
INNER JOIN colete c ON col_sof.id_colet=c.id_colet
INNER JOIN soferi s ON col_sof.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
START WITH nr_crt=1
CONNECT BY PRIOR col_sof.id_sofer=col_sof.id_sofer AND PRIOR nr_crt=nr_crt-1
GROUP BY col_sof.id_sofer,a.nume,a.prenume
ORDER BY 1

/* 75.Lista coletelor livrate de către fiecare șofer din afara județul Iași în luna Iulie cu vehicule ce au fost înmatriculate începând cu anul 2012. Ce colete au fost livtrate de către fiecare șofer din afara județul Iași în luna Iulie cu vehicule ce au fost înmatriculate începând cu anul 2012?*/
WITH col_sof AS(
SELECT t.id_sofer, d_t.id_colet,ROW_NUMBER()OVER(PARTITION BY t.id_sofer ORDER BY id_colet) AS nr_crt
FROM transporturi t 
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport 
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul 
INNER JOIN soferi s ON t.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
INNER JOIN coduri_postale c_p ON a.cod_postal=c_p.cod_postal
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND
EXTRACT (MONTH FROM data_plecare)=7 AND EXTRACT(YEAR FROM data_inmatriculare)>=2012 AND judet!='Iași')
SELECT col_sof.id_sofer,CAST(a.nume||' '||a.prenume AS VARCHAR(500)) AS sofer, MAX(nr_crt) AS nr_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||': '||c.continut_colet,' \')) AS lista_colete
FROM col_sof
INNER JOIN colete c ON col_sof.id_colet=c.id_colet
INNER JOIN soferi s ON col_sof.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
START WITH nr_crt=1
CONNECT BY PRIOR col_sof.id_sofer=col_sof.id_sofer AND PRIOR nr_crt=nr_crt-1
GROUP BY col_sof.id_sofer,a.nume,a.prenume
ORDER BY 1

/* 76.Lista facturilor primite de către destinatarii cu un id<=1.000.000 în anul 2018. Ce facturi au primit destinatarii cu un id<=1.000.000 în anul 2018? */
WITH f_d AS(
SELECT f.*, ROW_NUMBER()OVER(PARTITION BY id_destinatar ORDER BY nr_factura) AS nr_crt
FROM facturi f
WHERE EXTRACT (YEAR FROM data)=2018 AND id_destinatar<=100000)
SELECT f_d.id_destinatar, CAST(nume||' '||prenume AS VARCHAR(500))AS destinatar, MAX(nr_crt) AS nr_facturi, MAX(SYS_CONNECT_BY_PATH(nr_factura||'('||data||')',' \')) AS lista_facturi
FROM f_d
INNER JOIN destinatari d ON f_d.id_destinatar=d.id_destinatar
START WITH nr_crt=1
CONNECT BY PRIOR f_d.id_destinatar=f_d.id_destinatar AND PRIOR nr_crt=nr_crt-1
GROUP BY f_d.id_destinatar,nume,prenume
ORDER BY 1

/* 77.Lista facturilor primite de către destinatarii cu un id<=1.000.000 care nu sunt din județul Prahova în anul 2018. Ce facturi au primit destinatarii cu un id<=100.000.0 care nu sunt din județul Prahova în anul 2018? */
WITH f_d AS(
SELECT f.*, ROW_NUMBER()OVER(PARTITION BY f.id_destinatar ORDER BY nr_factura) AS nr_crt
FROM facturi f
INNER JOIN destinatari d ON f.id_destinatar=d.id_destinatar
INNER JOIN coduri_postale c_p ON d.cod_postal=c_p.cod_postal
WHERE EXTRACT (YEAR FROM data)=2018 AND f.id_destinatar<=100000 AND judet!='Prahova')
SELECT f_d.id_destinatar, CAST(nume||' '||prenume AS VARCHAR(500))AS destinatar, MAX(nr_crt) AS nr_facturi, MAX(SYS_CONNECT_BY_PATH(nr_factura||'('||data||')',' \')) AS lista_facturi
FROM f_d
INNER JOIN destinatari d ON f_d.id_destinatar=d.id_destinatar
START WITH nr_crt=1
CONNECT BY PRIOR f_d.id_destinatar=f_d.id_destinatar AND PRIOR nr_crt=nr_crt-1
GROUP BY f_d.id_destinatar,nume,prenume
ORDER BY 1

/* 78.Lista coletelor transportate cu fiecare vehicul în luna Mai 2018. Ce colete au fost transportate prin intermediul fiecărui vehicul în luna Mai 2018? */
WITH col_veh AS(
SELECT t.*, d_t.id_colet,ROW_NUMBER()OVER(PARTITION BY id_vehicul ORDER BY id_colet) AS nr_crt
FROM transporturi t
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND
EXTRACT (MONTH FROM data_plecare)=5)
SELECT col_veh.id_vehicul,CAST(v.model||' ('||v.nr_inmatriculare||')' AS VARCHAR(500)) AS model, MAX(nr_crt) AS nr_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||': '||c.continut_colet,' \')) AS lista_colete
FROM col_veh
INNER JOIN colete c ON col_veh.id_colet=c.id_colet
INNER JOIN vehicule v ON col_veh.id_vehicul=v.id_vehicul
START WITH nr_crt=1
CONNECT BY PRIOR col_veh.id_vehicul=col_veh.id_vehicul AND PRIOR nr_crt=nr_crt-1
GROUP BY col_veh.id_vehicul,model,nr_inmatriculare
ORDER BY 1

/* 79.Lista coletelor transportate în anul 2018 cu vehicule conduse de către șoferi ce au depășit pragul de 25000 de km. Ce colete au fost transportate în anul 2018 cu vehicule conduse de către șoferi ce au depășit pragul de 25000 de km */
WITH col_veh AS(
SELECT t.*, d_t.id_colet,ROW_NUMBER()OVER(PARTITION BY id_vehicul ORDER BY id_colet) AS nr_crt
FROM transporturi t
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport
INNER JOIN soferi s ON t.id_sofer=s.id_sofer
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND km_parcursi>25000)
SELECT col_veh.id_vehicul,CAST(v.model||' ('||v.nr_inmatriculare||')' AS VARCHAR(500)) AS model, MAX(nr_crt) AS nr_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||': '||c.continut_colet,' \')) AS lista_colete
FROM col_veh
INNER JOIN colete c ON col_veh.id_colet=c.id_colet
INNER JOIN vehicule v ON col_veh.id_vehicul=v.id_vehicul
START WITH nr_crt=1
CONNECT BY PRIOR col_veh.id_vehicul=col_veh.id_vehicul AND PRIOR nr_crt=nr_crt-1
GROUP BY col_veh.id_vehicul,model,nr_inmatriculare
ORDER BY 1

/* 80.Lista coletelor transportate în anul 2018 cu vehicule conduse de către șoferi ce au depășit pragul de 25000 de km și care nu sunt din județul Iași. Ce colete au fost transportate în anul 2018 cu vehicule conduse de către șoferi ce au depășit pragul de 25000 de km și care nu sunt din județul Iași?*/
WITH col_veh AS(
SELECT t.*, d_t.id_colet,ROW_NUMBER()OVER(PARTITION BY id_vehicul ORDER BY id_colet) AS nr_crt
FROM transporturi t
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport 
INNER JOIN soferi s ON t.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
INNER JOIN coduri_postale c_p ON a.cod_postal=c_p.cod_postal
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND km_parcursi>25000 AND judet!='Iași')
SELECT col_veh.id_vehicul,CAST(v.model||' ('||v.nr_inmatriculare||')' AS VARCHAR(500)) AS model, MAX(nr_crt) AS nr_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||': '||c.continut_colet,' \')) AS lista_colete
FROM col_veh
INNER JOIN colete c ON col_veh.id_colet=c.id_colet
INNER JOIN vehicule v ON col_veh.id_vehicul=v.id_vehicul
START WITH nr_crt=1
CONNECT BY PRIOR col_veh.id_vehicul=col_veh.id_vehicul AND PRIOR nr_crt=nr_crt-1
GROUP BY col_veh.id_vehicul,model,nr_inmatriculare
ORDER BY 1

/* 81.Lista coletelor transportate în anul 2018 din primele 3 depozite cu vehicule conduse de către șoferi ce au depășit pragul de 25000 de km și care nu sunt din județul Iași. Ce colete au fost transportate în anul 2018 din primele 3 depozite ecu vehicule conduse de către șoferi ce au depășit pragul de 25000 de km și care nu sunt din județul Iași?*/
WITH col_veh AS(
SELECT t.*, d_t.id_colet,ROW_NUMBER()OVER(PARTITION BY id_vehicul ORDER BY id_colet) AS nr_crt
FROM transporturi t
INNER JOIN detalii_transport d_t ON t.id_transport=d_t.id_transport 
INNER JOIN soferi s ON t.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
INNER JOIN coduri_postale c_p ON a.cod_postal=c_p.cod_postal
WHERE EXTRACT (YEAR FROM data_plecare)=2018 AND km_parcursi>25000 AND judet!='Iași' AND id_depozit_1 BETWEEN 1 AND 3)
SELECT col_veh.id_vehicul,CAST(v.model||' ('||v.nr_inmatriculare||')' AS VARCHAR(500)) AS model, MAX(nr_crt) AS nr_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||': '||c.continut_colet,' \')) AS lista_colete
FROM col_veh
INNER JOIN colete c ON col_veh.id_colet=c.id_colet
INNER JOIN vehicule v ON col_veh.id_vehicul=v.id_vehicul
START WITH nr_crt=1
CONNECT BY PRIOR col_veh.id_vehicul=col_veh.id_vehicul AND PRIOR nr_crt=nr_crt-1
GROUP BY col_veh.id_vehicul,model,nr_inmatriculare
ORDER BY 1

/* 82.Lista coletelor primite de către destinatarii din fiecare județ în luna luna Aprilie 2018. Ce colete au fost livrate către fiecare județ în luna luna Aprilie 2018? */
WITH col_djud AS(
SELECT cp.*, c.id_colet,ROW_NUMBER()OVER(PARTITION BY cp.judet ORDER BY id_colet) AS nr_crt 
FROM coduri_postale cp
INNER JOIN destinatari d ON cp.cod_postal=d.cod_postal
INNER JOIN colete c ON d.id_destinatar=c.id_destinatar
WHERE EXTRACT (YEAR FROM data_primire_colet)=2018 AND
EXTRACT (MONTH FROM data_primire_colet)=4)
SELECT col_djud.judet, MAX(nr_crt) AS nr_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||': '||c.continut_colet,' \')) AS lista_colete
FROM col_djud
INNER JOIN colete c ON col_djud.id_colet=c.id_colet
START WITH nr_crt=1
CONNECT BY PRIOR col_djud.judet=col_djud.judet AND PRIOR nr_crt=nr_crt-1
GROUP BY judet
ORDER BY judet

/* 83.Lista coletelor cu o valoare mai mare de 1000 de lei și o greutate minimă de 5 kg primite de către destinatarii din fiecare județ în luna luna Aprilie 2018. Ce colete au fost livrate către fiecare județ în luna luna Aprilie 2018? */
WITH col_djud AS(
SELECT cp.*, c.id_colet,ROW_NUMBER()OVER(PARTITION BY cp.judet ORDER BY id_colet) AS nr_crt 
FROM coduri_postale cp
INNER JOIN destinatari d ON cp.cod_postal=d.cod_postal
INNER JOIN colete c ON d.id_destinatar=c.id_destinatar
WHERE EXTRACT (YEAR FROM data_primire_colet)=2018 AND
EXTRACT (MONTH FROM data_primire_colet)=4 AND valoare>1000 AND greutate>=5)
SELECT col_djud.judet, MAX(nr_crt) AS nr_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||': '||c.continut_colet,' \')) AS lista_colete
FROM col_djud
INNER JOIN colete c ON col_djud.id_colet=c.id_colet
START WITH nr_crt=1
CONNECT BY PRIOR col_djud.judet=col_djud.judet AND PRIOR nr_crt=nr_crt-1
GROUP BY judet
ORDER BY judet

/* 84.Lista coletelor trimise de expeditorii din fiecare județ în luna August. Ce colete au fost trimise(expediate) din fiecare județ în luna August ?*/
WITH col_ejud AS(
SELECT cp.*, c.id_colet,ROW_NUMBER()OVER(PARTITION BY cp.judet ORDER BY id_colet) AS nr_crt 
FROM coduri_postale cp
INNER JOIN expeditori e ON cp.cod_postal=e.cod_postal
INNER JOIN colete c ON e.id_expeditor=c.id_expeditor
WHERE EXTRACT (YEAR FROM data_expediere)=2018 AND
EXTRACT (MONTH FROM data_expediere)=8)
SELECT col_ejud.judet, MAX(nr_crt) AS nr_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||': '||c.continut_colet,' \')) AS lista_colete
FROM col_ejud
INNER JOIN colete c ON col_ejud.id_colet=c.id_colet
START WITH nr_crt=1
CONNECT BY PRIOR col_ejud.judet=col_ejud.judet AND PRIOR nr_crt=nr_crt-1
GROUP BY judet
ORDER BY judet

/* 85.Lista coletelor cu o greutate mai mică de 5 kg și o valoare mai mică de 500 de lei trimise de expeditorii din fiecare județ în luna August. Ce colete au fost trimise(expediate) din fiecare județ în luna August ?*/
WITH col_ejud AS(
SELECT cp.*, c.id_colet,ROW_NUMBER()OVER(PARTITION BY cp.judet ORDER BY id_colet) AS nr_crt 
FROM coduri_postale cp
INNER JOIN expeditori e ON cp.cod_postal=e.cod_postal
INNER JOIN colete c ON e.id_expeditor=c.id_expeditor
WHERE EXTRACT (YEAR FROM data_expediere)=2018 AND
EXTRACT (MONTH FROM data_expediere)=8 AND valoare<500 AND greutate<5)
SELECT col_ejud.judet, MAX(nr_crt) AS nr_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||': '||c.continut_colet,' \')) AS lista_colete
FROM col_ejud
INNER JOIN colete c ON col_ejud.id_colet=c.id_colet
START WITH nr_crt=1
CONNECT BY PRIOR col_ejud.judet=col_ejud.judet AND PRIOR nr_crt=nr_crt-1
GROUP BY judet
ORDER BY judet

/* 86.Lista coletelor pentru transporturile efectuate în anul 2018 cu un vehicul, model Ford Transit, ce au fost conduse de către șoferi ce au depășit pragul de 5000 de km, afișându-se și tracking number-ul fiecărui colet. Ce colete au fost transportate în anul 2018, cu un vehicul, model Ford Transit, ce au fost conduse de către șoferi ce au depășit pragul de 5000 de km, afișându-se și tracking number-ul fiecărui colet ? */
WITH i_t AS (
SELECT d_t.*,t.id_vehicul,t.id_sofer,t.data_plecare,ROW_NUMBER()OVER(PARTITION BY d_t.id_transport ORDER BY d_t.id_colet) AS nr_crt
FROM detalii_transport d_t 
INNER JOIN transporturi t ON d_t.id_transport=t.id_transport
INNER JOIN vehicule v ON t.id_vehicul=v.id_vehicul
INNER JOIN soferi s ON t.id_sofer=s.id_sofer 
WHERE model='Ford Transit' AND km_parcursi>=5000 AND EXTRACT(YEAR FROM data_plecare)=2018)
SELECT i_t.id_transport,CAST('Depozitul '||t.id_depozit_1||' ('||d.adresa||')' AS VARCHAR(500)) AS depozit, CAST(a.nume||' '||a.prenume AS VARCHAR(500)) AS sofer,MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||'-'||c.continut_colet||'('||u_c.trackingnumber||')',' \')) AS lista_colete
FROM i_t
INNER JOIN colete c ON i_t.id_colet=c.id_colet
INNER JOIN soferi s ON i_t.id_sofer=s.id_sofer
INNER JOIN angajati a ON s.id_angajat=a.id_angajat
INNER JOIN transporturi t ON i_t.id_transport=t.id_transport
INNER JOIN depozite d ON t.id_depozit_1=d.id_depozit
INNER JOIN urmarire_colete u_c ON i_t.id_colet=u_c.id_colet
START WITH nr_crt=1
CONNECT BY PRIOR i_t.id_transport=i_t.id_transport AND PRIOR nr_crt=nr_crt-1
GROUP BY i_t.id_transport,t.id_depozit_1,d.adresa,a.nume,a.prenume
ORDER BY 1

/* 87.Lista coletelor organizată pe facturi întocmite în anul 2018 de către primii 90.000 de expeditori, afisandu-se si tracking number-ul. Ce colete conține fiecare factură întocmită în anul 2018 de către primii 90.000 de expeditori, afisandu-se si tracking number-ul?*/
WITH l_f AS(
SELECT d_f.*,f.data,ROW_NUMBER()OVER(PARTITION BY d_f.nr_factura ORDER BY d_f.id_colet) AS nr_crt
FROM detalii_facturi d_f
INNER JOIN facturi f ON d_f.nr_factura=f.nr_factura
WHERE EXTRACT (YEAR FROM data)=2018 AND f.id_expeditor BETWEEN 1 AND 90000)
SELECT nr_factura,MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||'-'||c.continut_colet||'('||u_c.trackingnumber||')',' \')) AS lista_colete
FROM l_f
INNER JOIN colete c ON l_f.id_colet=c.id_colet
INNER JOIN urmarire_colete u_c ON l_f.id_colet=u_c.id_colet
START WITH nr_crt=1
CONNECT BY PRIOR l_f.nr_factura=l_f.nr_factura AND PRIOR nr_crt=nr_crt-1
GROUP BY nr_factura
ORDER BY nr_factura

/* 88.Lista coletelor pentru primele 2000 de transporturi, afisandu-se si tracking number-ul fiecaruia. Ce colete conțin primele 2000 de transporturi? */
WITH i_t AS (
SELECT d_t.*,t.data_plecare,ROW_NUMBER()OVER(PARTITION BY d_t.id_transport ORDER BY d_t.id_colet) AS nr_crt
FROM detalii_transport d_t 
INNER JOIN transporturi t ON d_t.id_transport=t.id_transport
WHERE d_t.id_transport<=2000)
SELECT i_t.id_transport,MAX(nr_crt) AS numar_colete, MAX(SYS_CONNECT_BY_PATH(nr_crt||'-'||c.continut_colet||'('||u_c.trackingnumber||')',' \')) AS lista_colete
FROM i_t
INNER JOIN colete c ON i_t.id_colet=c.id_colet
INNER JOIN urmarire_colete u_c ON i_t.id_colet=u_c.id_colet
START WITH nr_crt=1
CONNECT BY PRIOR i_t.id_transport=i_t.id_transport AND PRIOR nr_crt=nr_crt-1
GROUP BY i_t.id_transport
ORDER BY 1


























