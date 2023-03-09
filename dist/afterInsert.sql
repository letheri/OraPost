-- 2022-12-29  -  Saysis yapısı örnek alındı..

-- Canlı servis için ham veri kaydı
truncate table mis_eskom restart identity;
INSERT INTO public.mis_eskom
( index,bynmid, beyanturu, beyan, mahalle, sokak, sokakturu, tapumahalle, pafta, "ada", parsel, diskapi, ickapino, adresno, sicilno, kimlikno, ad, soyad, gelirturu, gecikenborc, toplamborc, matrah, muafiyet_tutari, tahakkuk_matrahi)
SELECT index, bynmid, beyanturu, beyan, mahalle, sokak, sokakturu, tapumahalle, pafta, "ada", parsel, diskapi, ickapino, adresno, sicilno, kimlikno, ad, soyad, gelirturu, gecikenborc, toplamborc, matrah, muafiyet_tutari, tahakkuk_matrahi
FROM public.mis_eskom_beyan;

update mis_eskom set ham_mahalle = mahalle;
update mis_eskom set ham_ada = ada;

-- Ham Veride Ön İşlem
update mis_eskom set mahalle = 'ALINCA MÜCAVİR' where mahalle = 'ALINCA';
update mis_eskom set mahalle = 'BARÇA MÜCAVİR' where mahalle = 'BARÇA';
update mis_eskom set mahalle = 'FEVZİÇAKMAK' where mahalle = 'FEVZİ ÇAKMAK';
update mis_eskom set mahalle = 'GÜNEYKÖY MÜCAVİR' where mahalle in ('GÜNEYKÖY','GUNEYKOY');
update mis_eskom set mahalle = 'HACIHÜSEYİN' where mahalle in ('HACI HÜSEYİN');
update mis_eskom set mahalle = 'SULTANSELİM' where mahalle in ('SULTAN SELİM');
update mis_eskom set mahalle = 'UZGUR MÜCAVİR' where mahalle in ('UZGUR','UZGUR KÖYÜ');
update mis_eskom set mahalle = 'TAYYAREDÜZÜ' where mahalle in ('TEYYAREDÜZÜ');

update mis_eskom set ada = substring(ada,'\d+');
update mis_eskom set ada = 0 where ada is null or ada = '';


with eslesenler as (
select me."index", p.objectid  from geomahalle gm 
inner join mis_eskom me  on trim(gm.adi_numarasi)  = trim(me.mahalle) 
inner join parsel p on p.idari_mah_id = gm.objectid and trim(me."ada") = trim(p.ada) and trim(me.parsel)=trim(p.parsel) 
where p.geo_durum and gm.geo_durum) 
update mis_eskom me set parsel_id = e.objectid from eslesenler e 
where parsel_id is null and e.index = me.index and e.index is not null and beyanturu in ('Bina', 'Arsa', 'Arazi');

-- Beyanlı Parseller
truncate mis_beyanli_parseller restart identity;
INSERT INTO public.mis_beyanli_parseller 
(parsel_id,  beyan_kayitli_ada, beyan_kayitli_parsel, adaparsel, paket_beyan_turu_aciklamasi, beyan_tipi_aciklamasi, beyan_adet, m_date, m_log_user, m_status) 
select distinct
parsel_id, ada, parsel, ada||'/'||parsel, 'Emlak' ,substring(array_agg(distinct beyanturu)::varchar(255) from 2 for length(array_agg(distinct beyanturu)::varchar(255))-2), count(bynmid), now()::date, 'SERVIS', 'INSERT' 
from mis_eskom where parsel_id is not null 
group by parsel_id, ada, parsel;

update mis_beyanli_parseller set tapu_mah_adi = parsel.tapu_mah_adi, idari_mah_adi= parsel.idari_mah_adi, poly = parsel.poly  from parsel 
where parsel.objectid = mis_beyanli_parseller.parsel_id and geometrytype(parsel.poly)='POLYGON';

update mis_beyanli_parseller set idari_mah_uavt_kod = geomahalle.uavt_kod from parsel 
left join geomahalle on geomahalle.objectid = parsel.idari_mah_id 
where parsel.objectid = mis_beyanli_parseller.parsel_id;

INSERT INTO public.mis_entegrasyon_history 
(m_date, adet, kapsam) 
select now()::date, count(*), 'Beyanlı Parseller' from mis_beyanli_parseller;

--- Borçlu Parseller
truncate table mis_borclu_parseller;
INSERT INTO public.mis_borclu_parseller 
(parsel_id, beyan_kayitli_ada, beyan_kayitli_parsel, adaparsel, toplam_borclu_adedi, toplam_borc_adedi, beyan_borc_tutari_toplam, m_date, m_log_user, m_status) 
select distinct 
parsel_id, ada, parsel, ada||'/'||parsel, count(bynmid),count(bynmid),sum(toplamborc),  now()::date, 'SERVIS', 'INSERT' 
from mis_eskom where parsel_id is not null and toplamborc is not null and toplamborc > 0 
group by parsel_id, ada, parsel;

update mis_borclu_parseller set tapu_mah_adi = parsel.tapu_mah_adi, idari_mah_adi= parsel.idari_mah_adi, poly = parsel.poly  from parsel 
where parsel.objectid = mis_borclu_parseller.parsel_id and geometrytype(parsel.poly)='POLYGON';

update mis_borclu_parseller set idari_mah_uavt_kod = geomahalle.uavt_kod from parsel 
left join geomahalle on geomahalle.objectid = parsel.idari_mah_id 
where parsel.objectid = mis_borclu_parseller.parsel_id;

INSERT INTO public.mis_entegrasyon_history 
(m_date, adet, kapsam) 
select now()::date, count(*), 'Borçlu Parseller' from mis_borclu_parseller;


-- Sehven Beyanlar
truncate table mis_yanlis_beyanli_parseller restart identity;

INSERT INTO public.mis_yanlis_beyanli_parseller 
(parsel_id, tapu_mah_adi, idari_mah_uavt_kod, idari_mah_adi, ada, parsel, adaparsel, durum, paket_beyan_turu_aciklamasi,beyan_adedi ,beyan_tipi_aciklamasi, m_date, m_log_user, m_status, poly) 
SELECT distinct parsel_id, tapu_mah_adi, idari_mah_uavt_kod, idari_mah_adi, ada, parsel, adaparsel, durum, paket_beyan_turu_aciklamasi,beyan_adedi,beyan_tipi_aciklamasi , m_date, m_log_user, m_status, poly 
FROM public.query_mis_yanlis_beyanli_arazi where geometrytype(poly) ='POLYGON' ;

INSERT INTO public.mis_yanlis_beyanli_parseller 
(parsel_id, tapu_mah_adi, idari_mah_uavt_kod, idari_mah_adi, ada, parsel, adaparsel, durum, paket_beyan_turu_aciklamasi,beyan_adedi ,beyan_tipi_aciklamasi, m_date, m_log_user, m_status, poly) 
SELECT distinct parsel_id, tapu_mah_adi, idari_mah_uavt_kod, idari_mah_adi, ada, parsel, adaparsel, durum, paket_beyan_turu_aciklamasi,beyan_adedi,beyan_tipi_aciklamasi , m_date, m_log_user, m_status, st_geometryn(poly,1) 
FROM public.query_mis_yanlis_beyanli_arazi where geometrytype(poly) !='POLYGON' ;

INSERT INTO public.mis_yanlis_beyanli_parseller 
(parsel_id, tapu_mah_adi, idari_mah_uavt_kod, idari_mah_adi, ada, parsel, adaparsel, durum, paket_beyan_turu_aciklamasi,beyan_adedi,beyan_tipi_aciklamasi, m_date, m_log_user, m_status, poly) 
SELECT distinct parsel_id, tapu_mah_adi, idari_mah_uavt_kod, idari_mah_adi, ada, parsel, adaparsel, durum, paket_beyan_turu_aciklamasi,beyan_adedi ,beyan_tipi_aciklamasi,m_date, m_log_user, m_status, poly 
FROM public.query_mis_yanlis_beyanli_arsa where geometrytype(poly) ='POLYGON';

INSERT INTO public.mis_yanlis_beyanli_parseller 
(parsel_id, tapu_mah_adi, idari_mah_uavt_kod, idari_mah_adi, ada, parsel, adaparsel, durum, paket_beyan_turu_aciklamasi,beyan_adedi,beyan_tipi_aciklamasi, m_date, m_log_user, m_status, poly) 
SELECT distinct parsel_id, tapu_mah_adi, idari_mah_uavt_kod, idari_mah_adi, ada, parsel, adaparsel, durum, paket_beyan_turu_aciklamasi,beyan_adedi ,beyan_tipi_aciklamasi,m_date, m_log_user, m_status, st_geometryn(poly,1) 
FROM public.query_mis_yanlis_beyanli_arsa where geometrytype(poly) !='POLYGON';

INSERT INTO public.mis_yanlis_beyanli_parseller 
(parsel_id, tapu_mah_adi, idari_mah_uavt_kod, idari_mah_adi, ada, parsel, adaparsel, durum, paket_beyan_turu_aciklamasi,beyan_adedi ,beyan_tipi_aciklamasi,m_date, m_log_user, m_status, poly) 
SELECT distinct parsel_id, tapu_mah_adi, idari_mah_uavt_kod, idari_mah_adi, ada, parsel, adaparsel, durum, paket_beyan_turu_aciklamasi, beyan_adedi ,beyan_tipi_aciklamasi , m_date, m_log_user, m_status, poly 
FROM public.query_mis_yanlis_beyanli_bina where geometrytype(poly) ='POLYGON';

INSERT INTO public.mis_yanlis_beyanli_parseller 
(parsel_id, tapu_mah_adi, idari_mah_uavt_kod, idari_mah_adi, ada, parsel, adaparsel, durum, paket_beyan_turu_aciklamasi,beyan_adedi ,beyan_tipi_aciklamasi,m_date, m_log_user, m_status, poly) 
SELECT distinct parsel_id, tapu_mah_adi, idari_mah_uavt_kod, idari_mah_adi, ada, parsel, adaparsel, durum, paket_beyan_turu_aciklamasi, beyan_adedi ,beyan_tipi_aciklamasi , m_date, m_log_user, m_status, st_geometryn(poly,1) 
FROM public.query_mis_yanlis_beyanli_bina where geometrytype(poly) !='POLYGON';



truncate table mis_uavt_bbolum_fark restart identity;

INSERT INTO public.mis_uavt_bbolum_fark (parsel_id,"ada", parsel, adaparsel, idari_mahalle_adi, uavt_bbolum_sayisi, mis_bbolum_sayisi, eslesme_turu, poly, m_date, m_log_user, m_status) 
SELECT distinct parsel_id, "ada", parsel, adaparsel, idari_mahalle_adi, uavt_bbolum_sayisi, mis_bbolum_sayisi, eslesme_turu, poly,now()::date,'Netcad-Admin','INSERT' FROM public.query_mis_uavt_bbolum_fark;

-- ## GIS | Eşleşmeyen Parseller ##
-- Beyan Sorgula
TRUNCATE TABLE mis_beyanli_parsel_eslesmeyen restart identity;
insert
	into
	public.mis_beyanli_parsel_eslesmeyen(           
	idari_mah_adi,
	ada,
	parsel,
	adaparsel,
	bina,
	arsa,
	arazi,
	ctv,
	top_borc,
	beyan_sayisi,
	beyan_turu,
	m_date,
	m_log_user,
	m_status)          
 select distinct
	mis.idari_mah_adi,
	mis.ada,
	mis.parsel,
	(mis.ada || '/' || mis.parsel) as adaparsel,
	sum(coalesce(mis.binabildirimisayisi, 0)) as bina,
	sum(coalesce(mis.arsabildirimisayisi, 0)) as arsa,
	sum(coalesce(mis.arazibildirimisayisi, 0)) as arazi,
	0 as ctv,
	sum(coalesce(mis.toplam_borc , 0)) as toplam_borc,
	sum(coalesce(mis.binabildirimisayisi, 0)) + sum(coalesce(mis.arsabildirimisayisi, 0)) 
    + sum(coalesce(mis.arazibildirimisayisi, 0)) as beyan_sayisi,
	string_agg(distinct mis.beyan_tipi , ',') as beyan_turu,
	now()::Date,
	'NETADMIN',
	'INSERT'
from 
	(
	select
		mis.beyanturu as beyan_tipi ,
		mis.mahalle as idari_mah_adi,
		mis.ada ,
		mis.parsel,
		case
			when mis.beyanturu = 'Arsa' then 1
			else 0
		end as arsabildirimisayisi,
		case
			when mis.beyanturu = 'Arazi' then 1
			else 0
		end as arazibildirimisayisi,
		case
			when mis.beyanturu = 'Bina' then 1
			else 0
		end as binabildirimisayisi,
		replace(mis.toplamborc::text , ',', '.')::float8 as toplam_borc ,
		now()::Date as m_date,
		'NETADMIN' as m_log_user,
		'INSERT' as m_status
	from 
		mis_eskom mis 
	inner join geomahalle g on g.adi_numarasi = mis.mahalle
	full outer join 
(
		select
			objectid ,
			idari_mah_id,
			idari_mah_adi,
			ada,
			parsel
		from
			parsel
		where
			geo_durum is true) p on
		p.objectid = mis.parsel_id
	where
		p.objectid is null
) mis 
group by
	mis.idari_mah_adi,
	mis.ada,
	mis.parsel;
	

INSERT INTO public.mis_entegrasyon_history 
(m_date, adet, kapsam) 
select now()::date, count(*), 'Eşleşmeyen Parseller' from mis_beyanli_parsel_eslesmeyen;

drop table mis_parsel_servis;
SELECT DISTINCT me.parsel_id,
    me.ham_ada,
    me.parsel,
    me.ham_mahalle,
    p.poly into mis_parsel_servis
   FROM mis_eskom me
     JOIN parsel p ON p.objectid = me.parsel_id
  WHERE p.geo_durum;