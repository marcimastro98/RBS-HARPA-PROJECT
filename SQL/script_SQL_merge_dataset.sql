create temp table tmp_gdcea as 

select giorno, max_val-min_val as VALORE

from (

select 

CAST(data as DATE) as giorno,

MIN(valore) as min_val,
MAX(valore) as max_val

from generale_data_center_energia_attiva gdcea 

group by CAST(data as DATE)

);


---

create temp table tmp_geea as 
select giorno, max_val-min_val as VALORE

from (

select 

CAST(data as DATE) as giorno,

MIN(valore) as min_val,
MAX(valore) as max_val

from generale_edificio_energia_attiva geea 

group by CAST(data as DATE)

);

---

create temp table tmp_ifeap as 
select giorno, max_val-min_val as VALORE


from (

select 

CAST(data as DATE) as giorno,

MIN(valore) as min_val,
MAX(valore) as max_val

from impianto_fotovoltaico_energia_attiva_prodotta ifeap 

group by CAST(data as DATE)

);



--select * from tmp_gdcea order by giorno asc;
--select * from tmp_geea order by giorno asc;
--select * from tmp_ifeap order by giorno asc;


select 

tmp_geea.giorno,

tmp_geea.valore as geea_valore,
tmp_gdcea.valore as gdcea_valore,
tmp_ifeap.valore as ifeap_valore,

case 
	when tmp_geea.valore - tmp_gdcea.valore >= 0 then 'OK'
	else 'KO'
end as check_energia_tot_energia_dc,

case 
	when tmp_geea.valore + coalesce(tmp_ifeap.valore, 0) >= tmp_gdcea.valore then 'OK'
	else 'KO'
end as check_energia_tot_più_fotovoltaico_meno_energia_dc



from tmp_geea

left join tmp_gdcea
on tmp_geea.giorno = tmp_gdcea.giorno

left join tmp_ifeap 
on tmp_geea.giorno = tmp_ifeap.giorno

where 1=1


-- Con il fitro sottostante
--and tmp_geea.valore + coalesce(tmp_ifeap.valore, 0) - tmp_gdcea.valore < 0

order by tmp_geea.giorno ASC

;










