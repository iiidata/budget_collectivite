SELECT
	cast(sis.siren as varchar(9)) as City_Siren,
	"Code INSEE Commune" as City_Insee_Code,
	"Nom Commune MAJ" as City_Name,
	Population as City_Population,
	cast(cast("Code EPCI" as integer) as varchar(9)) as City_EPCI_Siren,
	"NOM EPCI" as City_EPCI_Name,
	CASE
	    WHEN Population<500 THEN 1
	    WHEN Population between 500 and 2000 THEN 2
	    WHEN Population between 2000 and 3500 THEN 3
	    WHEN Population between 3500 and 5000 THEN 4
	    WHEN Population between 5000 and 10000 THEN 5
	    WHEN Population between 10000 and 20000 THEN 6
	    WHEN Population between 20000 and 50000 THEN 7
	    WHEN Population between 50000 and 100000 THEN 8
	    WHEN Population between 100000 and 300000 THEN 9
	    WHEN Population > 300000 THEN 10
	END as Strate_Code,
	CASE
	    WHEN Population<500 THEN 'Inférieur à 500 habitants'
	    WHEN Population between 500 and 2000 THEN 'De 500 à moins de 2000 habitants'
	    WHEN Population between 2000 and 3500 THEN 'De 2000 à moins de 3500 habitants'
	    WHEN Population between 3500 and 5000 THEN 'De 3500 à moins de 5000 habitants'
	    WHEN Population between 5000 and 10000 THEN 'De 5000 à moins de 10000 habitants'
	    WHEN Population between 10000 and 20000 THEN 'De 10000 à moins de 20000 habitants'
	    WHEN Population between 20000 and 50000 THEN 'De 20000 à moins de 50000 habitants'
	    WHEN Population between 50000 and 100000 THEN 'De 50000 à moins de 100000 habitants'
	    WHEN Population between 100000 and 300000 THEN 'De 100000 à moins de 300000 habitants'
	    WHEN Population > 300000 THEN 'Supérieur à 300000 habitants'
	END as Strate_Libelle,

FROM STG_CITY_REFERENTIAL scr
INNER JOIN STG_INSEE_SIREN sis on sis.insee_com = scr."Code INSEE Commune"
