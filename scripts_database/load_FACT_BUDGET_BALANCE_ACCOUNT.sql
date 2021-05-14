SELECT
	cast(scaa.EXER as integer)*10000+1231 as date_id,
	dc.id as city_id,
	case
		when scaa.CBUDG = '1' then 'PRINCIPAL'
		when scaa.CBUDG = '2' then 'PRINCIPAL RATTACHE'
		when scaa.CBUDG = '3' then 'ANNEXE'
	end as Budget_Type,
	scaa.LBUDG as Budget_Name,
	scaa.CFT as Operating_Expense_Amount,
	scaa.PFT as Operating_Income_Amount,
	scaa.DIT as Invest_Expense_Amount,
	scaa.RIT as Invest_Income_Amount,
	(scaa.PFT - scaa.PF45) - (scaa.CFT - scaa.CF51 - scaa.CF52) as Gross_Saving

FROM STG_CITY_AGGREGATES_ACCOUNTANCY scaa
INNER JOIN DIM_CITY dc on dc.City_Siren = cast(scaa.SIREN as varchar(9))
