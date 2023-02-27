
* daily ticker date ret: DSF ret + ticker valid for that date

clear
odbc load, exec("SELECT a.permno,a.date,b.ticker,a.ret,b.namedt,b.nameendt FROM crsp.dsf a INNER JOIN crsp.msenames b ON a.permno = b.permno WHERE date >= '2021-12-01' AND date <= '2023-01-15' AND a.date BETWEEN b.namedt AND b.nameendt ORDER BY a.permno, a.date DESC") dsn("wrds-pgdata-64")
		
distinct permno date, joint // if same, bueno
				
sort permno date

keep ticker date ret
order ticker date ret

saveold crsp_2022_only, replace
zipfile crsp_2022_only.dta, saving(crsp_2022_only)

/* optionally reduce to S&P500 firms

preserve
	import delimited using input/sp500_firms.csv, clear
	li in 1/2
	keep symbol
	rename symbol ticker
	duplicates report *
	tempfile sample_firms
	save `sample_firms'
restore

merge m:1 ticker using `sample_firms', keep(3) nogen
distinct ticker

order ticker
drop permno
sort ticker date

duplicates drop ticker date, force
distinct ticker
