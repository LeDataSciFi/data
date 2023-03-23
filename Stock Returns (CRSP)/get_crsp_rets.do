* daily ticker date ret: DSF ret + ticker valid for that date

clear
odbc load, exec("SELECT a.permno,a.date,b.shrcd, b.shrcls, b.ticker,a.ret,b.namedt,b.nameendt FROM crsp.dsf a INNER JOIN crsp.msenames b ON a.permno = b.permno WHERE date >= '2021-12-01' AND date <= '2023-01-15' AND a.date BETWEEN b.namedt AND b.nameendt ORDER BY a.permno, a.date DESC") dsn("wrds-pgdata-64")
		
distinct permno date, joint // if same, bueno

// students will be using ticker, so need distinct by ticker, except need to also add shrcls to ensure tic date uniqueness

drop if missing(ticker)

distinct ticker date, joint  

distinct ticker date shrcls, joint 

// so keep class A firms if duplicate 
// this doesn't cover all firms
// MKC is class V or (missing)
// UHAL is N or (missing)

// 21 problem firms
egen dups = count(ret), by(ticker date)
distinct ticker if dups > 1
drop dups

// drop non-class A if a class A exists
bysort ticker date: g keep = ((_N == 1) | (shrcls == "A")
bysort ticker date: egen keep_count  = sum(keep)
tab keep_c
drop if keep_c == 1 & keep == 0
drop keep keep_c 

// 2 problem firms
egen dups = count(ret), by(ticker date)
distinct ticker if dups > 1
drop dups

// drop non-labeled class if a labeled class exists
bysort ticker date: g keep = ((_N == 1) | (shrcls == "V" & tic == "MKC") | (tic == "UHAL" & missing(shrcls)))
bysort ticker date: egen keep_count  = sum(keep)
tab keep_c
drop if keep_c == 1 & keep == 0

// problem solved!
distinct ticker date if keep, joint   				
		
// output 
		
sort permno date

keep ticker date ret
order ticker date ret

saveold crsp_2022_only, replace
zipfile crsp_2022_only.dta, saving(crsp_2022_only , replace)

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
