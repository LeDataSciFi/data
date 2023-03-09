
capture program drop load_DGWT_expand_to_monthly
program define load_DGWT_expand_to_monthly
* USAGE: load_DGWT_expand_to_monthly [DGTW input csv file path] [output path]
*
* Loads the DGWT CSV file, cleans it, expands it to monthly

*** Load raw DGTW and clean

	set more off
	
	import delimited "`1'", ///
		clear delim(comma) varname(1)
	sum
		
	* require all info
	
	drop if size_jun == .
	drop if book_m_jun == .
	drop if mom_jun == .
	drop if permno == .
		
	* no duplicates!
	
	duplicates report permno year book_m_jun
	
	* dist of number of firms per port?
	
	bysort year size_jun book_m_jun mom_jun: g firms_per = _N if _n == 1
	sum firms_per, d
	drop firms_per
	
*** Fill in as monthly. Reassignments are on June 30 each year, assign as of July 1

	g 	ob 		= _n
	xtset permno year
	
	* 
	g 	daytemp = 1
	g 	monthtemp = 7
	g 	mdytemp = mdy(monthtemp, daytemp, year)	
	g 	month	= mofd(mdytemp)										
	drop daytemp monthtemp mdytemp year
	expand 12
	bysort ob: replace month = month[_n-1] + 1 	if _n > 1						
	drop ob
	
	// NOW WE HAVE PERMNO MONTH and the port each is assigned too

*** Outputs

	keep month size_jun book_m_jun mom_jun permno

	*** -> DATA A-2: unique month-DGTW port-permno mapping (aka event_port_id - permno is unique)
	*** -> OUTPUT (1): save A-2 within invariant output.
	
	save "`2'", replace
	
end
