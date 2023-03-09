
capture program  drop ccmMerge
program define ccmMerge
/*
DATA IN MEMORY: contains i_gvkey or o_gvkey (see input 1), date (of event)
GLOBAL IN MEMORY: $temp, a path to a temp folder to trash files
GLOBAL IN MEMORY: $ccmvars

Input 1: 	"o_" or "i_"
Input 2: 	Path to file from Compustat contain gvkey, datadate, others
				
OUTPUT: 	Data in memory + (Input 3) variables, with prefix of (Input 1)

============================================================================

Basic strategy: 

1.		Save memory data sorted on gvkey date.

2.		Find the latest previous obs in CCM for that gvkey and get a
		data structure containing (gvkey date $ccmvars), sorted on gvkey date.
		
	a.	For each obs, cross with all CCM obs for the same gvkey.
	b.	Keep CCM that preexists event, but not by too many years. Keep latest one.
	c.	Double check, and find out how many events are matched. (% of gvkey 
		dates that are success.)
		
3.		Combine the two datasets from the previous steps with:
		merge 1:m gvkey date using "STEP 1 DATA", keep (using match)

4. 		Double checks, prep output (var names).
		
*/



/* STEP 1: Save memory data sorted on gvkey date. */
qui {
	count
	local startingobs = `r(N)'
	
	sort `1'gvkey date
	
	save "$temp/temptemp938475938745", replace
}	



/* STEP 2: Find the latest previous obs in CCM for that gvkey-date & get $ccmvars. */
qui {
	*** Prep for cross

	rename `1'gvkey gvkey
		
	duplicates drop gvkey date, force			// saves memory, don't repeat work
	
	keep if gvkey != .
	
	keep gvkey date								// only keep these, will merge back other variables 
	
	g	temp_id = _n								// just to track original obs	
	
	count 
	local 	temp_to_try_to_match = `r(N)'
	
	
	
	*** For each obs, cross with all CCM obs for the same gvkey.

	joinby gvkey using "`2'"
	
	distinct temp_id
	local frac = floor(10000*`r(ndistinct)'/`temp_to_try_to_match')/100
}
	di "`frac'% gvkey-date possible after join"				// 83% for "i_" call
qui {	

	*** Keep latest CCM that preexists event, but not by too many years. 

	drop  if datadate == .
	
	drop if datadate < date - 365*4			 	// don't allow match if obs in CCM more than 4 years before event

	drop if datadate > date 		 			// CCM data must preexist

	bysort temp_id (datadate): keep if _n == _N // keep latest
	

	***	Double check things...  
	
	sum 
	
	*** How many events are matched? (% of gvkey dates that are success.)	
	
	distinct temp_id
	local frac = floor(10000*`r(ndistinct)'/`temp_to_try_to_match')/100
}
	di "`frac'% gvkey-date matched after restrictions"		// 70% for "i_" call
	
	/* NOTES from development on innovator events: 
	
		Lose 17% because no gvkey match in smaller_ccm database.
		Lose 13% due to year restrictions, ALMOST all lost by requiring 
				preexisting, not the "recent enough" restriction.	
	*/
	
	
	
	
	
/* STEP 3: Combine the two datasets from the previous steps. */
qui{
	sort gvkey date	
	rename gvkey `1'gvkey
	
	merge 1:m `1'gvkey date using "$temp/temptemp938475938745", keep (using match)
		
	drop temp_id _merge
}	
	
	
	
/* STEP 4. Double checks, prep output. */
qui {	
	local ccmvars = "$ccmvars"
	foreach v of varlist `ccmvars' {
		rename `v' `1'`v'
	}
		
	sort `1'gvkey date
}	
	
end
	
	
	
