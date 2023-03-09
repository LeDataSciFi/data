
*** INPUTS (FILE LOCATIONS)
		
/*
INPUT 1: 	DATABASE (path) WITH DATE PERMNO, want to merge in gvkey

INPUT 2: 	CCM LINKING DATABASE (path)

INPUT 3: 	DATE VARIABLE

INPUT 4:	folder to throw temp files
		
OUTPUT:		INPUT 1 WITH NEW GVKEY (numeric) VARIABLE
*/

	
capture program  drop addGVKEYtoPERMNO
program define addGVKEYtoPERMNO
	
*** PREP INPUT 2 (LINKING TABLE)

qui {

	use "`2'", clear
	
	rename lpermno permno
	
	sort permno
	
	duplicates report permno
	
	save "`4'/linking_sorted_renamed", replace
	
}
	

qui {

	use "`1'", clear
	
	// the original variables
	local x
	foreach var of varlist _all {
		local x `x' `var'
	}
		
	count
	local whole_data_obs_count = `r(N)'
	
	g obs_tracker = _n
	
	*keep pnum- permno obs_tracker // for now, to make visual inspection easier
	
	sort permno
	
	merge m:m permno using "`4'/linking_sorted_renamed"
	drop if _merge == 2
	
	// save no merge partition
	
	preserve 
	keep if _merge == 1
	drop _merge
	save "`4'/no_merge_partition", replace
	restore
	
	*** deal with obs_tracker where permno has multiple possible gvkeys
	
	keep if _merge == 3 	
	duplicates tag obs_tracker, g(tag)
	tab tag
	
	// save merge but no duplicates partition
	
	preserve 
	keep if tag == 0
	drop _merge
	save "`4'/no_dups_partition", replace
	restore
		
	keep if tag > 0
	drop tag
		
	count
	local dup_count = `r(N)'
	if `dup_count' > 0 {
	
	distinct obs_tracker 	// how many should we have at end? 122
	local end_with = `r(ndistinct)'
		
	// if multiple obs of same gvkey only, keep that
	
	bysort obs_tracker (gvkey): g temp = gvkey!=gvkey[_n-1]		// temp looks for changing gvkey within obs_tracker
	bysort obs_tracker (gvkey): replace temp = 0 if _n == 1   
	bysort obs_tracker (gvkey): egen temp2 = sum(temp)				// temp2 = # of gvkey for the pnum MINUS 1 (so temp2 = 0 is 1 gvkey)
	bysort obs_tracker (gvkey): drop if _n > 1 & temp2 == 0		// if no changes, just keep the first
	
	drop temp
	
	distinct obs_tracker
	
	g 	end_gvkey = gvkey if temp2 == 0
	
	// now choose based on THE DATE VARIABLE (`3'/GRANT for this application)	
	
	g	choosethis 	= (linkdt <= `3' & linkenddt >= `3') if temp2 != 0
	bysort obs_tracker (gvkey): egen choicemade = sum(choosethis)					
	replace end_gvkey = gvkey if end_gvkey == "" & choosethis == 1 & choicemade == 1 // endgvkey isnt filled, the one we chose, and only one choice made	
	drop if end_gvkey == "" & choosethis == 0 & choicemade == 1		// DROP OTHERS NOT FILLED IN
	drop choosethis choicemade 
	
	// reduce to output
	
	drop gvkey
	rename end_gvkey gvkey
	keep `x' gvkey obs_tracker
	bysort obs_tracker: keep if _n == 1
	
	count
}
}
	if `dup_count' > 0 {
	if `r(N)' != `end_with' {
		di "SOMETHING IS WRONG dealing with duplicate gvkey possibilities!!!"
		error
	}
	}
	
qui {
	// append
	append using "`4'/no_merge_partition", force	
	append using "`4'/no_dups_partition", force			
		
	keep `x' gvkey
	
	count
}
	if `r(N)' != `whole_data_obs_count' {
		di "SOMETHING IS WRONG, end obs ~= start obs!!"
		error
	}

qui {
	destring gvkey, replace
	
	qui count if permno != .
	local perm = `r(N)'
	qui count if gvkey != .
	local ratio = floor(10000*`r(N)'/`perm')/100
}
	di "SUCCESSFULLY MATCHED: `ratio'% of perm to gvkey"
	
end
