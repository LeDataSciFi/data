
capture program drop DGTW_rets_on_set_of_dates
program define DGTW_rets_on_set_of_dates, rclass
/* 
USAGE: DGTW_rets_on_set_of_dates [win_params] output1_path output2_path dgtw_path crsp_path eventdatevar_name_to_make

DATA IN MEMORY HAS a date variable "date" and each obs is an event.
	
This function expands each date to 125 observations (1 for each DGWT 5x5x5 portfolio).

Then it merges in all permnos for each date-portfolios. (Using DGTW produced 
annual permno-portfolio map). 

	* OUTPUT 1:				This date-portfolio-permno mapping. (INPUT #5: "output1_path")
	* INPUT 5 (str):		"output1_path" DOES NOT INCLUDE .dta!!!!
	
	
Then, for each date-portfolio, get the equal and valued weighted portfolio 
return. To get this, it obtains the latest mktcap before the date, and the CRSP 
returns according to the 4 win parameters as follows. Look for the nearest
valid CRSP dates with non-missing data for variables for that permno, and merge
in the returns of that stock as long as they are within the window:

	* INPUT 1 (int): 		window_start, # of **OBSERVATIONS** from the event nexus included (negative if before event date)
	* INPUT 2 (int): 		window_end, # of **OBSERVATIONS** from the event nexus included 	
	
	The last two variables restrict allowed observations to be [t+`input5' 
	calendar days to t + `input6' calendar days from the event date in memory].
	
	* INPUT 3 (int): 		See above
	* INPUT 4 (int): 		See above 	

Now for each firm, sum the returns (arithmetic returns) and collapse back to 
date-portfolio-permno (and firm event returns). From here, collapse back to 
date-portfolio (and DGTW_ewret DGTW_vwret).

	* OUTPUT 2: 			[date DGWT_portfolio DGTW_ewret DGTW_vwret] (INPUT #6: "output2_path")
	* INPUT 6 (str): 		"output2_path" DOES NOT INCLUDE .dta!!!!
	
We also need to know

	* INPUT 7 (str): 		a path to the dgtw csv file
	* INPUT 8 (str): 		a path to the crsp dta file
	* INPUT 9 (str):		name of date/event date variable at end
	
	
WARNING #1!!!!! This program REPLACES the dataset in memory!!!	An error will NOT return the pre-call data!!!
*/

	confirm integer number `1'
	confirm integer number `2'
	confirm integer number `3'
	confirm integer number `4'
	
	*"output1_path" DOES NOT INCLUDE .dta!!!!
	*"output2_path" DOES NOT INCLUDE .dta!!!!

	keep date										// reduce to date only

	tempfile unique_event_date_DGTW_port permno_date_lag_mktcap_for_DGTW
	
	capture get_CRSP_vars_in_event_window // THIS FUNCTION MUST BE PRE LOADED
	if _rc != 7 {
		di _newline(10)
		forval i = 1/3{
		di "ERROR: get_CRSP_vars_in_event_window is not loaded!!!"
		}
		error
	}
	else {
		di _newline(10)
		forval i = 1/3{
		di "WARNING: DATA IN MEMORY DESTROYED!!!"
		
		// Because this function destroys memory, the environment is controlled and 
		// the variables names are lazily not stata "tempvar" names, as good 
		// function habits require...
		
		}
		error
	}


*** Expand each date to 125 portfolios (5x5x5)

	expand 5
	bysort date: g size_jun = _n
	expand 5
	bysort date size_jun: g book_m_jun = _n
	expand 5
	bysort date size_jun book_m_jun: g mom_jun = _n

	g	event_port_id = _n	// a by-able variable (b/c we will 
							// expand each obs later and work within 
							// the group)

*** Outputs
							
	*** -> DATA A: unique event date-DGTW port mapping.
	*** -> Save Data A as temp.
		
	save "`unique_event_date_DGTW_port'", replace	
		
	count 		// 333625

	
	
	
*======================================================================
*  (2) Match all DGTW permnos assigned to those DGTW ports for that date
*======================================================================

	di _newline(10)
	di "Match all DGTW permnos"
{
** get the portfolio-permno mapping from DGWT

	capture confirm file "`5'"
	// if we don't have the date-portfolio-permno mapping, get that, else skip it
	if _rc != 0 {	
		
		load_DGWT_expand_to_monthly "`7'" "`5'"
		*** USAGE: load_DGWT_expand_to_monthly [DGTW input csv file path] [output path]

		return scalar output1_redone = 1		// to display at the end whether this was done or not
	}
	else {
		return scalar output1_redone = 0 
	}

*** Load DATA A (unique event date - DGTW ports) and do the joinby to get permnos in each DGTW port
{
	use "`unique_event_date_DGTW_port'", clear
	
	g	month = mofd(date)
	
	joinby month size_jun book_m_jun mom_jun using "`5'" // bring in all permnos for each date-DGWT port

	*** -> DATA B: unique event date - DGTW port - permno map

	*** post join checks
	
		* dist of number of firms per port?

		bysort date size_jun book_m_jun mom_jun: g firms_per = _N if _n == 1
		sum firms_per, d					// [9, 527] med 16, mean 34, p25 12, p75 31
											// VERY similar!
										
		* year coverage?
		
		g year = yofd(date)
		sum year, d							// perfect- 1975-2010
		drop firms_per year		
		
		* merge quality?
		
		merge m:1 permno month size_jun book_m_jun mom_jun using "`5'"
			
		tab _merge 							// ~100k == 2
		g y = yofd(dofm(month))		
		sum y if _merge == 2				// 2011-2013 have DGTW, but KPSS don't provide permnos after 2010, so no matches are possible.	
											// But what about the 2010 obs?
		count if _merge == 2 & y == 2010	// ~3500
		format month %tm					 	
		tab month if _merge == 2 & y == 2010 // ALL failures in 2010 in december
		
		drop if _merge == 2
		drop _merge
		drop y
}	
}	

*======================================================================
*  (3)	Get CRSP event returns
*======================================================================

	di _newline(10)
	di "Get CRSP event returns"
	
*** checks

	duplicates report date permno	

*** merge in CRSP return info
		
	get_CRSP_vars_in_event_window `1' `2' "`8'" "ret" `3' `4'
		
	* Clean ret variable
	
	drop if ret == .
	
	* make sure our event-permnos have enough info

	*bysort event_port_id permno: g count = _N if _n == 1
	*tab count, m												// good
	
*** firm event level return (arithmetic returns)

	collapse (sum) firm_event_ret = ret, by(event_port_id permno)
	count
	
	* event coverage?
	
	bysort event : g c = _N if _n == 1
	sum c, d													// still good
	drop c

	*** -> DATA B (unique event date - DGTW port - permno) + firm_event_ret
	
	
*======================================================================
*  (4)	Get CRSP market value
*======================================================================
	
	di _newline(10)
	di "Getting Market Value"
	
***	(use section from GET EVENTS.do)
*** get CRSP event window information (prior mktcap)

	* merge the event date back in
	
	merge m:1 event_port_id using "`unique_event_date_DGTW_port'", keepusing(date)
	drop if _merge == 2			// the post 2010 dates
	drop _merge
			
	count													// want to end up back here
		
	preserve	
	
		get_CRSP_vars_in_event_window -5 1 "`8'" "prc shrout" -5 1
			// -> 2 minutes
			
			/* Inputs, what happens:		
			Keep trading days t-5 to t+1 around events, using CRSP file "fake_crsp",
			getting CRSP variables ret, prc, and shrout (must all be non-missing. 
			Trading days only kept if they are within [-5, 1] calendar days of the 
			event.  	*/
		
		
		* create mktcap variable	
		replace prc 	= -prc 		if prc < 0
		replace shrout 	= . 		if shrout == 0	
		g lag_mktcap = prc*shrout	
		winsorsimple lag_mktcap 1

		* keep closest date preexisting 
		keep if dist_cal < 0								// only keep preexisting dates!
		drop if lag_mktcap == .								// require variable to exist
		bysort permno date (dist_cal): keep if _n == _N		// keep last one
		
		* reduce to event identifying info and merge in new var
		keep permno date lag_mktcap
		save "`permno_date_lag_mktcap_for_DGTW'", replace
	
	restore
	count
	merge 1:1 permno date using "`permno_date_lag_mktcap_for_DGTW'", keep(match master)
	drop _merge
	count
	sum
	
	count  if firm_event_ret != . & lag_mktcap == . & date > 0 		// good! (very low)
																	// GOOD MATCH!!!
		
	***	-> DATA B + firm_event_ret + lag_mktcap: unique event date - DGTW port - permno
	
*** Calculate portfolio event returns

	di _newline(10)
	di "Calculate portfolio event returns"

	* value weighted (require BOTH mktcap and return)
	
	g	has_both 	= lag_mktcap != . & firm_event_ret != .	
	g 	temp_mkt	= lag_mktcap 		if has_both
	g 	temp_ret	= firm_event_ret 	if has_both
	
	bysort event_port_id: egen event_mktcap_weight = sum(temp_mkt) if has_both
	replace event_mktcap_weight = temp_mkt/event_mktcap_weight if has_both
	
	g 	temp_weighted = event_mktcap_weight*temp_ret if has_both
	
	* weights should sum to one within events...
	
	egen temp444 = sum(event_mktcap_weight), by(event_port_id) missing
	sum temp444												// GOOD!
	count if temp444 < .99									// GOOD!
	
	* calc
	
	egen DGTW_vwret = sum(temp_weighted), missing by(event_port_id)
	
	count if DGTW_vwret == .								// 0 ==> all events have a ret!

	drop temp* event_mktcap_weight has_both lag_mktcap	
	
	* equal weighted 

	bysort event_port_id: egen DGTW_ewret = mean(firm_event_ret)
	
	count if DGTW_ewret == .								// 0 ==> all events have a ret!
	
	tabstat DGTW_ewret DGTW_vwret, s(mean sd min p1 p5 p10 p25 p50 p75 p90 p95 p99 max)

	
*** Output
		
	bysort event_port_id: keep if _n == 1 					// reduce to event_port_id level
	
	*** merge the portfolio 5x5x5 numbers back in...
	
	merge m:1 event_port_id using "`unique_event_date_DGTW_port'", keepusing(size_jun book_m_jun mom_jun)
	drop if _merge == 2			// the post 2010 dates
	drop _merge

	
	keep date size_jun book_m_jun mom_jun DGTW_ewret DGTW_vwret
		
	***	-> DATA A + DGTW_ewret + DGTW_vwret
	***	-> OUTPUT (2): save within invariant output.		
	
	rename date `9'
	
	save "`6'", replace		
	

end
