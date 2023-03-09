**** Winsor program (winsor varname [1,5])
  capture program drop winsorsimple
  program define winsorsimple
	quiet sum `1', detail
	replace `1' = r(p1)  if `1' ~= . & (`1' < r(p1))  & `2' == 1
	replace `1' = r(p99) if `1' ~= . & (`1' > r(p99)) & `2' == 1
	replace `1' = r(p5)  if `1' ~= . & (`1' < r(p5))  & `2' == 5
	replace `1' = r(p95) if `1' ~= . & (`1' > r(p95)) & `2' == 5
  end

capture program  drop get_CRSP_vars_in_event_window
program define get_CRSP_vars_in_event_window
	
/* 
DATA IN MEMORY HAS "permno' and "date" and each obs is an event.
	
This function expands each event to the desired window size, obtains nearest 
valid CRSP dates with non-missing data for variables , and merges in the requested variables. 

	* INPUT 1 (int): 				window_start, # of **OBSERVATIONS** from the event nexus included (negative if before event date)
	* INPUT 2 (int): 				window_end, # of **OBSERVATIONS** from the event nexus included 	
	* INPUT 3 (str): 				CRSP file path
	* INPUT 4 (str, with blanks): 	variables from CRSP desired.
	
	The last two variables restrict allowed observations to be [t+`input5' 
	calendar days to t + `input6' calendar days from the event date in memory].
	
	* INPUT 5 (int): 				See above
	* INPUT 6 (int): 				See above 	
	 	
OUTPUT:

	Each observation in data in memory is possibly replicated as many times as
	the length of the window. Each new subobs for the event contains
	
		crsp_datadate 		- the date from which the crsp data comes
		dist_cal			- calendar distance from the date in memory for the event
		dist_obs			- trading days since the date in memory for the event
								0 is the first date (either exact date, or next
								if exact is unavailable)
		
	All OBS have a variable called:
	
		has_crsp_window 	- 1 if the variable has ANY CRSP window 
								observations, 0 if complete failure
								
	
	
WARNING #1!!!!! This program CHANGES the dataset in memory substantively!!!

	Not only is sort order altered, but new observations are created!!!
	
	This is purposeful, so that you can use the multiple observation per event
	to create new variables by event. It is the user's requirement to reduce it
	back to one OBS per event.
	
*/

	confirm integer number `1'
	confirm integer number `2'
	confirm integer number `5'
	confirm integer number `6'

	preserve
	
*** ERROR CATCH

	set more off
	if `1' > `2' {
		ERROR, 1 MUST NOT BE LARGER THAN 2
	}
	ds
	* make sure we don't try to merge in a var that is in CRSP
	foreach crsp_var in `4' {
		foreach existing_var in `r(varlist)' {
			if "`crsp_var'" == "`existing_var'" {
				di "ERROR: RENAME CURRENT variable `existing_var' so it doesn't conflict!"
				di "ERROR: `existing_var' is in the CRSP merge list but already exists..."
				ERROR, RENAME CURRENT 
			}			
		}		
	}
	* make sure we don't try to create a varname that already exists
	foreach new_var in "crsp_datadate" "obs_dist" "cal_dist" {
		foreach existing_var in `r(varlist)' {
			*di "`new_var' `existing_var'"
			if "`new_var'" == "`existing_var'" {
				di "ERROR: RENAME CURRENT variable `existing_var' so it doesn't conflict!"
				di "ERROR: This function creates an important variable called `existing_var'"
				ERROR, RENAME CURRENT 
			}			
		}		
	}

	
*** SOME EVENTS WON'T HAVE VALID INFO, THIS FILE IS USED TO BRING THEM BACK

	tempfile before finalanswer
	save `before'		// to merge completely failed matches back later
	

*** prep to merge in CRSP 

	* first replicate all observations
	
	local window_start 	= `1'
	local window_end 	= `2'
	local slack 		= 5					// allows us to get returns by skipping weekends, e.g.
	expand `window_end' - `window_start' + 1 + `slack' + `slack'	

	* "date" is the variable crsp uses, so rename the current date for the moment

	tempvar event_date 
	rename date `event_date'	
	
	* and date the replicate observations

	bysort permno `event_date': g date = `event_date' + `window_start' - `slack' + _n - 1

	* bring in crsp

	merge m:1 permno date using "`3'", keepusing(`4') keep(match)		// ONLY KEEP OBS with CRSP info, merge failed events back later...
	drop _merge

	rename date crsp_datadate				// improve the date name
	rename `event_date' date				// and reset the other one
	
	format crsp_datadate %td

*** RESTRICTION ON DATA: crsp vars must not be blank

	foreach crsp_var in `4' {
		drop if `crsp_var' == .
	}
	
	
*** POST MERGE: get the distance of each event obs from the main event obs (calendar and trading date distance)

	* we want to know the calendar distance from the event	
	
	g dist_cal = crsp_datadate - date				// positive is after the event, negative before

	* implement restrictions on trading date distance from event

	drop if dist_cal < `5'
	drop if dist_cal > `6'

	
	
	* AND we want to know the trading date distance from the event (or the first
	* trading date  after the event if the event isn't a trading date)
	
	bysort permno date (crsp_datadate): g dist_obs = _n
		// order them, then shift it down so the first trading date on or after the event is dist_obs == 0
		// we need the dist_obs # for the (first trading date on or after the event s dist_obs == 0)
		// in all obs for each event
	
	tempvar temp1 temp2 temp3 temp4
	g	`temp1' = (dist_cal >= 0 )	
	bysort permno date (crsp_datadate): g `temp2' = sum(`temp1') 		// `temp2' is 0 for obs before the event_date
																	// and increments 1 once dist_cal is >= 0
	replace `temp2' = dist_obs * (`temp2' == 1)							// the amount we want to shift down is `temp2'
	bysort permno date (crsp_datadate): egen `temp3' = sum(`temp2')	// this puts `temp2' in all obs for the event as `temp3'
	
	replace dist_obs = dist_obs - `temp3'								// shift down as described 
	
	// if temp3 == 0, then no obs for that event are after the event date... 
	// do the dist_obs needs to be shifted down so the latest date has dist_obs = -1
	bysort permno date (crsp_datadate): egen `temp4' = max(dist_obs) 
	replace dist_obs = dist_obs - 1 - `temp4' if `temp3' == 0
	
	
*** implement restrictions on trading date distance from event
	
	drop if dist_obs < `1'
	drop if dist_obs > `2'
	
	
*** join into the starter file

	save "`finalanswer'"
	
	restore
	
	*use "`finalanswer'", clear
	*merge m:1 permno date using "`before'"
	
	joinby permno date using "`finalanswer'", unmatched(both) 
		
	g has_crsp_window = (_merge == 3)
	drop _merge
	
		
di "	"
di "	"
di "===================================================================================="
di "	"	
di "WARNING #1!!!!! This program CHANGES the dataset in memory substantively!!!"
di ""
di "	Not only is sort order altered, but new observations are created!!!"
di ""	
di "	This is purposeful, so that you can use the multiple observation per event"
di "	to create new variables by event. It is the user's requirement to reduce it"
di "	back to one OBS per event."	
di "	"
di "===================================================================================="
di "	"	
	
end




/* TEST CODE

clear
input o_permno	date v1 v2
1	3	1	2
2	5	3	4
2	6	5	6
3	8	9	10
end
save fake_events, replace	

clear
input permno date ret v3
1	1	9	23
1	2	10	34
1	4	12	58
2	5	.5	48
2	6	.4	78
2	7	.3	77
1	8	13	76
1	9	14	75
end
save fake_crsp, replace		

clear
input permno crsp_datadate ret date dist_cal dist_obs has_crsp_window
1	2	10	3	-1	-1	1
1	4	12	3	1	0	1
1	8	13	3	5	1	1
1	9	14	3	6	2	1
2	5	.5	5	0	0	1
2	6	.4	5	1	1	1
2	7	.3	5	2	2	1
2	5	.5	6	-1	-1	1
2	6	.4	6	0	0	1
2	7	.3	6	1	1	1
3	.	.	8	.	.	0
end
save answer, replace			// note that day 5 is repeated

* get ret

use fake_events, clear 		// what I have, includes other vars, permno - date possibly not unique
rename o_permno permno		// FCN requires "permno" and "date" as names
get_CRSP_vars_in_event_window -1 2 "fake_crsp" "ret" -4 6
cf permno crsp_datadate ret date dist_cal dist_obs has_crsp_window using answer	// BOOM

* get ret and v3

use fake_events, clear 		// what I have, includes other vars, permno - date possibly not unique
rename o_permno permno		// FCN requires "permno" and "date" as names
get_CRSP_vars_in_event_window -1 2 "fake_crsp" "ret v3" -4 6
cf permno crsp_datadate ret date dist_cal dist_obs has_crsp_window using answer	// BOOM
*/


