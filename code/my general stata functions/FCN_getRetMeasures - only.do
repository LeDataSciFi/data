**** Winsor program (winsor varname [1,5])
  capture program drop winsorsimple
  program define winsorsimple
	quiet sum `1', detail
	replace `1' = r(p1)  if `1' ~= . & (`1' < r(p1))  & `2' == 1
	replace `1' = r(p99) if `1' ~= . & (`1' > r(p99)) & `2' == 1
	replace `1' = r(p5)  if `1' ~= . & (`1' < r(p5))  & `2' == 5
	replace `1' = r(p95) if `1' ~= . & (`1' > r(p95)) & `2' == 5
  end
  

capture program  drop GetEventRetsBasic
program define GetEventRetsBasic

/*
Input 1: Name of the date variable
Input 2: CRSP file path

INPUT: 	Data in memory contains 1 observation per event with 
		"event date" (e.g. pubdate) and CRSP permno

NEED:	CRSP file containing permno, date, ret, vwretd, siccd

		Optionally: prc, shrout, vol can be used for other variables
		
OUTPUT: Returns data in memory, 
		market cap before event day (NOT IN THIS VERSION RIGHT NOW!!!!),
		various stock market return measures as specified below.

INFO:	Some grant dates (from KPSS) in their data are NOT Tuesday. 
		This code takes the 
		data as given and leaves the grant pubdate intact.
		
		Takes each patent grant pubdate and expands the pnum observations to
		multiple trading days, adjusting for weekends (but ignoring holidays).	
		
SIMPLIFIED VERSION OF SAME FUNCTION IN KPSS REPLICATION FOLDER!
*/

	set more off
	
	rename `1' date
	
	g	start_dow = dow(date)
	
	g	temp_event_id 	= _n
	
	expand 4				// each obs gets 4 days	(from -1 to +2 days from grant day)
	bysort temp_event_id (date): g days_from_event = _n - 2 	//
	replace date = date + days_from_event
	
	*g	dow = dow(date)
	*tab dow start_dow
	
	*** adjust for weekends (day of week 0 and 6)
		* thursday case
		replace date = date + 2 if start_dow == 4 & days_from >= 2 		// thurs start has sat, move to monday
		
		* friday case
		replace date = date + 2 if start_dow == 5 & days_from >= 1 		// fri start has sat, move to monday (sun to tues, etc)
	
		* sat case
		replace date = date + 2 if start_dow == 6 & days_from >= 0		// other days shifted forward
		
		* sun case
		replace date = date - 1 if  start_dow == 0 &  days_from == -1 	// move -1 from sat to fri
		replace date = date + 1 if  start_dow == 0 &  days_from >= 0	// other days shifted forward
		
		* mon case
		replace date = date - 2 if  start_dow == 1 &  days_from == -1 	// move -1 from sun to fri
	
	// these lines show how the modifications tweaked the dates... everything is
	// a weekday now...
	
	*drop dow  
	*g	dow = dow(date)
	*tab dow start_dow if days_from >= 0
	*tab dow start_dow if days_from == -1
	*drop start_dow dow
		
	sort permno date
	*desc
	
	label var days_from_event "Trading Days Since Event"
	

*** merge CRSP returns

	*use "$temp/pnum_permno_date_expanded_GRANTS", clear
	*local 2 "D:\Google Patent Grants\crsp_for_permnos_with_pats_KPSS_1925_2013"
	merge m:1 permno date using "`2'", keep(match master) keepusing(ret vwretd sic)
	g	xret = ret - vwretd
	drop ret vwretd
		
*** CREATE MEASURE OF INNOVATION #1 (Simple): xret = ret - vwretd
*** Create 4 versions: geometric and arithmetric means over window, excess and raw returns

	*save "$temp/pre_innov_measures_GRANTS", replace 		// checkpoint before a boss level
	*count
	*use "$temp/pre_innov_measures_GRANTS", clear 		// checkpoint before a boss level

{	
	
	sum xret, d
		
	global 	l 	2	// how many days after the patent grant does the event window go?
	local 	l 	"$l" // for variable names
	
	// USE ARITHMETIC AVERAGE RETURNS TO COMPARE TO KPSS
	// 0_2 means the event window is from the grant date (0) to 2 days after (2)	
	bysort temp_event_id (days_from): egen xret_arth_0_`l' = sum(xret) if days_from >= 0 & days_from <= $l		, missing	
	replace xret_arth_0_`l' = . if days_from != 0

	// without subtracting market returns
	*by temp_event_id (days_from): egen ret_arth_0_`l' = sum(ret) if days_from >= 0 & days_from <= $l		, missing	
	*replace ret_arth_0_`l' = . if days_from != 0
	
	// USE GEOMETRIC AVERAGE RETURNS FOR MY ANALYSIS	
	// 0_2 means the event window is from the grant date (0) to 2 days after (2)	
	g l_Gxret = ln(1 + xret) if _merge != 1
	by temp_event_id (days): egen xret_geo_0_`l' = sum(l_Gxret) if days >= 0 & days <= $l, missing
	replace xret_geo_0_`l' = exp(xret_geo_0_`l')
	replace xret_geo_0_`l' = . if days_from != 0	
	replace xret_geo_0_`l' = xret_geo_0_`l' - 1 if days_from == 0
	drop l_Gxret
	
	// without subtracting market returns
	*g l_Gret = ln(1 + ret) if _merge != 1
	*by temp_event_id (days): egen ret_geo_0_`l' = sum(l_Gret) if days >= 0 & days <= $l, missing
	*replace ret_geo_0_`l' = exp(ret_geo_0_`l')
	*replace ret_geo_0_`l' = . if days_from != 0	
	*replace ret_geo_0_`l' = ret_geo_0_`l' - 1 if days_from == 0
	*drop l_Gret	

}

*** WHAT ABOUT THOSE MATCHED BUT WITHOUT SAME DAY RETURNS 

/*	// they are set to missing currently, and how many are there?
	foreach v of varlist xret_* ret_* {
		di "`v'"
		count if `v' != . & ret == . & days_from == 0
	}

	* option 1:  LEAVE IN AS MISSING
	sum daily if days_from == 0, d

	* option 2:  DROP if missing measures
	sum daily if days_from == 0 & ret_geo_0_ != ., d
*/
	*** use option 1 ***
	
*** ready to merge to patent level data

	keep if days_from == 0
	
	drop temp_event_id days_from xret _merge
	
	capture drop mktcap
		
	rename date `1'

end
