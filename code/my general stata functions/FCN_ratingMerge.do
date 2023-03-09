* "C:\Research Local\Databases\ratings\via_compustat"

capture program  drop ratingMerge
program define ratingMerge
/*
DATA IN MEMORY: contains i_gvkey or o_gvkey (see input 1), date (of event)
GLOBAL IN MEMORY: $temp, a path to a temp folder to trash files

Input 1: 	"o_" or "i_"
Input 2: 	Path to file from Compustat contain gvkey, datadate, splticrm,
			spsdrm, & spsticrm 
				
OUTPUT: 	Data in memory + (i_rating or o_rating) variable

============================================================================

Basic strategy: There is the data in memory that has gvkey/date (possibly 
repeating). We want to get whether there was a rating prior to the event from
the data in input 2.

	1. Get the unique set of gvkey/date event in memory. We want to get whether 
		there was a rating prior to the event from the data in input 2. The idea
		is to the gvkey/date of each event with observations for that gvkey in
		input 2, and keep the last observation preceeding the event.
	2. However, there is a memory problem: Average gvkey has 200+ obs in input 
		2, so we want to lessen the data size... Build a temp dataset 
		containing for each event, the gvkey and the year of the event, plus 
		the proceeding two years.
	3. Merge this temp dataset against input 2 to reduce the dataset size, then
		cross it against the gvkeys of the events. For each event, keep the last 
		observation preceeding the event.	

*/

	save "$temp/in_memory934876983476", replace

	
	
*** step 1
	
	use "$temp/in_memory934876983476", clear

	keep `1'gvkey date
	*keep if `1'gvkey == 11128				// testing restriction
	duplicates drop `1'gvkey date, force
	*keep if _n == 10 | _n == 25			// testing restriction
	rename `1'gvkey gvkey
	rename date event_date
	g event_id = _n
	
	*******************************
	keep if gvkey != .
	*******************************
	
	sort gvkey event_date
	
	save "$temp/joinby_this", replace		// gvkey and event dates

	

*** step 2
*** joinby gvkey on the rating dataset is COSTLY, reduce the rating dataset a bit

	use "$temp/in_memory934876983476", clear

	keep `1'gvkey date
	*keep if `1'gvkey == 11128				// testing restriction
	duplicates drop `1'gvkey date, force
	*keep if _n == 10 | _n == 25			// testing restriction
	
	rename `1'gvkey gvkey

	*******************************
	keep if gvkey != .
	*******************************	
	
	g	yr = year(date)
	g event_id = _n	
	expand 3
	bysort event_id: g year = yr - _n + 1		// keep observations for the years t-2 t-1 and t
	keep gvkey year
	duplicates drop gvkey year, force
	sort gvkey year	
		
	save "$temp/gvkey_year_set", replace		// gvkey 

	
	
*** step 3	
***	get the lagged info
	
	use "`2'" , clear	
	destring gvkey, replace	
	g year = year(datadate)
	
	* remove real crap
	
	replace splticrm = "" if splticrm == "D" | splticrm == "N.M." | splticrm == "SD" | splticrm == "Suspended"	
	replace spsticrm = "" if spsticrm == "D" | spsticrm == "SD" 
	replace spsdrm   = "" if spsdrm == "D"   | spsdrm == "N.M."   | spsdrm == "Suspended"	
	
	* rated?
	
	g		rating = 0 if gvkey != .
	replace rating = 1 if gvkey != . & splticrm != ""
	replace rating = 1 if gvkey != . & spsdrm != ""
	replace rating = 1 if gvkey != . & spsticrm != ""
	
	* inv grade
	
	g 		invgrade = 0 if gvkey != . 
	replace invgrade = 1 if regexm(splticrm,"A") | regexm(splticrm,"BBB")
	replace invgrade = 1 if regexm(spsdrm,"A") | regexm(spsdrm,"BBB")
	replace invgrade = 1 if regexm(spsticrm,"A") 
	
	* junk
	
	g		junk 	= (rating == 1 & invgrade == 0)
	
	drop splticrm spsdrm spsticrm

	/* What % of firm-years in the rating data have ratings?
	distinct gvkey
	bysort gvkey year: egen ann_rat = sum(rating)
	tab ann_rat, m
	replace ann_rat = (ann_rat > 0)
	tab ann_rat, m
	bysort gvkey year: keep if _n == 1				// 1 obs per firm year
	sum
	tabstat ann_rat, s(n mean) by(year)				// 24% FY have ratings
	*/
	
	drop if year < 1981			// no ratings before (see block above)
	
	// reduce to obs within a few years of an event
	merge m:1 gvkey year using "$temp/gvkey_year_set"	
	keep if _merge == 3	
	drop _merge year
	
	// joinby event (all dates available for a firm to each firm's event)
	joinby gvkey using "$temp/joinby_this"		
	keep if event_id != .
	
	*** for each event, choose the latest 
	
	sort event_id datadate	
	g event_date_lagged = event_date - 365*0		// event_date - 365`targetlagyears'	
	keep if datadate < event_date_lagged			// all obs are known before event (possibly lagged)	
	bysort event_id (datadate): keep if _n == _N	// keep the last feasible obs per event
	
	*** information shouldn't be rotten (too old)
	
	drop if datadate < event_date - 365*3			// HARDWIRE: MUST BE WITHIN 3 YEARS (arbitrary)
	
	*** save

	drop event_date_lagged event_id	
	sort gvkey event_date
	
	save "$temp/ratings_to_merge_in", replace
	
	
	
	
*** merge back to original database

	* create rating dummy, set to missing if gvkey blank, 1 if present, 0 else
	* are all rating field values -> rating == 1?

	use "$temp/in_memory934876983476", clear
	rename `1'gvkey gvkey
	rename date event_date
	
	merge m:1 gvkey event_date using "$temp/ratings_to_merge_in"
	
	rename gvkey `1'gvkey
	rename event_date date
	rename rating `1'rating
	rename invgrade `1'invgrade
	rename junk `1'junk
		
	drop _merge datadate
	
	*******************************
	*******************************
	*replace `1'rating = 0 if `1'rating == . & `1'gvkey != .		// ASSUMPTION!!!
	*******************************
	*******************************
	
	tab `1'rating, m
	tab `1'invgrade, m
	tab `1'junk, m

end


/*

. use "C:\Research Local\Databases\ratings\via_compustat", clear
r; t=0.47 17:10:54

. r; t=0.17 17:11:00
tab splticrm

        S&P |
   Domestic |
  Long Term |
     Issuer |
     Credit |
     Rating |      Freq.     Percent        Cum.
------------+-----------------------------------
          A |     53,548        8.28        8.28
         A+ |     36,890        5.71       13.99
         A- |     48,346        7.48       21.47
         AA |     18,212        2.82       24.29
        AA+ |      5,157        0.80       25.08
        AA- |     22,790        3.53       28.61
        AAA |     10,346        1.60       30.21
          B |     39,063        6.04       36.25
         B+ |     65,070       10.07       46.32
         B- |     19,229        2.97       49.29
         BB |     41,243        6.38       55.67
        BB+ |     31,753        4.91       60.58
        BB- |     52,452        8.11       68.70
        BBB |     68,574       10.61       79.30
       BBB+ |     55,844        8.64       87.94
       BBB- |     52,030        8.05       95.99
          C |         42        0.01       96.00
         CC |      1,655        0.26       96.25
        CCC |      4,633        0.72       96.97
       CCC+ |      7,965        1.23       98.20
       CCC- |      1,891        0.29       98.49
          D |      7,300        1.13       99.62
       N.M. |      1,834        0.28       99.91
         SD |        599        0.09      100.00
  Suspended |          2        0.00      100.00
------------+-----------------------------------
      Total |    646,468      100.00
r; t=0.25 17:11:49

. tab spsdrm

        S&P |
Subordinate |
     d Debt |
     Rating |      Freq.     Percent        Cum.
------------+-----------------------------------
          A |      3,719        3.56        3.56
         A+ |      3,084        2.95        6.51
         A- |      7,154        6.85       13.36
         AA |        583        0.56       13.92
        AA+ |        414        0.40       14.32
        AA- |      1,966        1.88       16.20
        AAA |        404        0.39       16.59
          B |     12,361       11.84       28.42
         B+ |      9,171        8.78       37.20
         B- |     19,547       18.72       55.92
         BB |        279        0.27       56.19
        BB+ |      5,705        5.46       61.65
        BB- |      5,775        5.53       67.18
        BBB |      5,202        4.98       72.16
       BBB+ |      5,403        5.17       77.33
       BBB- |      6,956        6.66       83.99
          C |        929        0.89       84.88
         CC |        972        0.93       85.81
        CCC |      3,047        2.92       88.73
       CCC+ |      6,343        6.07       94.80
       CCC- |      1,592        1.52       96.33
          D |      3,821        3.66       99.99
       N.M. |         12        0.01      100.00
  Suspended |          2        0.00      100.00
------------+-----------------------------------
      Total |    104,441      100.00
r; t=0.19 17:11:53

. tab spsticrm

        S&P |
   Domestic |
 Short Term |
     Issuer |
     Credit |
     Rating |      Freq.     Percent        Cum.
------------+-----------------------------------
        A-1 |     67,220       31.02       31.02
       A-1+ |     47,561       21.94       52.96
        A-2 |     82,421       38.03       90.99
        A-3 |     11,912        5.50       96.48
          B |      4,065        1.88       98.36
        B-1 |        744        0.34       98.70
        B-2 |      1,128        0.52       99.22
        B-3 |        435        0.20       99.42
          C |        706        0.33       99.75
          D |        532        0.25      100.00
         SD |          9        0.00      100.00
------------+-----------------------------------
      Total |    216,733      100.00
r; t=0.20 17:11:58

*/
