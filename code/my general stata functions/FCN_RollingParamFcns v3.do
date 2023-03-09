

*** THERE IS A SECOND FUNCTION AT THE BOTTOM OF THIS CODE!!!!! ***


capture program  drop  		getNewRollingParams
program define 				getNewRollingParams

/* ===============================================================================================

Data in memory has a list of "permno" and "event_month_stata" (month, in Stata format) where we 
want to estimate parameters.

INPUT 1: 	Path to save the successful parameter estimates.
INPUT 2: 	Path to save the permno-dates where estimation failed.
INPUT 3: 	Path to the CRSP daily returns file being used.
INPUT 4: 	Path to daily fama-french factors.

=================================================================================================

OUTPUT 1: 	Path is [INPUT 1]. Contains unique observations of (permno event_month_stata) and the variables

	parbeta_mktmod idio_mktmod					-parameters and idiosyncratic vol from model 1 (see below)
	parbeta_ff parsmb_ff parhml_ff idio_ff		-parameters and idiosyncratic vol from model 2 (see below)
	idio_xret									-vol(xret)

	WHERE EACH IS ESTIMATED ON RETURNS IN MONTHS T-13 TO T-1 (INCLUSIVE)
	
OUTPUT 2: 	Path is [INPUT 2]. Contains unique observations with: permno event_month_stata
	
=================================================================================================

Strategy: We estimate the parameters monthly. So we'd like to take each (permno event_month_stata),
expand by twelve, and assign a stata_month = t-13 to t-1 of event_month_stata. Then, we'd merge (permno
stata_month) into daily CRSP with a stata_month variable added. Then, we'd run the regressions on these months, 
store the results, and collapse back to the beginning.

Problem 1: Two estimation windows for a firm can overlap. 
Problem 2: The estimation steps are slow when doing too many at a time.

Solution 1/2: So assign an arbitrary increasing id number ("reg_id") to each (permno event_month_stata). 
			Expand and add a stata_month variable. Run the regressions on a subset of reg_id.
			
Problem 3: We'll add daily returns from CRSP using joinby (because a permno stata_month can be repeated).
			Joinby for all of CRSP is very slow.
			
Solution 3: Reduce CRSP to necessary permno-months only.

Strategy: Join the reduced CRSP to the (permno event_month_stata reg_id stata_month) dataset on permno 
stata_month. The run regressions by reg_id, storing results.

Problem 4: Joinby for the whole of the set of permno-event months to the reduced CRSP is still impossible.

Solution 4: Break set of permno-event months up into partitions. Run the regressions within this partition
and stitch the results from each partition together at the end.

Strategy: Run regressions by reg_id for the partition, storing results as (reg_id [params]).

Problem 5: Optimal partition size is about 100000. Optimal regression speed is on 1000 reg_id's at a time.

Solution 5: Break partitions into chunks. Run the regressions within these chunks and stitch the results 
together at the end of partition.

=================================================================================================


Pseudocode in order:

	(1) Prep input of permno events:
	
		Assign an integer to each (permno event_month_stata).	(Solution 2)
		Save (permno event_month_stata reg_id)					(FILE: "LINKING_TABLE")
		Expand, add months. 									(Solution 1)
		Save (permno event_month_stata stata_month reg_id) 		(FILE: "EXPANDED_EVENTS")
		Save (permno stata_month)								(Solution 3)
	
	(2) Prep CRSP: Reduce to needed months and merge in FF factors.
	
	(3) Run the regressions. Loop over partitions:
	
		(A) Break EXPANDED SOURCE file into a partition. Join this partition to CRSP.		
		(B) Run estimator function on chunks within this partition.
	
	(4) Aggregate results. For every chunk, the estimator function produces 3 files:
		
		- Each obs is reg_id, plus FF loadings and and FF idio vol
		- Each obs is reg_id, plus CAPM loadings and and CAPM idio vol
		- Each obs is reg_id, plus variance and count of xret 
		
		Append all files of each type together separately, then merge these on reg_id.
		
	(5) Outputs
	
		OUTPUT 1: Merge the aggregated results with "LINKING TABLE". Drop reg id. 
					Keep only _merge == 3, then drop _merge. Save.
		OUTPUT 2: Merge the aggregated results with "LINKING TABLE". Keep only _merge == 1.
					Keep permno event-stata_month.
			


===============================================================================================*/


*==============================================================================
*	(0) Prep folders for the partition/chunk results, ensure no contamination
*==============================================================================

/*
	The best solution I have currently is to stash the 4 model results with 
	a temp folder structure, then aggregate them with a looped append and 
	finally a merge based on reg_id (created below and then deleted after the
	merge back to permno-date).
	
	So reg_id is internally important, and the result files are given names 
	based on reg_id. Then the append thing appends all available result files
	without checks... what if a result is there from a prior run? 
	
	THAT'S NOT GOOD!
	
	So clean these folders out!	
*/

	set more off
	
	*** if folders don't exist, create 

	capture mkdir		"$temp/idio_xret"		// these names are specified in the getBetas_for_getNewRollingParams function below, so DO NOT change them!
	capture mkdir		"$temp/ff_regs"
	capture mkdir		"$temp/umd_regs"
	capture mkdir		"$temp/mkt_regs"
	
	*** delete all files within

	foreach folder in "$temp/idio_xret" "$temp/ff_regs" "$temp/umd_regs" "$temp/mkt_regs" {
	local files : dir "`folder'" files "*.dta"
	foreach file in `files' {
		erase "`folder'/`file'"
	}
	}
	
	

*==============================================================================
*	(1) Prep input of permno events
*==============================================================================
	
	keep permno date						// don't trust the user, i.e.:	 me :(
	duplicates drop permno date, force		// don't trust the user, i.e.:	 me :(

	* Assign an integer to each (permno event_month_stata).	(Solution 2)
	
	egen reg_id = group(permno date) 		// the group ID on which to run individual models!!!
														// Since we enforce only permno and event_month existing, there is no
														// possible conflict with existing vars. 
														//
														// This let's me be lazy in coding the sub function getBetas_for_getNewRollingParams too.
														//
														// This function is not "BREAK" robust, but given its usage for now 
														// (directly after a "use" statement), the cost is low.
	
*** Save file: "LINKING TABLE"

	tempfile LINKING_TABLE PARTITION_FILE EXPANDED_EVENTS permmonths_to_keep_crsp CRSP_light O_PARAMS		// some of the filenames we'll be making
	save "`LINKING_TABLE'", replace
	
*** Prep for joinby with CRSP (expand, add months) 

	expand 14
	sort reg_id
	by reg_id:	g stata_month = mofd(date) - _n + 1 	// (So we'll merge in CRSP info for the same month, and the 13 prior months.)

	save "`EXPANDED_EVENTS'", replace
		
*** create file of permno months needed to reduce CRSP database size, make joinby quicker later
	
	keep permno stata_month
	duplicates drop permno stata_month, force
	count
	sort permno stata_month
	save "`permmonths_to_keep_crsp'", replace
	
	
	
*==============================================================================
*	(2) Prep CRSP: Reduce to needed months and merge in FF factors.
*==============================================================================
	
	use "`3'" 	
	g stata_month = mofd(date)
	
*** Reduce to needed months 

	count
	merge m:1 permno stata_month using "`permmonths_to_keep_crsp'", keep(match)		
	drop _merge
	
*** Merge FF facttors
	
	merge m:1 date using "`4'",  keep(match)
	
	drop _merge	// should be perfect
	
	*sum mktrf smb hml rf 		// FF has these as percentage pts, redine to decimal
	foreach v in mktrf smb hml rf umd {
		replace `v' = `v'/100			
	}
	
	rename date crsp_date
	
	save "`CRSP_light'", replace
	
	
	
*==============================================================================
*	(3) Run the regressions. Loop over partitions:
*==============================================================================
	
	*** set up partition breaks
		
	use "`EXPANDED_EVENTS'", clear
	sum reg_id
	local max_reg_id = `r(max)'								// we need to do regs from 1 to max_reg_id
	local part_size = 100000								// partition size = 100,000
	local part_incs = ceil(`max_reg_id'/`part_size')		// how many increments?
	di "Increments to do: `part_incs'"						
	
	
	*** LOOP OVER PARTITIONS
	
	forval j = 1/`part_incs' {
	
		*** SET UP PARTITION CUTS (what is the highest and lowest reg_id in this?)
	
		local part_top = `part_size'*`j'
		local part_bottom = `part_top' - `part_size' + 1
		if `part_top' > `max_reg_id' {
			local part_top = `max_reg_id'
		}
		di "PARTITION: `part_bottom' to `part_top'."       
		
		*** ACTUALLY GET THE PARTITION
		*** (A) Break EXPANDED SOURCE file into a partition. Join this partition to CRSP.	
	
		use "`EXPANDED_EVENTS'", clear
		keep if reg_id >= `part_bottom' & reg_id <= `part_top'
		sum
		joinby permno stata_month using "`CRSP_light'"

		*After this, the reg_id has CRSP info for the same month, and the 13 prior 
		*months. Some of this is outside the estimation window! Run 

		drop if crsp_date > date - 30   		// crsp date must be at least 30 calendar days back
		drop if crsp_date < date - 30 - 365  	// crsp date is within 13 months of the date
		
		save "`PARTITION_FILE'", replace	
				
		*** SET UP CHUNK ITERATOR
		
		local inc_size = 1000
		local incs = ceil((`part_top'-`part_bottom')/`inc_size')
		
		*** LOOP OVER PARTITIONS
		
		forval i = 1/`incs' {
		
			*** SET UP CHUNK CUTS (what is the highest and lowest reg_id in this chunk?)
			
			local top = (`part_bottom'-1)+`inc_size'*`i'            // partitions start at say, 10001. So the first partition top is 10250. 
			local bottom = `top' - `inc_size' + 1
			if `top' > `part_top' {
				 local top = `part_top'
			}
			di "PARTITION: `part_bottom' to `part_top'. SUBSET: `bottom' TO `top'"    

			*** DO THE DAMN THING!!!
			*** (B) Run estimator function on chunks within this partition.
			
			getBetas_for_getNewRollingParams `bottom' `top'	`PARTITION_FILE'
			
			di "Done on subset."
			 
		}		
	}	
	

	
*==============================================================================
*	(4) Aggregate results. For every chunk, the estimator function produces 4 files, one per directory of result type:
*==============================================================================
	
*** aggregate within result time
	
	foreach folder in "idio_xret" "ff_regs" "umd_regs" "mkt_regs" {
	
	* append within type
	clear
	local files : dir "$temp/`folder'" files "*.dta"
	foreach file in `files' {
		di "$temp/`folder'/`file'"
		append using "$temp/`folder'/`file'"
	}
	save "$temp/`folder'/merged_`folder'"
	}

	
*** merge these 4 master files together
	
	use "$temp/ff_regs/merged_ff_regs", clear
	
	rename _b_* param_ff_*							// change names	
	g	idio_ff = _eq2_stat_1 / (_eq2_stat_2 - 1)	// e(rss)/(e(N)-1)	(get idio vol)
	rename _eq2_stat_2 obs_ff_est
	drop _eq*										// drop others needed
	
	* file 2
	
	merge 1:1 reg_id using "$temp/mkt_regs/merged_mkt_regs"
	drop _merge
	
	rename _b_* param_mkt_*							// change names	
	g	idio_mkt = _eq2_stat_1 / (_eq2_stat_2 - 1)	// e(rss)/(e(N)-1)	(get idio vol)
	rename _eq2_stat_2 obs_mkt_est
	drop _eq*										// drop others needed
	
	* file 3
	
	merge 1:1 reg_id using "$temp/idio_xret/merged_idio_xret"
	drop _merge
	
	rename _stat_1 idio_xret
	drop _stat
	
	* file 4
	
	merge 1:1 reg_id using "$temp/umd_regs/merged_umd_regs"
	drop _merge
	
	rename _b_* param_umd_*							// change names	
	g	idio_umd = _eq2_stat_1 / (_eq2_stat_2 - 1)	// e(rss)/(e(N)-1)	(get idio vol)
	rename _eq2_stat_2 obs_umd_est
	drop _eq*										// drop others needed
		
	*
	
	count
	
	sum
	
	pwcorr idio*
	pwcorr param*mktrf	
	
	*** requirement for # of obs in estimation?
	
	pwcorr ob*		// the obs variables are the same because the FF and mkt 
					// factors always exists, so LHS determines #
	tab obs_ff_est	// For p95, lose % if cutoff is
					//				2.1			50
					//				2.4			60		equivalent to 5 years of monthly obs
					//				3.0			75
					//				4.1			100
	
		
	drop obs_mkt_est obs_umd_est
	
	rename obs_ff_est obs_est_rolling
	label var obs_est_rolling	"OBSERVATIONS USED TO GET ROLLING PARAMETERS"
	
	*** use linking table: permno event_month_stata <==> reg_id to get permno event_month 
	
	rename * o_*
	rename o_reg_id reg_id
	
	save "`O_PARAMS'", replace
	
		
*==============================================================================
*	(5) Outputs
*==============================================================================
	
*** successes

	use "`LINKING_TABLE'"	
	merge 1:1 reg_id using "`O_PARAMS'", keep(match)
	drop _merge reg_id
	save "`1'", replace
	
*** failures	
	
	use "`LINKING_TABLE'"	
	merge 1:1 reg_id using "`O_PARAMS'", keep(master)
	keep permno date
	save "`2'", replace
	
end





*==============================================================================
*==============================================================================
*==============================================================================
*** PROG DO THE REGS AND STORE COEFFS
*==============================================================================
*==============================================================================
*==============================================================================

capture program  drop getBetas_for_getNewRollingParams
program define getBetas_for_getNewRollingParams
/*
Input 1: lowest reg_id number allowed
Input 2: highest reg_id number allowed
Input 3: path to partition file

OUTPUT FILES:

		reg_id ff_param_mktrf ff_param_smb ff_param_hml ff_param_cons
		reg_id mktmod_param_mktrf mktmod_param_cons

TEST FILE: "TEST_ROLLING_REGS"
*/

	set more off
	use "`3'", clear
	
	keep if reg_id >= `1' & reg_id <= `2'	
	
	keep permno reg_id stata_month date ret vwretd mktrf smb hml umd
		
	
	* idio_xret	
	
	g	xret = ret - vwret
	statsby r(Var) r(N), by(reg_id) saving("$temp/idio_xret/idio_xret_`1'_to_`2'", replace): sum xret 

	* FF model	
	
	statsby _b e(rss) e(N), by(reg_id) saving("$temp/ff_regs/ff_regs_`1'_to_`2'", replace): reg ret mktrf smb hml 

	* FF + UMD model	
	
	statsby _b e(rss) e(N), by(reg_id) saving("$temp/umd_regs/umd_regs_`1'_to_`2'", replace): reg ret mktrf smb hml umd

	* market model	
	
	statsby _b e(rss) e(N), by(reg_id) saving("$temp/mkt_regs/mkt_regs_`1'_to_`2'", replace): reg ret mktrf
		
end	
