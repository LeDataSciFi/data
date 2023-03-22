/*
	AUTHOR: Don Bowen
	DATE:   2020-06-01
    IN:     $ccm_data (raw CCM)
    OUT:    $ccm_cleaned (standard ready for analysis)	
    DESC:   includes deflators from BLS, Hoberg data
			1975- 
			drop sic 6, 49, 9
			require at != .
			CF source-use within 1%
	   
			WINSOR BY YEAR at 1%: financing info vars, CDWY CF identity vars
			NOT WINSOR: logged vars, dummy vars (e.g. div_d)   
*/

*======================================================================
*   LOAD FUNCTIONS 
*   assumes this folder is inside the cd
*======================================================================

	local folder "./my general stata functions"
	local files : dir "`folder'" files "*.do"
	foreach file in `files' {
		di "`folder'/`file'"
		qui do "`folder'/`file'"
	}

*======================================================================
*   Run download_raw_files.do ==> DL_in_yyyy folder 
*======================================================================

	manually? sure, why not

*======================================================================
*   Declare I/O and temp folders, output filename, variables to keep
*======================================================================
	
*** inputs 

	cd "DL_in_2023" // which download iteration to use?

	global ccm_data             "ccm" 
	global constraints  		"ConstraintsDatabase"
	global fluidity     		"FluidityData"
	global tnic3hhi     		"TNIC3HHIdata"
	
*** SPECIFY OUTPUT FILE PATH AND FILENAME	
	
	global ccm_cleaned "../../Firm Year Datasets (Compustat)/2021_ccm_cleaned.dta" // note: final line of code only keeps fyear 2021 
	
*** VARS TO KEEP: 

	*keep $ccmvars at_raw raw_* /* note: raw_* are CDWY vars) */
	
	// identifying info
	global ccmvars 		"         datadate lpermno lpermco gvkey tic fyear sic sic3 gdpdef age"
	// financing info
	global ccmvars 		"$ccmvars td me td_a td_mv l_a l_sale prof_a mb ppe_a cash_a div_d xrd_a dltt_a capx_a"
	global ccmvars 		"$ccmvars dv_a dltt_a invopps_FG09 sales_g l_reala l_reallongdebt short_debt long_debt_dum "
	global ccmvars      "$ccmvars atr smalltaxlosscarry largetaxlosscarry"
	// CDWY financing cash flow identify vars
	global ccmvars 		"$ccmvars Inv Ch_Cash Div Ch_Debt Ch_Eqty Ch_WC CF"  
	// constraint vars
	global ccmvars 		"$ccmvars kz_index ww_index hp_index ww_constrained ww_unconstrain kz_constrained kz_unconstrain hp_constrained hp_unconstrain"
		*HM_constrained HM_unconstrain"
	// hoberg vars
	global ccmvars 		"$ccmvars tnic3hhi tnic3tsimm prodmktfluid delaycon equitydelaycon debtdelaycon privdelaycon" 
	// others
	global ccmvars 		"$ccmvars l_emp l_ppent l_laborratio" 
	
	// WILL WINSORIZE: financing info vars, CDWY financing cash flow identity vars
	// EXCLUDE: logged vars, dummy vars (e.g. div_d)  
		
*======================================================================
* an XTILE function that doesn't die with big samples 
*======================================================================
	
	cap prog drop myxtile 
	prog def myxtile 
	syntax, val(varname) bygroup(varlist) Nquants(int) gen(name)
	/* CHANGES THE SORT ORDER */
	/* REPORTS AN XTILE EVEN IF ANY VARS IN "BYGROUP" ARE BLANK!  */
	qui {
		confirm new var `gen'
		tempvar N nless pctile

		sort `bygroup' `val'	/* CHANGES THE SORT ORDER */
			
		by `bygroup' (`val'): egen `N' = count(`val')
		by `bygroup' (`val'): g `nless' = (_n - 1) if (`val' != `val'[_n-1]) & `val' != .
		by `bygroup' (`val'): replace `nless' = `nless'[_n-1] if `nless' == . & `val' != .
		g `pctile' = `nless'/`N' + 0.000001* (mod(`nless'/`N',1/`nquants') == 0) // second term fixes beginning of bins: 0 perc -> bin 1, 50 perc -> bin 3 if quartiles
		g `gen' = ceil(`pctile'/(1/`nquants'))
		noi di as error "WARNING: myxtile changes the sort order!"
	}
	end	
	
*======================================================================
* a TSFILL function where all panels end in the same year
*======================================================================
	
	cap prog drop my_tsfill_forward
	prog def my_tsfill_forward
	syntax , time(varname) id(varname) [oneextrayear]
		tempvar new expand
		sum `time'
		local plus = 0
		if "`oneextrayear'" == "oneextrayear" {
			local plus = 1
		}
		bysort `id' (`time'): g `expand' = 2 if _N == _n & `time' < `r(max)' + `plus'
		expand `expand', gen(`new')
		replace `time' = `r(max)' + `plus' if `new' != 0
		tsset `id' `time'
		tsfill
	end		
	
*======================================================================
* ad-hoc reg functions
*======================================================================
	
	cap prog drop estadd_reghdfe
	prog def estadd_reghdfe
		foreach stat in N N_clust F r2 r2_a r2_a_within {
			cap estadd local = `e(`stat')'
		}
		foreach stat in vce clustvar vcetype absvars {
			cap estadd local = "`e(`stat')'"
		} 
	end
	
	cap prog drop esttab_reghdfe
	prog def esttab_reghdfe
	syntax, [options(string)]
		esttab , `options' ///
			star(* .1 ** .05 *** .01) b(3)  lab nogap ///
			stats(N N_clust F r2 r2_a r2_a_within vce clustvar vcetype absvars, ///
					fmt(%9.0fc %9.0fc 2 2 2 2 0) ///
					labels("N" "N_clust" "F" "r2" "ar2" "withinr2" "vce" "clustvar" "vcetype" "absvars") ///
			) 
	end	
		
*======================================================================
*   LOAD CCM  
*======================================================================
		
	use if fyear >= 1975 using ccm, clear
destring gvkey, force replace
duplicates report gvkey fyear
duplicates drop gvkey fyear, force

	xtset gvkey fyear	// note: gaps in time panel
	
	* SICs
	/* 
	destring sic, replace 
	sum sic*	
	count if sich != sic & sich != .
	g sicmash = sich
	replace sicmash = sic if sicmash == .
	drop sic sich
	*/
	g sic3 = floor(sich/10)
	rename sich sic
	label var sic  "Compustat SICH."
	label var sic3 "Compustat SICH."
	
	keep if ~(sic>=6000 & sic<=6999) & ~(sic>=4900 & sic<=4999) & ~(sic>=9000 & sic < 9999) 	 
	
	// MORE SAMPLE RESTIRCTIONS BELOW!!!
	
*======================================================================
*   DEFINE CASH FLOW IDENTITY VARIABLES AS IN CDWY 2014, RFS
*======================================================================

	/***************************************************************************
	/***************************************************************************

				THE SOURCES AND USES OF FUNDS equation:
			
				Inv + Ch_Cash + Div = Ch_Eqty + Ch_Debt + CF
			
				where "Ch_" means "change"

	***************************************************************************/
	***************************************************************************/

	replace scf = 7 if scf == . & fyear > 1988
	
	*** MISSING IMPUTATION... (helps the CDWY replications in terms of increasing # of valid obs.)

	* any obs is dropped if this imputation causes an accounting identity to error by more 1%

	foreach v in capx ivch aqc fuseo sppe siv ivstch ivaco chech dv dltis dltr dlcch sstk prstkc {
		replace `v' = 0 if `v' == .
	}
	foreach v in wcapc recch invch apalch txach aoloch fiao ibc xidoc dpc txdc esubc sppiv fopo fsrco exre {
		replace `v' = 0 if `v' == .
	}		

	*** DEFINE CASH FLOW IDENTITY VARIABLES AS IN CDWY 2014, RFS 
			
	// See CDWY , RFS 2014 for these
	// Some of these don't match CDWY listed equations on PURPOSE but are equivalent.
	// E.g. CDWY says CF is many variables. Compustat lists an equivalent definition
	// with three variables, which have fewer missing values.
	// CDWY: 0 = ChCash + Inv + Div - CF - Ch_Debt - Ch_Eqty

	g 		Inv 		= capx + ivch + aqc + fuseo - sppe - siv
	replace Inv 		= -ivncf												if scf == 7 
	
	g 		Ch_Cash 	= chech
	
	g		Div 		= dv
	
	g		Ch_Debt		= dltis - dltr + dlcch									if (scf != 1 & scf !=. ) 
	replace Ch_Debt 	= dltis - dltr - dlcch									if scf == 1
	replace Ch_Debt 	= dltis - dltr 											if Ch_Debt == . 
																				// DLCCH coverage bad
	
	g		Ch_Eqty		= sstk - prstkc
	
	g 		Ch_WC		= -wcapc
	replace Ch_WC		= wcapc													if scf == 1
	replace Ch_WC		= - recch - invch - apalch - txach - aoloch - fiao		if scf == 7 
	
	g		CF 			= fopt + fsrco - wcapc									if scf == 1
	replace CF 			= fopt + fsrco + wcapc									if scf == 2
	replace CF 			= fopt + fsrco + wcapc									if scf == 3
	replace CF 			= oancf + exre + fiao									if scf == 7 	
		
	sort gvkey fyear
	foreach v in Inv Ch_Cash Div Ch_Debt Ch_Eqty Ch_WC CF{
		by gvkey (fyear): replace `v' = `v'/at
	}
	
	g 		DIF		 	= abs(Inv + Ch_Cash + Div - Ch_Eqty - Ch_Debt - CF)
	
	label var Inv      "Investment (CDWY RFS 2014)"
	label var Ch_Cash  "Change in Cash (CDWY RFS 2014)"
	label var Div      "Div. (CDWY RFS 2014)"
	label var Ch_Debt  "Debt Iss. (CDWY RFS 2014)"
	label var Ch_Eqty  "Eqty Iss.  (CDWY RFS 2014)"
	label var Ch_WC    "Change in WC (CDWY RFS 2014)"
	label var CF       "Cash Flow (CDWY RFS 2014)"
	
*======================================================================
*   DEFINE OTHER VARIABLES
*======================================================================
	
	g	td					= (dlc+dltt)
	
	g	td_a				= td/at
	
	g 	short_debt 			= dlc/td							// % 1 yr debt
	g 	long_debt_dum 		= (dltt > 0) if dltt != .			// any long term debt?
	
	g	me					= csho*prcc_f
	
	g	td_mv				= td/(td+me)
	
	g	dltt_a  			= dltt / at
	
	g 	l_a 				= log(at)
	
	g	l_sale				= log(sale)
	
	g	prof_a				= oibdp/at							// aka ROA
	
	*g 	prof_dum 			= (prof_a > 0) if prof_a != .
	
	g	mb					= (at - ceq + (csho*prcc_f))/at 
	
	g	ppe_a				= ppent/at							// aka tangibility
	
	g	cash_a 				= che/at
	
	g	xrd_a	 			= xrd / at
	replace xrd_a 			= 0 if xrd_a == . 
	
	g 	capx_a 				= capx/at
	
	g 	div_d 				= .
	replace div_d 			= 0 if dv == 0
	replace div_d 			= 1 if dv >0
	replace div_d 			= . if dv == .
	
	g	dv_a				= dv/at
	
	g	invopps_FG09 		= (prcc_f * cshpri + pstkl + dltt + dlc - txditc) / at

	bysort gvkey (fyear): 	g 	sales_g = (sale/sale[_n-1])-1 if fyear == fyear[_n-1] + 1
	
	*bysort gvkey (fyear): 	g 	emp_g = (emp/emp[_n-1])-1 if fyear == fyear[_n-1] + 1
	
	g	temp 				= fyear if prcc_f != .
	bysort gvkey (fyear): 	egen first_fyear_with_price = min(temp)
	count if first_fyear_with_price > fyear & first_fyear_with_price != .
	g	age 				= fyear - first_fyear_with_price
	replace age 			= . if age < 0
	drop temp  first_fyear_with_price
	
	g 	atr 				= txt / (txt + ib)
	replace atr 			= 0 if txt < 0
	replace atr 			= 1 if txt > ib 
	replace atr 			= . if txt == . | ib == .
	
	egen temp = rowtotal(ib dp txt xint), missing
	g 	smalltaxlosscarry  	= (tlcf > 0) & (tlcf < ib + dp + txt + xint) if tlcf != . & temp != .
	g 	largetaxlosscarry	= (tlcf > 0) & (tlcf > ib + dp + txt + xint) if tlcf != . & temp != .
		// blank if tlcf blank OR if all of (ib dp txt xint) blank
		
	g l_emp = log(1+emp)
	g l_ppent = log(1+ppent)
	g l_laborratio = log(ppent/emp)
	
*======================================================================
*   DEFLATED VARIABLES
*======================================================================

	**** Get deflators 	
		
	preserve 
	
		import fred gdpdef, clear
		drop datestr
		rename GDPDEF gdpdef // legacy code was lower case
		g month = mofd(daten)
		drop daten
		format month %tm
		tsset month
		tsfill
		sort month
		replace gdp = gdp[_n-1] if gdp == .
		tempfile refmted_gdp_data
		save `refmted_gdp_data', replace 
	restore

	* merge deflators 
	g month = mofd(datadate)
	format month %tm

	merge m:1 month using `refmted_gdp_data'
	drop if _merge == 2
	drop _merge	
	drop month
	
	**** Deflated variables
	
	g 	l_reala 				= at/gdpdef		
	replace l_reala				= log(l_reala)
	label var l_reala 	"In 2012 Dollars" // sorta bad label: the anchor date depends on when you download from FRED, they might update it!
	
	g	l_reallongdebt 			= log(1 + (dltt/gdpdef))
	
*======================================================================
*   CLEAN VARIABLES (WINSORIZE)
*======================================================================

*** labels

	cap label var me                "csho*prcc_f"
	cap label var td_a 				"(dlc+dltt)/at"
	cap label var td_mv 		 	"(dlc+dltt)/(dlc+dltt+csho*prcc_f)"
	cap label var mb				"(at - ceq + (csho*prcc_f))/at "
	cap label var prof_a			"oibdp/at"
	cap label var ppe_a				"ppe/at"
	cap label var cash_a			"che/at"
	cap label var xrd_a				"xrd/at; 0 if blank"
	cap label var capx_a			"capx/at"
	cap label var dltt_a			"dltt/at"
	cap label var invopps_FG09 		"(prcc_f * cshpri + pstkl + dltt + dlc - txditc) / a"
	cap label var sales_g			"(sale/sale[_n-1])-1; within firm"
	cap label var dv_a				"dv/at"
	cap label var prof_dum			"(prof_a > 0) if prof_a != ."
	cap label var emp_g				"(emp/emp[_n-1])-1; within firm"
	cap label var short_debt		"% of 1 yr debt: dlc/td"
	cap label var long_debt_dum		"Any LT debt? (dltt > 0) if dltt != ."
	cap label var atr 				"Avg Tax Rate"
	
	cap label var l_a 				"Log(at)"
	cap label var l_sale 			"Log(Sales)"
	cap label var div_d 			"Div Dummy, blank if dv blank"
	cap label var age 				"fyear - first_fyear_with_price; blank if <0"
	cap label var l_reala 			"Log(Real Assets); 2009 $; BLS: GDPDEF"
	cap label var l_reallongdebt	"Log(1+(dltt/gdpdef)); 2009 $; BLS: GDPDEF"		
	
	cap label var l_emp             "Log(1+emp)"
	cap label var l_ppent           "Log(1+ppent)"
	cap label var l_laborratio      "Log(ppent/emp)"

*======================================================================
*  Constraint variables
*======================================================================
	
	*** KZ index
	
	bysort gvkey (fyear): 	g 	laggedppent = ppent[_n-1] if fyear == fyear[_n-1] + 1
	
	g 	kz_q 		= (at + prcc_f*csho - ceq - txdb) /at 
	g 	kz_prof 	= (ib + dp) /laggedppent
	g 	kz_lev 		= (dltt + dlc) /(dltt + dlc + seq )
	g 	kz_dv 		= (dvc +dvp) /laggedppent 
	g 	kz_cash 	= che /laggedppent 
	
	tabstat kz_* , s(mean min p1 p5 p25 p50 p75 p95 p99 max)

	foreach v in kz_q  kz_prof kz_lev kz_dv kz_cash	{		
		winsorby `v', by(fyear) p(0.05)
		drop `v'
		rename w_`v' `v'	// replace the present var
	}	
	
	g 	kz_index = 	- 1.001909  * kz_prof 				///
					+ 0.2826389 * kz_q					///
					+ 3.139193  * kz_lev				///
					- 39.3678   * kz_dv				  	///
					- 1.314759  * kz_cash		
					
	drop kz_q  kz_prof kz_lev kz_dv kz_cash 

	**** WW index 
	
	egen	posdiv 	= rowtotal(dvc dvp)				// treat missing as 0
	replace posdiv  = posdiv > 0 if posdiv != .		// indicator set to one if dvc + dvp is positive, and zero otherwise
	
	// average industry sales growth, estimated separately for each three-
	// digit SIC industry and each year, with sales growth defined as above] 
	cap g	sic3 = floor(sic/10)
	egen sic3year_sales_g = mean(sales_g) ,by(sic3 fyear)
	
	g 	ww_prof 	= (ib + dp) /at
	g	ww_lev 		= dltt/at	
					
	g 	ww_index = - 0.091 * ww_prof  			///
				   - 0.062 * posdiv 			///
				   + 0.021 * ww_lev				///
				   - 0.044 * l_a 				///
				   + 0.102 * sic3year_sales_g 	///
				   - 0.035 * sales_g
				   
	drop sic3year_sales_g ww_prof ww_lev posdiv
		
	**** HP index

	g 	size 		= l_reala
	replace size 	= log(5000) if size > log(5000) & size != .  	// HP correction uses 4500 as cap
																	// My dollars are in 2009. Deflator 2004 = .9. 4500/0.9 = 5000.
	g 	size2 		= l_reala^2
	
	g 	hp_age 		= age
	replace hp_age 	= 37 if hp_age > 37 & hp_age != .				// HP correction
	
	g 	hp_index  	= -0.737 * size + 0.043 * size2 - 0.040 * age
	 
	drop size size2 hp_age 
		
	sum *_index
		
	**** create tercile dummies for each (top terc is constrained, low terc is unc)
		
		**** Constraint tercile program (USE: "define_con_and_unc stem" (where stem is from the from variable: "stem_index")
		**** produces a dummy variables for those in lower third and upper third, by year, of a variable call [stem]_index
		capture program drop define_con_and_unc
		program define define_con_and_unc
			egen `1'_33 = pctile(`1'_index) , p(33) by(fyear)
			egen `1'_66 = pctile(`1'_index) , p(66) by(fyear)	
			g 	`1'_unconstrain 	= (`1'_index <= `1'_33)						// lower numbers -> less constrained
			g 	`1'_constrained 	= (`1'_index > `1'_66) if `1'_index != .
			drop `1'_33 `1'_66
		end
	
	define_con_and_unc ww
	define_con_and_unc kz
	define_con_and_unc hp
	  
	* how similar are these?
	pwcorr *_cons*
	pwcorr *_uncons*	
	
*======================================================================
*   HOBERG ET AL. VARIABLES
*======================================================================

	* HHI
	
	g datadateyear = year(datadate)
	
	merge m:1 gvkey datadateyear using "$tnic3hhi", keep(master match)	
	drop _m
	
	/*
	[gvkey datadateyear tnic3hhi tnic3tsimm]		TNIC3HHIdata_20130422.dta
	

	The TNIC3HHI variable is the concentration measure (note that this 
	is a sum of squared market shares and so is a number in the interval (0,1]) and 

	TNIC3TSIMM is the total similarity measure (note that 
	the total similarity variable is multiplied by 100, and so it is the sum of 
	percentage similarities, and also note that this measure 
	includes self similarity so the measure has a lower bound of 100).
	*/
	
	label var tnic3hhi 		"Concentration within TNIC3 peers"
	label var tnic3tsimm 	"Total simularity within TNIC3 peers"
	
	* Fluidity
	
	merge m:1 gvkey datadateyear using "$fluidity", keep(master match)
	drop _m

	label var prodmktfluid 	"Fluidity"

	* HM Constraints
		
	merge m:1 gvkey datadateyear using "$constraints", keep(master match)
	drop _m
	drop datadateyear 
	
	*gvkey year (delaycon equitydelaycon debtdelaycon privdelaycon)
	
	label var delaycon			"Higher -> more similar to delayed invest from liquidity"
	label var equitydelaycon	"Combines delaycon with indication of new equity soon"
	label var debtdelaycon		"Combines delaycon with indication of new debt soon"
	label var privdelaycon		"Combines delaycon with indication of new priv soon"
	
*======================================================================
*   SAMPLE RESTRICTIONS
*======================================================================
	
	* years
	keep if fyear >= 1975 								
		
	* SIC	
	// above
	
	* info requirements
	keep if (gvkey!=.) & (fyear!=.) & (at!=.)	
	
	* sources and uses equation holds	
	count
	count if DIF != .
	count if DIF == .                   // firm-years where we couldn't compute the whole identity
	                                    // what to do with these? (keep, I guess)
	tab fyear if DIF != . & DIF > 0.01  // firm-years with FAILED identities
	                                    // ALMOST ALL ARE IN 2006-2013
	tab fyear if DIF != . & DIF <= 0.01 // firm-years with good identities
	drop if DIF != . & DIF > 0.01
	drop DIF
			
	* # of years
	
	*bysort gvkey: egen years_in = count(at)
	*drop if years_in < 7
	*drop years_in
	
	* financially distressed firms
	
	*drop if at < 1																// NOT THE DEFLATED VERSION... simple
	
	*bysort gvkey (fyear): g 	at_growth = at/at[_n-1]
	*drop if at_growth > 2 & at_growth != .
	*drop at_growth
	
	*drop if sale < 1															// NOT THE DEFLATED VERSION... simple
				
*======================================================================
*	WINSORIZE
*======================================================================
	
*** keep versions of the CF vars unaltered

	g at_raw = at
	label var at_raw "Item AT, unaltered"
	
	foreach v in Inv Ch_Cash Div Ch_Debt Ch_Eqty Ch_WC CF {
		g raw_`v' = `v' 
		local hey : var label `v'
		label var raw_`v'  "Unwinsored: `hey'"		
	}	
	
*** winsorize 

	// WILL WINSORIZE: financing info vars, CDWY financing cash flow identify vars
	// EXCLUDE: logged vars, dummy vars (e.g. div_d)  
	foreach v in l_emp l_ppent l_laborratio Inv Ch_Cash Div Ch_Debt Ch_Eqty Ch_WC CF td_a td_mv mb prof_a ppe_a cash_a xrd_a dltt_a invopps_FG09 sales_g dv_a prof_dum emp_g short_debt	{	
		* winsorby varlist
		* winsorby varlist, by(year)
		* winsorby varlist, p(0.05)
		* winsorby varlist, by(year) p(0.05)
	
		// "cap" so it runs even if the var doesn't exist (e.g. you comment its definition out)
		cap winsorby `v', by(fyear) p(0.01)
		
		// update label
		if _rc == 0 {
			local label : variable label  `v'
			label variable w_`v' `"Winsored by Yr: `label'"'			
			drop `v'
			rename w_`v' `v'	// replace the present var
		}
	}	
	
*======================================================================
*   OUTPUT
*======================================================================
		
*** keep vars we want

	keep $ccmvars at_raw raw_* /* note: raw_* are CDWY vars) */	
	
	keep if fyear == 2021
	
	save "$ccm_cleaned", replace
	
