/* 
Important: Need 

	1. To set up ODBC connection to WRDS for this code to word!
	2. Files within BHM S-1 project
	3. Cleaned CCM file 

*/

clear
set more off
capture log close 

/* PARAMETERS */

	// intermediate files will go in gitignored folder named DL_in_$updateY , documents time of DL

	global updateY "2023" 

/* create download folder and cd into it */

	cap mkdir "DL_in_$updateY"
	cd        "DL_in_$updateY"

/*	****	Compustat	****	*/

	local fyear_lo 2019
	local fyear_hi 2022
	local query "SELECT lnk.lpermno, lnk.lpermco, cst.* FROM crsp.ccmxpf_lnkhist lnk INNER JOIN comp.funda cst ON lnk.gvkey = cst.gvkey WHERE lnk.linktype IN ('LU', 'LC')    AND lnk.linkprim IN ('P', 'C')    AND lnk.linkdt <= cst.datadate    AND (cst.datadate <= lnk.linkenddt OR lnk.linkenddt IS NULL )   AND cst.indfmt = 'INDL'    AND cst.datafmt = 'STD'    AND cst.popsrc = 'D'    AND cst.consol = 'C'     AND cst.fyear BETWEEN `fyear_lo' AND `fyear_hi' ORDER BY lnk.gvkey, cst.datadate"

	clear
	odbc load, exec( "`query'"	) dsn("wrds-pgdata-64")

	save ccm, replace 

/*	****	FluidityDataExtend	****	*/

	local url "http://hobergphillips.tuck.dartmouth.edu/idata/FluidityData.zip"
	copy `url' FluidityData.zip
	unzipfile FluidityData.zip

	insheet using "FluidityData.txt", clear
	rename year datadateyear  
	sum
	save "FluidityData", replace

/*	****	TNIC3HHIdata_extend	****	*/

	local url "http://hobergphillips.tuck.dartmouth.edu/idata/TNIC3HHIdata.zip"
	copy `url' TNIC3HHIdata.zip
	unzipfile TNIC3HHIdata.zip

	insheet using "TNIC3HHIdata.txt", clear
	rename year datadateyear
	sum
	save "./TNIC3HHIdata", replace
  
/*	****	ConstraintsDatabase_ext2013	****	*/

	local url "http://faculty.marshall.usc.edu/Gerard-Hoberg/MaxDataSite/idata/ConstraintsDatabase_ext2015.zip"
	copy `url' ConstraintsDatabase.zip
	unzipfile ConstraintsDatabase.zip

	insheet using "ConstraintsDatabase_ext2015.txt", clear
	rename year datadateyear
	sum
	save "./ConstraintsDatabase", replace

/* clean up - keep dta and readme files */

	local datafiles: dir "" files "*.zip"

	foreach datafile of local datafiles {
		di "`datafile'"
		rm `datafile'
	}

	local datafiles: dir "" files "*.txt"

	foreach datafile of local datafiles {
		if substr("`datafile'",1,6) != "readme" {
			rm `datafile'			
		}
	}





/*	****	TNIC	****	*/

/*
insheet using "./raw_DL_in_$updateY/tnic3_allyears_extend_scores.txt", clear
rename year datadateyear
sort gvkey1 datadateyear

sort gvkey1 gvkey2 datadateyear
sum
replace score = 1 if score == .

save "./tnic3_allyears_scores-through2013", replace
*/

/*	****	FICHHI	****	*/

*insheet using "./raw_DL_in_$updateY/FICHHIdata_extend.txt", clear

/*	****	Fitted_herfindahl_Hoberg_Phillips	****	*/

*insheet using "./raw_DL_in_$updateY/Fitted_herfindahl_Hoberg_Phillips.txt", clear

/*	****	tnic2_allyears_extend_scores_HIGRAN	****	*/
/*
insheet using "./raw_DL_in_$updateY/tnic2_allyears_extend_scores_HIGRAN.txt", clear
rename year datadateyear
sum
save change name "./tnic2_allyears_extend_scores_HIGRAN", replace
*/