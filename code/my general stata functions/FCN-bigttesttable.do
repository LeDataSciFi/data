cap prog drop bigttesttable
prog def bigttesttable
syntax varlist, grpvar(varname) [matname(name)]
* results are in a matrix called T unless matname() is specified

/*
PURPOSE:  Compare mean of many variables over two groups

OUTPUT LOOKS LIKE:

	by:     group1	group2	diff 	t		p
	-----------------------------------------------
	var1	mean1	mean2	#		tstat	pval
	...
	varN	mean1	mean2	#		tstat	pval

EXAMPLE: Creates excel file with one of these tables per sheet

	use data_file, clear
	keep if something1
	bigttesttable varlist, grpvar(treatment) matname("something1")

	use data_file, clear
	keep if something2
	bigttesttable varlist, grpvar(treatment) matname("something2")

	putexcel set "$univar_out/preevent_comps", modify 
	putexcel A1 = ("something1") B2 = matrix("Sample: something1", names), 	sheet("something1", replace)
	putexcel A1 = ("something2") B2 = matrix("Sample: something2", names), 	sheet("something1", replace)

*/

*local varlist pla l_a	                  	     // put varlist here
*local grpvar event_firm                           // put grouping variable here

	local n_var=wordcount("`varlist'")
	qui distinct `grpvar', 
	local n_grp=`r(ndistinct)'
	if `n_grp' != 2 {
		di "The code below assumes two groups."
		di "For a version ready for more group types,comparing the mean of each type to the overall, wisit:"
		di "http://stackoverflow.com/questions/24915409/create-comparison-of-means-table-with-multiple-variables-by-multiple-groups-comp"
		error
	}
	qui {
	mat T=J(`n_var',`n_grp'+3,.)                   //

	**colnames
		qui levelsof `grpvar', local(groupvals)
		di "`groupvals'"	
		local cnames = "`grpvar':`groupvals'"
		local cnames = subinstr("`cnames'", " "," `grpvar':",1)
		local cnames = "`cnames' diff t p"
		mat colnames T=`cnames'                    //

	**rownames
		mat rownames T=`varlist'                   //
		
	** tests	
		local i=1
		foreach var in `varlist'    {
		
			* means
			
			local j = 1
			foreach grp in `groupvals' {
				qui summ `var' if `grpvar' == `grp'
				mat T[`i',`j'] = `r(mean)'
				local ++j
			}
			
			* diff and 
			
			reg `var' `grpvar', robust
			mat T[`i',3] = _b[`grpvar']
			mat T[`i',4] = abs(_b[`grpvar']/_se[`grpvar'])
			mat T[`i',5] = (2 * ttail(e(df_r), abs(_b[`grpvar']/_se[`grpvar'])))
			
			local i=`i'+1
		}
	}
	mat list T, f(%8.3f)
	if  "`matname'" != "" {
	mat `matname' = T
	}	
	
end
