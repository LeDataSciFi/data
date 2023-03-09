
capture program drop mytable_fmt 	
program define mytable_fmt 
syntax , by_var(varname) table_vars(string) table_stats(string) 
/*
PURPOSE:  Compare several moments of a distribution by group, and do so for many variables

OUTPUT LOOKS LIKE:

by:                        group1    group2 ...
-----------------------------------------------
var1     |   stat1     |        x         x ...
         |   stat2     |        x         x ...
         |   stat3     |        x         x ...		 
         |   ...	   | 
         |   statS     |        x         x ...
-----------------------------------------------
var2     |   stat1     |        x         x ...
         |   stat2     |        x         x ...
         |   stat3     |        x         x ...		 
         |   ...	   |
         |   statS     |        x         x ...
-----------------------------------------------
...
-----------------------------------------------
varN     |   stat1     |        x         x ...
         |   stat2     |        x         x ...
         |   stat3     |        x         x ...		 
         |   ...	   |	 
         |   statS     |        x         x ...
*/
		 
preserve
qui {	

	*** prog

	levelsof `by_var',  local(bylevels) 

	local S: list sizeof local(table_stats)		// number of statistics 
	local V: list sizeof local(table_vars)		// number of variables 	

	qui distinct `by_var'
	local cols = `r(ndistinct)' 
	matrix out = J(`S'*`V',`cols'+1,0)
	
	forval stat_i = 1/`S' {
	
		local this_stat `: word `stat_i' of `table_stats''
		di "`this_stat'"
		
		qui {
		tabstat `table_vars', s(`this_stat') by(`by_var') save
		tabstatmat temp_`this_stat'
		matrix temp_`this_stat' = temp_`this_stat''
		}
		*mat li temp_`this_stat', noheader format(%9.2f)	
		qui {
		matrix kr  = J(`S',1,0)
		matrix kr[`stat_i',1] = 1	
		}
		matrix temp_`this_stat' = temp_`this_stat' # kr	
		
		matrix out = out + temp_`this_stat'
	}	
	
	mat li out
	
	*** produce output (including column labels and row labels)
	
	clear
	svmat out
	
	forval col = 1/`cols' {
		di `col'
		local vvv `: word `col' of `bylevels''
		di `"`vvv'"'
		label var out`col' `"`by_var': `vvv'"'
	}
	local lastcol = `cols' + 1
	label var out`lastcol' `"overall"'
	rename out`lastcol' overall
	
	format * %9.3g
	
	g variable = ""		// Stretch each `table_vars' `S' times
	g stat = "" 		// Repeat `table_stats' `V' times

	qui count
	forval row = 1/`r(N)' {

	local wordi = floor((`row'-1)/`S')+1
	local stati = mod(`row'-1,`S')+1
	di "`row' `wordi' `stati'"
	local v `: word `wordi' of `table_vars''
	local s `: word `stati' of `table_stats''
	
	replace variable = "`v'" if _n == `row'
	replace stat = "`s'" if _n == `row'
	}	
	
	order var stat

	foreach v of varlist out* {
	   local x : variable label `v'
	   *local q_`v'  =strtoname("`x'")
	   local q_`v'  =strtoname(substr("`x'",length(`"`by_var': "')+1,.))
	   ren `v' `q_`v''
	   label variable `q_`v'' "`v'"
	}
	
}	
	di _newline(10)
	di "STATISTICS BY SUBVALUES OF: `by_var'"
	di ""
	list, separator(`S')   noobs ab(18)
	restore
end	

