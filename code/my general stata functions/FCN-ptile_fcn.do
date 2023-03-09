cap prog drop my_perc_fcn
prog def my_perc_fcn
syntax, by(varlist) value(varname) 
/* Within groups specified in by() option, assign percentiles of a variable
(`value') to a new variable called `value'_ptile. */ 
	tempvar n_lower group_N

	egen `n_lower' = rank(`value'), by(`by') track
	replace `n_lower' = `n_lower' - 1 // "the track rank is 1 + the number of values that are lower" 
	
	egen `group_N' = count(`value'), by(`by')
	
	g `value'_ptile = floor(100*`n_lower'/`group_N')

end

cap prog drop my_ptiles_fcn
prog def my_ptiles_fcn
syntax varlist, by(varlist) 
	foreach v in `varlist' {
		cap my_perc_fcn, by(`by') value(`v') 
	}
end

/*
clear
input group value v2
1 -1 .
1 2 5
1 4 1
2 6 6
2 7 5
2 8 4
2 . 3
3 3 1
3 3 4
3 4 3
3 4 3
3 5 3
end 

*my_perc_fcn, by(group) value(value) 
*li, noo
my_ptiles_fcn value v2, by(group)
li, noo
*/
