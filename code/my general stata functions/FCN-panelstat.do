cap prog drop panelstat
prog define panelstat
syntax varname(max=1) , Start(int) End(int) stat(name)
/*
DESCRIPTION: For each observation, produces 
	
		stat(observations for that panel variable from t = start to t = end])
		
where stat can be any of the row`stat' functions of egen.

NOTES:
	
	* dataset must be xtset
	* stat can be one of: mean sd min median max miss nonmiss total
	* treats missing values as blanks; EXCEPT total, which treats missing as 0.
*/
qui {	
	if `start' > `end' {
		di as error "start must be not be bigger than end"
	}
	else {

	* clean up if these exist for some reason
	cap drop tempv00253_*

	* move adjacent panel values (in long form) from other time periods into new vars (wide form)
	forval i = `start'/`end' {
		if `i' < 0 {
			local j = - `i'
			g tempv00253__`j' = l`j'.`varlist'
		}
		else {
			g tempv00253_`i' = f`i'.`varlist'
		}
	}

	* make name of new variable
	if `start' < 0 {
		local startabsnum = abs(`start')
		local starttext = "l`startabsnum'" 
	}
	else {
		local starttext = "f`start'" 	
	}
	if `end' < 0 {
		local endabsnum = abs(`end')
		local endtext = "l`endabsnum'" 
	}
	else {
		local endtext = "f`end'" 	
	}
	
	* new variable
	local newname = substr("`stat'_`starttext'`endtext'__`varlist'",1,32) // make sure new var name is short
	cap drop `newname' // in case we did this already  	
	egen `newname' = row`stat'(tempv00253_*)

	* clean up
	drop tempv00253_*
		
	}
}
end

/* test data

clear
input t id y 
1 1 1
2 1 2
3 1 3
4 1 4
5 1 .
6 1 6
7 1 7
1 2 2
2 2 2
3 2 2
4 2 2
5 2 .
6 2 3
7 2 4
end

xtset id t
panelstat y , s(-2) e(-1) stat(mean)
panelstat y , s(1) e(2)  stat(mean)
list
*/
