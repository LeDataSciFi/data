cap prog drop logthisvar_withoutlosingobs
prog def logthisvar_withoutlosingobs
	* logthisvar_withoutlosingobs `1' `2'
	* 1 is current variable
	* 2 is new variable
	confirm new variable `2'
	qui {
		sum `1'
		if `r(min)' < 0 {
			cap g `2' = log(1 + `1' - `r(min)')									
		}
		else {
			cap g `2' = log(1 + `1')									
		}
		count if `1' == .
		local nnnnn874358745 = `r(N)'
		count if `2' == .
		if `r(N)' != `nnnnn874358745' {
			drop `2'
			di as error "The new var has a different number of variables!"
			di as error "No new variable created!"
		}
	}	
	end
	cap prog drop logthisvar_withoutlosingobs_rep
	prog def logthisvar_withoutlosingobs_rep
	* logthisvar_withoutlosingobs_rep `1' 
	* 1 is current variable, it will be replaced
	tempvar newv
	logthisvar_withoutlosingobs `1' `newv'
	drop `1'
	rename `newv' `1'
end
