
	
*********	winsorize WINVARLIST by BYVARLIST at p(0.01) tails

capture program drop winsorgroup
program define winsorgroup
	// byable(onecall)
	syntax varlist(min=1 numeric) ,  [ by(varlist)  p(real 0.01) ]
	//, min=0 max=0.5) 
	display "varlist now contains |`varlist'|"
	display "byvars now contains |`by'|"
	display "p now contains |`p'|"
	if `p' < 0 | `p' > .5 {
		display "ERROR: p() must be between 0 and 0.5"
		ERROR
	}
	
	
	
	/*
	egen group = group(`by')
	foreach v of varlist `varlist' {
		gen w_`v' = .
		qui su group, meanonly
		forval i  = 1/`r(max)' {
			capture { 
				winsor `v' if group == `i', gen(temp) p(0.01)		
				replace w_`v' = temp if group == `i'
				drop temp
			}
		}
	}
	drop group
	*/
end

winsorgroup o_param_ff_mktrf o_param_ff_smb, by(date) 
winsorgroup o_param_ff_mktrf o_param_ff_smb, by(date o_permno) p(0.01)
winsorgroup o_param_ff_mktrf o_param_ff_smb, p(0.01)


capture program drop winsorgroup
program define winsorgroup
	syntax varlist(min=1 numeric) ,  [ by(varlist)  p(real 0.01) ]
	* Examples:
	* winsorgroup varlist
	* winsorgroup varlist, by(year)
	* winsorgroup varlist, p(0.05)
	* winsorgroup varlist, by(year) p(0.05)
	*
	* If by() is not specified, winsorizes over all observations.
	* This winsorizes by percentile tails (for now).
	*
	* If p() not specified, 1% tails assumed. P must strictly be between 0 and 0.5
	
	display "varlist now contains |`varlist'|"
	display "byvars now contains |`by'|"
	display "p now contains |`p'|"
	if `p' < 0 | `p' > .5 {
		display "ERROR: p() must be between 0 and 0.5"
		ERROR
	}
	
	tempvar group
	egen `group' = group(`by')
	foreach v of varlist `varlist' {
		gen w_`v' = .
		qui su `group', meanonly
		di "Winsoring `v' across `r(max)' groups"
		forval i  = 1/`r(max)' {
			capture { 
				winsor `v' if `group' == `i', gen(temp) p(0.01)		
				replace w_`v' = temp if `group' == `i'
				drop temp
			}
		}
	}

end

sum o_param_ff_mktrf, d
*g year = year(date)
winsorgroup o_param_ff_mktrf , by(year) 
sum w_o_param_ff_mktrf, d

sum o_param_ff_smb, d
winsorgroup o_param_ff_smb , by(year) 
sum w_o_param_ff_smb, d







