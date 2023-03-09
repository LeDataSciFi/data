
	**** Winsor program (winsorsimple varname [1,5])
	  capture program drop winsorsimple
	  program define winsorsimple
		quiet sum `1', detail
		replace `1' = r(p1)  if `1' ~= . & (`1' < r(p1))  & `2' == 1
		replace `1' = r(p99) if `1' ~= . & (`1' > r(p99)) & `2' == 1
		replace `1' = r(p5)  if `1' ~= . & (`1' < r(p5))  & `2' == 5
		replace `1' = r(p95) if `1' ~= . & (`1' > r(p95)) & `2' == 5
	  end
