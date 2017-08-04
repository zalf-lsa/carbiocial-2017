#!/bin/bash

for i in {0..2520..20}
  do
    python calc_onsets.py start-row=$i end-row=$((i + 19)) input-path=climate-data-years-2013-2040-rows-0-2544/ output-path=onsets-1981-2012/ &
    #Rscript onset_calc.R $i $((i + 19)) climate-data-years-2013-2040-rows-0-2544/ onset_out_2013_2040/ &
    #echo $i, $((i+19))
  done
		
python calc_onsets.py start-row=2540 end-row=2544 input-path=climate-data-years-2013-2040-rows-0-2544/ output-path=onsets-1981-2012/ &
#Rscript onset_calc.R 2540 2544 climate-data-years-2013-2040-rows-0-2544/ onsets_out_2013_2040/ &
#echo 2540, 2544