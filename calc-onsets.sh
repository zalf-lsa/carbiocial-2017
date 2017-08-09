#!/bin/bash

start_year=$1
end_year=$2

for i in {0..2514..30}
  do
    python calc-onsets.py start-row=$i end-row=$((i+29)) from-year=$start_year to-year=$end_year input-path=/archiv-daten/md/projects/carbiocial/climate-data-years-$start_year-$end_year-rows-0-2544/ output-path=/archiv-daten/md/projects/carbiocial/onsets-$start_year-$end_year/ &
    #Rscript onset_calc.R $i $((i+29)) climate-data-years-2013-2040-rows-0-2544/ onset_out_2013_2040/ &
    #echo from $start_year to $end_year and from row $i to row $((i+29))
  done
		
python calc-onsets.py start-row=2520 end-row=2544 from-year=$start_year to-year=$end_year input-path=/archiv-daten/md/projects/carbiocial/climate-data-years-$start_year-$end_year-rows-0-2544/ output-path=/archiv-daten/md/projects/carbiocial/onsets-$start_year-$end_year/ &
#Rscript onset_calc.R 2520 2544 climate-data-years-2013-2040-rows-0-2544/ onsets_out_2013_2040/ &
#echo from $start_year to $end_year and from row 2520 to row 2544