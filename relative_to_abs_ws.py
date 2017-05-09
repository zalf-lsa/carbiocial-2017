import os
import csv
from datetime import date
from datetime import timedelta
import json
import copy

def generate_abs_rotation(rel_rotation, start_year, end_year):#, ref_dates):
    "generate absolute dates rotation based on rain season onset"    

    with open("all_crops.json") as all_crops:
        all_crops = json.load(all_crops)

    def next_crop(rotation, index):
        if index == len(rotation) - 1:
            next_crp = rotation[0]
            next_index = 0
        else:
            next_crp = rotation[index + 1]
            next_index = index + 1
        return next_crp, next_index

    def create_cultivation_method(year, crop_info):
        cultivation_method = {"worksteps": []}
        added_year = False
        for step in crop_info["worksteps"]:
            mystep = copy.deepcopy(step)
            for k,v in mystep.iteritems():
                if "0000-" in str(v):
                    mystep[k] = v.replace("0000", str(year))
                if "0001-" in str(v):
                    if not added_year: year +=1
                    added_year = True
                    mystep[k] = v.replace("0001", str(year))
            cultivation_method["worksteps"].append(mystep)
        return year, cultivation_method

    current_year = start_year
    current_index = -1 #identifies current crop
    
    rotation_abs_dates = []
    
    while current_year <= end_year:

        crop_in_rotation, current_index = next_crop(rel_rotation, current_index)
        
        current_year, cultivation_method = create_cultivation_method(current_year, all_crops[crop_in_rotation])

        rotation_abs_dates.append(cultivation_method)

    return rotation_abs_dates

rotation = ["soybean", "maize"]

generate_abs_rotation(rotation, 1980, 2010)
