import os
import csv
from datetime import date, timedelta
import json
import copy

def next_crop(rotation, index):
        if index == len(rotation) - 1:
            next_crp = rotation[0]
            next_index = 0
        else:
            next_crp = rotation[index + 1]
            next_index = index + 1
        return next_crp, next_index

def generate_template_abs(rel_rotation, start_year, end_year, crops_data):
    ""

    def create_cultivation_method(crop_info):
        cultivation_method = {"worksteps": []}
        for step in crop_info["worksteps"]:
            mystep = copy.deepcopy(step)
            cultivation_method["worksteps"].append(mystep)
        return cultivation_method

    current_index = -1 #identifies current crop

    buffer_years = 2 #to be sure that there will be some template cm at the end of the rotation (avoid rot to restart)
    n_years = end_year - start_year + 1 + buffer_years
    crops_per_year = 2

    template_abs_dates = []   

    for year in range(n_years * crops_per_year):
        crop_in_rotation, current_index = next_crop(rel_rotation, current_index)
        cultivation_method = create_cultivation_method(crops_data[crop_in_rotation])
        template_abs_dates.append(cultivation_method)

    return template_abs_dates

def set_abs_dates(rot, template_abs_rot, ref_dates):
    
    #max crop cycle duration; 10 days between harvest and sowing
    max_mz_c = 160 #used only the last year
    max_sun_c = 160 #used only the last year
    max_co_c = 210 #used only the last year
    max_soy_c = 130
    if "soybean_8" in rot:
        max_soy_c = 140

    cm = -1
    current_index = -1 #identifies current crop
    latest_harvest_mz = date(2199, 1, 1) # here to comply with q&d test
    latest_harvest_sun = date(2199, 1, 1)
    latest_harvest_co = date(2199, 1, 1)

    #ref_dates = [(year, onset_doy)]
    for rd in range(len(ref_dates)):
        year = int(ref_dates[rd][0])
        doy = int(ref_dates[rd][1])

        sowing_soy = date(year, 1, 1) + timedelta(days=doy - 1)
        latest_harvest_soy = sowing_soy + timedelta(days=max_soy_c)
        #latest harvest of mz and co cannot be > than (next onset - 5d): in this way soy is always sown
        if rd < len(ref_dates) - 1:
            year_next = int(ref_dates[rd+1][0])
            doy_next = int(ref_dates[rd+1][1])
            next_onset = date(year_next, 1, 1) + timedelta(days=doy_next - 6)
            latest_harvest_mz = next_onset
            latest_harvest_sun = next_onset
            latest_harvest_co = next_onset
        else:
            #the last year of the list does not have a next onset :)
            latest_harvest_mz = latest_harvest_soy + timedelta(days=(5 + max_mz_c))
            latest_harvest_sun = latest_harvest_soy + timedelta(days=(5 + max_mz_c))
            latest_harvest_co = latest_harvest_soy + timedelta(days=(5 + max_sun_c))
        

        for i in range(len(rot)):
            cm += 1
            crop_in_rotation, current_index = next_crop(rot, current_index)
            sowing_ws = template_abs_rot[cm]["worksteps"][0]
            harvest_ws = template_abs_rot[cm]["worksteps"][1]

            if "soybean" in crop_in_rotation: #works for both soy 7 and 8
                sowing_ws["date"] = unicode(sowing_soy.isoformat())
                harvest_ws["latest-date"] = unicode(latest_harvest_soy.isoformat())
            elif crop_in_rotation == "maize":
                harvest_ws["latest-date"] = unicode(latest_harvest_mz.isoformat())
            elif crop_in_rotation == "sunflower":
                harvest_ws["latest-date"] = unicode(latest_harvest_sun.isoformat())
            elif crop_in_rotation == "cotton":
                harvest_ws["latest-date"] = unicode(latest_harvest_co.isoformat())
        
    #put a footer cultivation method that will prevent following to be executed
    cm += 1
    crop_in_rotation, current_index = next_crop(rot, current_index)
    sowing_ws = template_abs_rot[cm]["worksteps"][0]
    harvest_ws = template_abs_rot[cm]["worksteps"][1]
    if "soybean" in crop_in_rotation: #works for both soy 7 and 8
        sowing_ws["date"] = unicode("2199-12-31")
        harvest_ws["latest-date"] = unicode("2199-12-31")
    else:
        #this should never be fired
        print("no soybean found as a footer!! Look for errors")

    #with open("test_rotation.json", "w") as _:
    #    _.write(json.dumps(template_abs_rot))

    return template_abs_rot

def generate_template_abs_old(rel_rotation, start_year, end_year, crops_data):
    "this functions works with the information from the file all_crops_OLD, which uses relative dates"

    def create_cultivation_method(year, crop_info, crop_id):
        cultivation_method = {"worksteps": []}
        added_year = False
        for step in crop_info["worksteps"]:
            mystep = copy.deepcopy(step)
            for k,v in mystep.iteritems():
                if "0001-" in str(v):
                    if not added_year: year +=1
                    added_year = True
                    break
            cultivation_method["worksteps"].append(mystep)
        return year, cultivation_method

    current_year = start_year
    current_index = -1 #identifies current crop

    template_abs_dates = []   

    while current_year <= end_year:

        crop_in_rotation, current_index = next_crop(rel_rotation, current_index)

        current_year, cultivation_method = create_cultivation_method(current_year, crops_data[crop_in_rotation], crop_in_rotation)

        template_abs_dates.append(cultivation_method)

    return template_abs_dates

def rel_to_abs_dates(rot, template_abs_rot, start_year, end_year, ref_dates_sowing):
    
    early_harvest_soy = False
    if "cotton" in rot:
        early_harvest_soy = True

    year = start_year

    current_index = -1 #identifies current crop

    for cm in range(len(template_abs_rot)):

        crop_in_rotation, current_index = next_crop(rot, current_index)

        sowing_ws = template_abs_rot[cm]["worksteps"][0]
        template = sowing_ws["earliest-date_relt"].replace("0000", str(year))
        if crop_in_rotation == "soybean":
            earliest_sowing = date(year, 1, 1) + timedelta(days=ref_dates_sowing[year] - 1)
            template = template.replace("09", str.zfill(str(earliest_sowing.month), 2))
            template = template.replace("15", str.zfill(str(earliest_sowing.day), 2))
            sowing_ws["earliest-date"] = template
            sowing_ws["latest-date"] = unicode((earliest_sowing + timedelta(days=30)).isoformat())
        else:
            sowing_ws["earliest-date"] = template
            sowing_ws["latest-date"] = sowing_ws["latest-date_relt"].replace("0000", str(year))

        harvest_ws = template_abs_rot[cm]["worksteps"][1]
        template = harvest_ws["latest-date_relt"]
        if "0000-" in str(template):
            harvest_ws["latest-date"] = template.replace("0000", str(year))
        else: # 0001-
            year += 1
            if crop_in_rotation == "soybean" and early_harvest_soy:
                template = template.replace("03-01", "02-10")
            template = template.replace("0001", str(year))
            harvest_ws["latest-date"] = template
    
    return template_abs_rot

def rel_to_abs_dates_old(rot, template_abs_rot, start_year, end_year, ref_dates_sowing):
    
    early_harvest_soy = False
    if "cotton" in rot:
        early_harvest_soy = True

    year = start_year

    current_index = -1 #identifies current crop

    for cm in range(len(template_abs_rot)):
        added_year = False
        crop_in_rotation, current_index = next_crop(rot, current_index)
        for step in template_abs_rot[cm]["worksteps"]:
            for k,v in step.iteritems():
                if "0000-" in str(v) and "_relt" not in str(k):
                    rel_template = k + "_relt"
                    template = step[rel_template]
                    template = template.replace("0000", str(year))
                    if crop_in_rotation == "soybean" and step["type"] == "AutomaticSowing":
                        earliest_sowing = date(year, 1, 1) + timedelta(days=ref_dates_sowing[year] - 1)
                        template = template.replace("09", str.zfill(str(earliest_sowing.month), 2))
                        template = template.replace("15", str.zfill(str(earliest_sowing.day), 2))
                        step["latest-date"] = unicode((earliest_sowing + timedelta(days=30)).isoformat())
                    step[k] = template
                if "0001-" in str(v) and "_relt" not in str(k):
                    if not added_year:
                        year += 1
                        added_year = True
                    rel_template = k + "_relt"
                    template = step[rel_template]
                    if crop_in_rotation == "soybean" and step["type"] == "AutomaticHarvest" and early_harvest_soy:
                        template = template.replace("03-01", "02-10")
                    template = template.replace("0001", str(year))
                    step[k] = template
    
    return template_abs_rot

