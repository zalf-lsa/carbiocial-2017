#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Michael Berg-Mohnicke <michael.berg@zalf.de>
# Tommaso Stella <tommaso.stella@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# This file has been created at the Institute of
# Landscape Systems Analysis at the ZALF.
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

import csv
import json
import os
import sqlite3
import sys
import time
import zmq
import monica_io
import soil_io
import ascii_io
from datetime import date, timedelta
import copy
from absolute_rot_generator import generate_template_abs, rel_to_abs_dates, set_abs_dates
import numpy as np
from collections import defaultdict


USER = "hampf-desktop"

PATHS = {
    "hampf": {
        "INCLUDE_FILE_BASE_PATH": "C:/GitHub",
        "LOCAL_PATH_TO_ARCHIV": "Z:/md/projects/carbiocial/",
        "LOCAL_PATH_TO_REPO": "C:/GitHub/carbiocial-2017/"
    },
    "hampf-desktop": {
        "INCLUDE_FILE_BASE_PATH": "C:/Users/hampf/Documents/GitHub",
        "LOCAL_PATH_TO_ARCHIV": "Z:/md/projects/carbiocial/",
        "LOCAL_PATH_TO_REPO": "C:/Users/hampf/Documents/GitHub/carbiocial-2017/"
    },
    "stella": {
        "INCLUDE_FILE_BASE_PATH": "C:/Users/stella/Documents/GitHub",
        "LOCAL_PATH_TO_ARCHIV": "Z:/projects/carbiocial/",
        "LOCAL_PATH_TO_REPO": "C:/Users/stella/Documents/GitHub/carbiocial-2017/"
    },
    "berg-xps15": {
        "INCLUDE_FILE_BASE_PATH": "C:/Users/berg.ZALF-AD/GitHub",
        "LOCAL_PATH_TO_ARCHIV": "P:/carbiocial/",
        "LOCAL_PATH_TO_REPO": "C:/Users/berg.ZALF-AD/GitHub/carbiocial-2017/"
    },
    "berg-lc": {
        "INCLUDE_FILE_BASE_PATH": "C:/Users/berg.ZALF-AD/GitHub",
        "LOCAL_PATH_TO_ARCHIV": "P:/carbiocial/",
        "LOCAL_PATH_TO_REPO": "C:/Users/berg.ZALF-AD/GitHub/carbiocial-2017/"
    }
}

PATH_TO_ARCHIV_DIR = "/archiv-daten/md/projects/carbiocial/"
#PATH_TO_ARCHIV_DIR = "Z:/projects/carbiocial/"

def main():
    "main function"

    print ("____________________________________________________________________________________________________")
    print ("CHECK N and W response: if you are running sims for calcultating BYM, turn them off. Otherwise ON!!!")
    print ("CHECK output requirements: for maps and BYM are different!")
    print ("____________________________________________________________________________________________________")
    
    config = {
        "port": "6666",
        "start-row": "0",
        "end-row": "2543",
        "server": "localhost",
        "period": "historical"
    }
    if len(sys.argv) > 1:
        for arg in sys.argv[1:]:
            k,v = arg.split("=")
            if k in config:
                config[k] = v 

    local_run = False
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    if local_run:
        socket.connect("tcp://localhost:" + config["port"])
    else:
        socket.connect("tcp://" + config["server"] + ":" + config["port"])
    

    soil_db_con = sqlite3.connect(PATHS[USER]["LOCAL_PATH_TO_REPO"] + "soil-carbiocial.sqlite")
    #print soil_db_con

    with open("sim.json") as _:
        sim = json.load(_)

    with open("site.json") as _:
        site = json.load(_)

    with open("crop.json") as _:
        crop = json.load(_)

    with open("all_crops.json") as _:
        all_crops = json.load(_)

    sim["include-file-base-path"] = PATHS[USER]["INCLUDE_FILE_BASE_PATH"]

    periods = [
        {
            "name": "historical",
            "start_year": 1981,
            "end_year": 2012,
            "climate_folder": "climate-data-years-1981-2012-rows-0-2544"
        }, 
        {
            "name": "future_wrf",
            "start_year": 2001,
            "end_year": 2040,
            "climate_folder": "climate-data-years-2001-2040-rows-0-2544"
        },
        {
            "name": "future_starr",
            "start_year": 2013,
            "end_year": 2040,
            "climate_folder": "climate-data-years-2013-2040-rows-0-2544"
        }
    ]

    run_period = config["period"]

    # keep soybean as the first element please
    rotations = [
        #("soybean_7", "cotton")#,
        ("soybean_8", "maize")
    ]

    n_rows = 2544
    n_cols = 1928

    def read_onset_dates(period, row):
        "read onset dates and return a dict; k:col; v:(year, onset_doy)"
        
        print("reading onset dates of row " + str(row))

        filepath = PATHS[USER]["LOCAL_PATH_TO_ARCHIV"] + "onsets-"
        if period["name"] == "historical":
            filepath += "1981-2012/"
        elif period["name"] == "future_wrf":
            filepath += "2001-2040/"
        elif period["name"] == "future_starr":
            filepath += "2013-2040/"
        filepath += "onsets_row-" + str(row) + ".csv"

        onset_dates = defaultdict(list)

        with open(filepath) as _:
            reader = csv.reader(_)
            next(reader)
            for row in reader:
                col = int(row[2])
                onset_dates[col].append((int(row[0]), int(row[1])))
        return onset_dates

    def ascii_grid_to_np2darray(path_to_file, skipheader=True):
        "0=row, 1=col"
        with open(path_to_file) as file_:
            if skipheader:
                for header in range(0, 6):
                    file_.next()
            out = np.empty((n_rows, n_cols), np.dtype(int))
            r = 0
            for line in file_:
                if r == n_rows:
                    break
                c = 0
                for val in line.split(' '):
                    out[r, c] = int(val)
                    c = c + 1
                r = r + 1
            return out

    def grids_to_3darrays(path_to_grids, start_year, end_year, skipheader=True, file_ext=".asc"):
        "key=year, 0=row, 1=col"
        out = {}
        for filename in os.listdir(path_to_grids):
            if file_ext not in filename:
                continue            
            year = int(filename.split("_")[0])
            if int(year) < start_year or int(year) > end_year:
                continue
            print("loading " + filename)
            out[year] = ascii_grid_to_np2darray(path_to_grids + "/" + filename, skipheader)
        return out

    profile_cache = {}
    latitude_cache = {}
    def update_soil(row, col):
        "in place update the env"

        def lat(con, profile_id):
            query = """
                select                
                lat_times_1000
                from soil_profile_data 
                where id = ? 
                order by id
            """
            con.row_factory = sqlite3.Row
            for row in con.cursor().execute(query, (profile_id,)):
                if row["lat_times_1000"]:
                    return float(row["lat_times_1000"])

        profile_id = soil_ids[row, col]
        if profile_id in profile_cache:
            profile = profile_cache[profile_id]
            latitude = latitude_cache[profile_id]
        else:
            profile = soil_io.soil_parameters(soil_db_con, profile_id)
            profile_cache[profile_id] = profile
            latitude = lat(soil_db_con, profile_id) #TODO calculate latitude based on the row?
            latitude_cache[profile_id] = latitude

        for env in envs.itervalues():
            env["params"]["siteParameters"]["Latitude"] = latitude
            #site["HeightNN"] = #TODO
            env["params"]["siteParameters"]["SoilProfileParameters"] = profile

    print "loading soil id grid"
    soil_ids = ascii_grid_to_np2darray(PATHS[USER]["LOCAL_PATH_TO_ARCHIV"] + "Soil/Carbiocial_Soil_Raster_final.asc")

    env_no = 0
 
    #create an env for each rotation (templates, customized within the loop)
    crops_data = {}
    envs = {}
    for rot in rotations:
        my_rot = []
        for cp in rot:
            my_rot.append(all_crops[cp])
        crop["cropRotation"] = my_rot
        envs[rot] = monica_io.create_env_json_from_json_config({
            "crop": crop,
            "site": site,
            "sim": sim,
            "climate": ""
        })

        for i in range(len(rot)):
            cp = rot[i]
            if cp not in crops_data:
                crops_data[cp] = envs[rot]["cropRotation"][i]

    start_send = time.clock()
    for p in periods:
        if p["name"] != run_period:
            continue
        # onset are not anymore read from grids
        #print ("loading rain onset grids for " + p["name"])
        #rain_onset = grids_to_3darrays(PATHS[USER]["LOCAL_PATH_TO_ARCHIV"] + "rain_onset_grids/" + p["name"], p["start_year"], p["end_year"])

        templates_abs_rot = {}
        for rot in rotations:
            templates_abs_rot[rot] = generate_template_abs(rot, p["start_year"], p["end_year"], crops_data)

        for row in range(int(config["start-row"]), int(config["end-row"])+1): #n_rows):
            onset_dates_row = read_onset_dates(p, row)
            
            for col in range(n_cols):
                if soil_ids[row, col] == -9999:
                    continue
                
                #update soil data
                update_soil(row, col)

                #customize rotation
                #ref_dates = {}
                #for year in range(p["start_year"], p["end_year"] + 1):
                #    ref_dates[year] = rain_onset[year][row, col]

                #onset calculation requires 3 years in a row:
                #for this reason, we drop any onset calculated in the first year or in the first half of the second year
                #to avoid comparing two different seasons in the same map
                while onset_dates_row[col][0][0] == p["start_year"]:
                    onset_dates_row[col].pop(0)
                if onset_dates_row[col][0][0] == p["start_year"] + 1 and onset_dates_row[col][0][1] < 180:
                    onset_dates_row[col].pop(0)
                #print onset_dates_row[col]

                for rot in rotations:
                    env = envs[rot]
                    #env["cropRotation"] = rel_to_abs_dates(rot, templates_abs_rot[rot], p["start_year"], p["end_year"], ref_dates)
                    env["cropRotation"] = set_abs_dates(rot, templates_abs_rot[rot], onset_dates_row[col])

                    #set climate file - read by the server
                    env["csvViaHeaderOptions"] = sim["climate.csv-options"]
                    env["csvViaHeaderOptions"]["start-date"] = sim["start-date"].replace("1981", str(p["start_year"]))
                    env["csvViaHeaderOptions"]["end-date"] = sim["end-date"].replace("2012", str(p["end_year"]))
                    #note that the climate file content is csv like, despite the extension .asc
                    if local_run:
                        env["pathToClimateCSV"] = PATHS[USER]["LOCAL_PATH_TO_ARCHIV"] + p["climate_folder"] + "/" + "row-" + str(row) + "/col-" + str(col) + ".csv" 
                    else:
                        env["pathToClimateCSV"] = PATH_TO_ARCHIV_DIR + p["climate_folder"] + "/" + "row-" + str(row) + "/col-" + str(col) + ".csv" 

                    rot_id = rot[0] + "_" + rot[1]
                    env["customId"] = p["name"] \
                                        + "|" + str(row) \
                                        + "|" + str(col) \
                                        + "|" + rot_id

                    socket.send_json(env) 
                    print "sent env ", env_no, " customId: ", env["customId"]
                    env_no += 1

                    #if i > 150: #fo test purposes
                    #    return

    stop_send = time.clock()

    print "sending ", env_no, " envs took ", (stop_send - start_send), " seconds"
    

main()


#TEST
def send_test_env(path_to_env):
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    port = 6666 
    socket.connect("tcp://localhost:" + str(port))
    with open(path_to_env) as _:
        env = json.load(_)
    socket.send_json(env)
    print("test env sent!")

#send_test_env("test_env_fastest.json")
