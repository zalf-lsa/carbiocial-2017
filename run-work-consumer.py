#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Michael Berg-Mohnicke <michael.berg@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# This file has been created at the Institute of
# Landscape Systems Analysis at the ZALF.
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

import sys
#sys.path.insert(0, "C:\\Users\\berg.ZALF-AD\\GitHub\\monica\\project-files\\Win32\\Release")
#sys.path.insert(0, "C:\\Users\\berg.ZALF-AD\\GitHub\\monica\\src\\python")
#print sys.path

#import ascii_io
#import json
import csv
import types
import os
from datetime import datetime
from collections import defaultdict

import zmq
#print zmq.pyzmq_version()
import monica_io
import re
import numpy as np
#from dateutil.parser import parse

local_run = True

USER = "stella"

PATHS = {
    "stella": {
        "INCLUDE_FILE_BASE_PATH": "C:/Users/stella/Documents/GitHub",
        "LOCAL_PATH_TO_ARCHIV": "Z:/projects/carbiocial/",
        "LOCAL_PATH_TO_REPO": "C:/Users/stella/Documents/GitHub/carbiocial-2017/"
    }
}

#def to_doy(string):
#    try: 
#        my_date = parse(string)
#        tt = my_date.timetuple()
#        return tt.tm_yday
#    except ValueError:
#        return False

def update_temporary_output(tmp_data, values, orig_spec, oids, row, col, rot):
    "update temporary output data structure"
    if len(values) > 0:
        #check for crop/year index in oids
        for iii in range(0, len(oids)):
            oid = oids[iii]
            if oid["displayName"] != "":
                oid_name = oid["displayName"]
            else:
                oid_name = oid["name"]
            if oid_name == "Year":
                year_index = iii
            elif oid_name == "Crop":
                crop_index = iii
            
        for kkk in range(0, len(values[0])):
            for iii in range(0, len(oids)):
                oid = oids[iii]
                val = values[iii][kkk]
                year = values[year_index][kkk]
                crop = values[crop_index][kkk]
                if oid["displayName"] != "":
                    oid_name = oid["displayName"]
                else:
                    oid_name = oid["name"]
                if oid_name != "Year" and oid_name != "Crop":
                    tmp_data[rot][row][col][year][crop][oid_name] = val

def collector():
    "collect data from workers"

    n_rows = 2545
    n_cols = 1928

    def identify_cells_with_data(path_to_file):
        "0=row, 1=col"
        with open(path_to_file) as file_:
            for header in range(0, 6):
                file_.next()
            out = np.empty((n_rows, n_cols), np.dtype(int))
            r = 0
            for line in file_:
                c = 0
                for val in line.split(' '):
                    if val != "-9999" and val != "-9999\n":
                        out[r, c] = 1
                    c = c + 1
                r = r + 1
            return out
    
    print("loading template for output...")
    cells_with_data = identify_cells_with_data(PATHS[USER]["LOCAL_PATH_TO_ARCHIV"] + "Soil/Carbiocial_Soil_Raster_final.asc")
    cells_per_row = np.sum(cells_with_data, axis=1).tolist()
    print("load complete")
    
    #keys: tmp_data[rot][row][col][year][crop][oid_name]
    temp_out_data = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(dict))))))

    i = 0
    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    if local_run:
        socket.connect("tcp://localhost:7777")
    else:
        socket.connect("tcp://cluster2:7777")    
    socket.RCVTIMEO = 1000
    leave = False
    write_line = {}
    grid_created = False
    
    while not leave:
        try:
            result = socket.recv_json()
        except:
            continue

        if result["type"] == "finish":
            print "received finish message"
            leave = True

        else:
            print "received work result ", i, " customId: ", result.get("customId", "")

            custom_id = result["customId"]
            ci_parts = custom_id.split("|")
            period = ci_parts[0]
            row = int(ci_parts[1])
            col = int(ci_parts[2])
            rotation = ci_parts[3]
            
            if rotation not in write_line.keys():
                write_line[rotation] = 0

            for data in result.get("data", []):
                results = data.get("results", [])
                orig_spec = data.get("origSpec", "")
                output_ids = data.get("outputIds", [])
                if len(results) > 0:
                    #print len(results[0])
                    update_temporary_output(temp_out_data, results, orig_spec, output_ids, row, col, rotation)
            
            def add_line_grid(tmp_data, rot, write_line, cells_with_data, grid_fileinfo, copy_row=False):

                def append_line(filename, out_row):
                    with(open(filename, "a")) as _:
                        _.write(out_row)

                out_row = ""
                row = write_line[rot]
                if copy_row:
                    for value in cells_with_data[row]:
                        out_row += str(value) + " "
                        out_row = out_row.replace("0", "-9999")
                    for grid_file in grid_fileinfo[rot]:
                        append_line(grid_file["name"], out_row)
                else:
                    for grid_file in grid_fileinfo[rot]:
                        yr = grid_file["props"][0]
                        cp = grid_file["props"][1]
                        out_var = grid_file["props"][2]
                        for col in range(len(cells_with_data[row])):
                            if cells_with_data[row][col] == 0:
                                out_row += "-9999" + " "
                            else:
                                #tmp_data[rot][row][col][year][crop][oid_name]
                                try:
                                    if tmp_data[rot][row][col][yr][cp][out_var] == {}:
                                        print ("!!!!!!!!MISSING " + str(rot) + " " + str(row) + " " + str(col) + " " + str(yr) + " " + str(cp) + " " + str(out_var))
                                        out_row += "-9999" + " " #if for unknown reason the data is not there
                                    else:
                                        out_row += str(tmp_data[rot][row][col][yr][cp][out_var]) + " "
                                except:
                                    print 'MISSING DATA in rotation: {0} row: {1} col: {2} year: {3} crop: {4} output: {5}'.format(str(rot), str(row), str(col), str(yr), str(cp), str(out_var))
                                    out_row += "-9999" + " " #if for any reason the data is not there (e.g., stage not reached)
                        append_line(grid_file["name"], out_row)     
                    tmp_data[rot][row].clear() #save memory for next rows :)
                print 'added row {0} to files of rotation {1}'.format(str(row), str(rot))

            def write_line_check(write_line, tmp_data, cells_per_row, cells_with_data, grid_fileinfo):
                #tmp_data[rot][row][col][year][crop][oid_name]
                for rot in tmp_data.keys():
                    try:
                        row = write_line[rot]
                        n_cols = cells_per_row[row]
                        if n_cols == 0:
                            #copy from template
                            add_line_grid(tmp_data, rot, write_line, cells_with_data, grid_fileinfo, copy_row=True)
                            write_line[rot] += 1 #row
                            write_line_check(write_line, tmp_data, cells_per_row, cells_with_data, grid_fileinfo)
                        elif n_cols == len(tmp_data[rot][row]):
                            #fill template with available data
                            add_line_grid(tmp_data, rot, write_line, cells_with_data, grid_fileinfo, copy_row=False)
                            write_line[rot] += 1 #row
                            write_line_check(write_line, tmp_data, cells_per_row, cells_with_data, grid_fileinfo)
                    except IndexError:
                        print "time to check output maps for " + rot + " !"
            
            def create_retrieve_grid_files(tmp_data, period):
                #tmp_data[rot][row][col][year][crop][oid_name]
                header = """ncols         1928\nnrows         2545\nxllcorner     -9345.000000\nyllcorner     8000665.000000\ncellsize      900\nNODATA_value  -9999\n"""
                file_info = {rot: [] for rot in tmp_data.keys()}
                for rot in tmp_data.keys():
                    any_row = tmp_data[rot].keys()[0]
                    any_col = tmp_data[rot][any_row].keys()[0]
                    for yr in tmp_data[rot][any_row][any_col].keys():
                        for cp in tmp_data[rot][any_row][any_col][yr]:
                            cp_id = cp.replace("/", "") #avoid problems with out file name
                            cp_id = cp_id.replace(" ", "")
                            for out_var in tmp_data[rot][any_row][any_col][yr][cp]:
                                out_file = PATHS[USER]["LOCAL_PATH_TO_ARCHIV"] +"out_grids/" + period + "/" + cp_id + "_in_" + rot + "_" + out_var + "_" + str(yr) + ".asc"
                                file_info[rot].append({"name": out_file, "props": [yr, cp, out_var]})
                                with(open(out_file, "w")) as _:
                                    #header
                                    _.write(header)
                return True, file_info
                

            if i > 50: #wait to be sure that all the rotations are represented in temp_out_data
                if not grid_created:
                    grid_created, grid_fileinfo = create_retrieve_grid_files(temp_out_data, period)
                write_line_check(write_line, temp_out_data, cells_per_row, cells_with_data, grid_fileinfo)


            i = i + 1

collector()

