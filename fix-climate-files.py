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
import sys
import time
import zmq
from collections import defaultdict

def main():

    config = {
        "from-row": "1",
        "to-row": "2545",
        "path": "P:/carbiocial/climate-data-years-1981-2012-rows-0-2544/"
    }
    if len(sys.argv) > 1:
        for arg in sys.argv[1:]:
            k,v = arg.split("=")
            if k in config:
                config[k] = v 

    path = config["path"]

    for row in xrange(int(config["from-row"])-1, int(config["to-row"])):
        for col in xrange(0, 1928):
            with open(path + "row-" + str(row) + "/col-" + str(col) + ".txt") as _:
                data = defaultdict(lambda: defaultdict(dict))
                out = open(path + "row-" + str(row) + "/col-" + str(col) + ".csv", "w")
                header_line_written = False
                for line in _:
                    if line[0:3] == "day":
                        if not header_line_written:
                            out.write(line)
                            header_line_written = True
                        else:
                            continue
                    else:
                        dmy = map(int, line[0:10].split(",")[:3])
                        data[dmy[2]][dmy[1]][dmy[0]] = line
                for year in sorted(data.keys()):
                    md_data = data[year]
                    for month in sorted(md_data.keys()):
                        d_data = md_data[month]
                        for day in sorted(d_data.keys()):
                            out.write(d_data[day])   

                #print "written", (path + "row-" + str(row) + "/col-" + str(col) + ".csv")              
                out.close()
        print "written row:", row  

main()

