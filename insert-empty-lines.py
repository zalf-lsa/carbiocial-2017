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

import os
import fileinput
from collections import defaultdict

def main():

    empty_line = ""
    for i in xrange(1928):
        empty_line += "-9999" + (" " if i < 1927 else "\n")

    #dic = defaultdict(lambda: 0)

    path = "C:/Users/berg.ZALF-AD/GitHub/carbiocial-2017/historical_2017-09-04/"
    #path = "P:/carbiocial/out_grids/future_starr/"
    for filename in os.listdir(path):

        lines = []
        with open(path + filename, "r+") as _:
            lines = _.readlines()
            no_lines = len(lines)

            #dic[no_lines] += 1
            #if no_lines not in [2550, 2105]:
            #    print path, filename

            #print no_lines,
            #continue

            if no_lines < 2545:
                print "in", (path+filename), "only", no_lines, "lines"

            _.seek(0)

            for no, line in enumerate(lines):
                if no_lines > 2550 and no > 5 and no < (6 + no_lines - 2550):
                    continue

                if no == 6 and no_lines <= 2545:
                    for i in range(5):
                        _.write(empty_line)

                _.write(line)

        print "updated", (path+filename)

    print dic

main()