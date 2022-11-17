#!/usr/bin/env python

"""
step1_2_list_of_rainfall_images.py
=======================
Description: This script creates a csv containing the paths for all QLR rainfall raster images that meet the specified
search criteria, and create the two variables: rain_start_date, rain_finish_date which contain the date for the first
and last available images.


Author: Rob McGregor
email: robert.mcgreogr@nt.gov.au
Date: 27/11/2020
Version: 1.0

###############################################################################################

MIT License

Copyright (c) 2020 Rob McGregor

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the 'Software'), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.


THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

##################################################################################################

"""

# import modules
import os
import csv
import warnings
from glob import glob

warnings.filterwarnings("ignore")


def list_dir_fn(tree_height_dir, search_item):
    """ Return a list of the rainfall raster images in a directory for the given file extension.

    @param rainfall_dir: string object containing the path to the directory containing the rainfall tif files
    (command argument --rainfall_dir). @return list image: list object containing the path to all rainfall images within the rainfall directory that meet
    the search criteria.
    """
    list_image = []

    for image in glob(os.path.join(tree_height_dir, search_item)):
        # print("tree height: ", image)
        list_image.append(image)
    # print(list_image)
    return list_image

def output_csv_fn(list_image, export_dir_path, variable):
    """ Return a csv containing each file paths stored in the list_image variable (1 path per line).

    @param list_image: list object containing the path to all rainfall images within the rainfall directory that meet
    the search criteria - created under the list_dir_fn function.
    @param export_dir_path: string object containing the path to the export directory.
    @return export_rainfall: string object containing the path to the populated csv.
    """
    # assumes that file_list is a flat list, it adds a new path in a new row, producing multiple observations.
    export_rainfall = (export_dir_path + '\\{0}_image_list.csv'.format(variable))
    with open(export_rainfall, "w") as output:
        writer = csv.writer(output, lineterminator='\n')
        for file in list_image:
            writer.writerow([file])

    return export_rainfall


def main_routine(export_dir_path, variable_dir, variable, search_item):

    print("initiate step 1 2 list of tree height images")
    # # os walk
    # year_dir_list = next(os.walk(variable_dir))[1]
    # #print(year_dir_list)

    list_image = list_dir_fn(variable_dir, search_item)
    print(list_image)

    # call the output_csv_fn function to return a csv containing each file paths stored in the list_image variable
    # (1 path per line).
    export_csv = output_csv_fn(list_image, export_dir_path, variable)

    return export_csv #, rain_start_date, rain_finish_date


if __name__ == "__main__":
    main_routine()
