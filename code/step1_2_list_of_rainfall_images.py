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

warnings.filterwarnings("ignore")


def list_dir_fn(rainfall_dir, end_file_name):
    """ Return a list of the rainfall raster images in a directory for the given file extension.

    @param rainfall_dir: string object containing the path to the directory containing the rainfall tif files
    (command argument --rainfall_dir).
    @param end_file_name: string object containing the ends with search criteria (command argument --search_criteria3).
    @return list image: list object containing the path to all rainfall images within the rainfall directory that meet
    the search criteria.
    """
    list_image = []

    for root, dirs, files in os.walk(rainfall_dir):

        for file in files:
            if file.endswith(end_file_name):
                img = (os.path.join(root, file))
                list_image.append(img)

    return list_image


def rainfall_start_finish_dates_fn(list_image):
    """ Sorts the list of paths numerically and extract the first and last dates for the available rainfall images.

    @param list_image: list object containing the path to all rainfall images within the rainfall directory that meet
    the search criteria - created under the list_dir_fn function.
    @return rain_start_date: string object containing the date of the first rainfall image available.
    @return rain_finish_date: string object containing the date of the last rainfall image available.
    """
    print('-' * 50)
    # sort the list image
    list_image.sort()

    path_s = list_image[0]

    image_ = path_s.rsplit('\\')
    image_name = image_[-1]
    year_s = image_name[:4]
    month_s = image_name[4:6]
    rain_start_date = (str(year_s) + '-' + str(month_s) + '-01')
    print('rain_start_date: ', rain_start_date)

    path_f = list_image[-1]
    image_ = path_f.rsplit('\\')
    image_name = image_[-1]
    year_f = image_name[:4]
    month_f = image_name[4:6]
    rain_finish_date = (str(year_f) + '-' + str(month_f) + '-30')
    print('rain_finish_date: ', rain_finish_date)

    return rain_start_date, rain_finish_date


def output_csv_fn(list_image, export_dir_path, variable):
    """ Return a csv containing each file paths stored in the list_image variable (1 path per line).

    @param list_image: list object containing the path to all rainfall images within the rainfall directory that meet
    the search criteria - created under the list_dir_fn function.
    @param export_dir_path: string object containing the path to the export directory.
    @return export_rainfall: string object containing the path to the populated csv.
    """
    # assumes that file_list is a flat list, it adds a new path in a new row, producing multiple observations.
    # todo remove the word new
    export_rainfall = (export_dir_path + '\\{0}_image_list.csv'.format(variable))
    with open(export_rainfall, "w") as output:
        writer = csv.writer(output, lineterminator='\n')
        for file in list_image:
            writer.writerow([file])

    return export_rainfall


def main_routine(export_dir_path, rainfall_dir, end_file_name, variable):

    # call the list_dir_fn function to return a list of the rainfall raster images.
    list_image = list_dir_fn(rainfall_dir, end_file_name)

    # call the rainfall_start_finish_dates_fn function to sorts the list of rainfall image paths numerically
    # and extract the first and last dates for the available rainfall images.
    rain_start_date, rain_finish_date = rainfall_start_finish_dates_fn(list_image)

    # call the output_csv_fn function to return a csv containing each file paths stored in the list_image variable
    # (1 path per line).
    export_csv = output_csv_fn(list_image, export_dir_path, variable)

    return export_csv, rain_start_date, rain_finish_date


if __name__ == "__main__":
    main_routine()
