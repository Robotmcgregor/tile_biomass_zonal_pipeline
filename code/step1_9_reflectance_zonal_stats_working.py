#!/usr/bin/env python

# import modules
from __future__ import print_function, division

import fiona
import rasterio
import pandas as pd
from rasterstats import zonal_stats
import os
import shutil
import glob
import numpy as np
import geopandas as gpd
import warnings

warnings.filterwarnings("ignore")

'''
step1_5_dil_landsat_list.py
================

Description: This script calculates the zonal statistics of multi, multi band images, from polygon shapefiles.
Author: Grant Staben
email: grant.staben@nt.gov.au
Date: zzzz
Version: 1.0


Modified: Rob McGregor
email: Robert.Mcgregor@nt.gov.au
Date: 27/10/2020
Version: 2.0

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

========================================================================================================================
'''


def apply_zonal_stats_fn(image_s, no_data, band, shape, uid):
    """ Collect the zonal statistical information fom a raster file contained within a polygon extend outputting a
    list of results (final_results).

        @param image_s: string object containing an individual path for each rainfall image as it loops through the
        cleaned imagery_list_image_results.
        @param no_data: integer object containing the raster no data value.
        @param band: string object containing the current band number being processed.
        @param shape: open odk shapefile containing the 1ha site polygons.
        @param uid: unique identifier number.
        @return final_results: list object containing all of the zonal stats, image and shapefile polygon/site
        information. """

    # create empty lists to append values
    zone_stats = []
    list_site = []
    list_uid = []
    list_prop = []
    list_prop_code = []
    list_site_date = []
    list_image_name = []
    image_date = []
    list_band = []

    with rasterio.open(image_s, nodata=no_data) as srci:
        affine = srci.transform
        array = srci.read(band)

        with fiona.open(shape) as src:
            # using 'all_touched=True' will increase the number of pixels used to produce the stats 'False'
            # reduces the number define the zonal stats being calculated
            zs = zonal_stats(src, array, affine=affine, nodata=no_data,
                             stats=['count', 'min', 'max', 'mean', 'median', 'std', 'percentile_25', 'percentile_50',
                                    'percentile_75', 'percentile_95', 'percentile_99', 'range'], all_touched=False)

            # extract image name and append to list
            img_name = str(srci)[-54:-11]
            list_image_name.append(img_name)
            # extract image date and append to list
            img_date = str(srci)[-38:-30]
            image_date.append(img_date)

            for zone in zs:
                bands = 'b' + str(band)
                list_band.append(bands)
                # extract 'values' as a tuple from a dictionary
                keys, values = zip(*zone.items())
                # convert tuple to a list and append to zone_stats
                result = list(values)
                print("Results: ", result)
                zone_stats.append(result)

            for i in src:
                # extract shapefile records
                table_attributes = i['properties']

                uid_ = table_attributes[uid]
                details = [uid_]
                list_uid.append(details)

                site = table_attributes['site_name']
                site_ = [site]
                list_site.append(site_)

        # join the elements in each of the lists row by row
        final_results = [list_uid + list_site + zone_stats for
                         list_uid, list_site, zone_stats in
                         zip(list_uid, list_site, zone_stats)]

        # close the vector and raster file 
        src.close()
        srci.close()

    print("final results:", final_results)
    return final_results, str(site_[0])


def time_stamp_fn(output_zonal_stats):
    """Insert a timestamp into feature position 4, convert timestamp into year, month and day strings and append to
    dataframe.

    @param output_zonal_stats: dataframe object containing the Landsat tile Fractional Cover zonal stats
    @return output_zonal_stats: processed dataframe object containing the Landsat tile Fractional Cover zonal stats and
    updated features.
    """
    # Convert the date to a time stamp
    time_stamp = pd.to_datetime(output_zonal_stats.date, format='%Y%m%d')
    output_zonal_stats.insert(4, 'time_stamp_fn', time_stamp)
    output_zonal_stats['year'] = output_zonal_stats['date'].map(lambda x: str(x)[:4])
    output_zonal_stats['month'] = output_zonal_stats['date'].map(lambda x: str(x)[4:6])
    output_zonal_stats['day'] = output_zonal_stats['date'].map(lambda x: str(x)[6:])

    return output_zonal_stats


def landsat_correction_fn(output_zonal_stats):
    """ Replace specific 0 values with Null values and correct b1, b2 and b3 calculations
    (refer to Fractional Cover metadata)

    @param output_zonal_stats: dataframe object containing the Landsat tile Fractional Cover zonal stats.
    @return: processed dataframe object containing the Landsat tile Fractional Cover zonal stats and
    updated values.
    """

    # min imports a zero as a minimum
    output_zonal_stats['b1_ref_min'] = output_zonal_stats['b1_ref_min'].replace(0, np.nan)
    output_zonal_stats['b2_ref_min'] = output_zonal_stats['b2_ref_min'].replace(0, np.nan)
    output_zonal_stats['b3_ref_min'] = output_zonal_stats['b3_ref_min'].replace(0, np.nan)

    # band 1
    output_zonal_stats['b1_ref_min'] = output_zonal_stats['b1_ref_min'] - 100
    output_zonal_stats['b1_ref_max'] = output_zonal_stats['b1_ref_max'] - 100
    output_zonal_stats['b1_ref_mean'] = output_zonal_stats['b1_ref_mean'] - 100
    output_zonal_stats['b1_ref_med'] = output_zonal_stats['b1_ref_med'] - 100
    output_zonal_stats['b1_ref_p25'] = output_zonal_stats['b1_ref_p25'] - 100
    output_zonal_stats['b1_ref_p50'] = output_zonal_stats['b1_ref_p50'] - 100
    output_zonal_stats['b1_ref_p75'] = output_zonal_stats['b1_ref_p75'] - 100
    output_zonal_stats['b1_ref_p95'] = output_zonal_stats['b1_ref_p95'] - 100
    output_zonal_stats['b1_ref_p99'] = output_zonal_stats['b1_ref_p99'] - 100
    output_zonal_stats['b1_ref_range'] = output_zonal_stats['b1_ref_range'] - 100

    # band 2
    output_zonal_stats['b2_ref_min'] = output_zonal_stats['b2_ref_min'] - 100
    output_zonal_stats['b2_ref_max'] = output_zonal_stats['b2_ref_max'] - 100
    output_zonal_stats['b2_ref_mean'] = output_zonal_stats['b2_ref_mean'] - 100
    output_zonal_stats['b2_ref_med'] = output_zonal_stats['b2_ref_med'] - 100
    output_zonal_stats['b2_ref_p25'] = output_zonal_stats['b2_ref_p25'] - 100
    output_zonal_stats['b2_ref_p50'] = output_zonal_stats['b2_ref_p50'] - 100
    output_zonal_stats['b2_ref_p75'] = output_zonal_stats['b2_ref_p75'] - 100
    output_zonal_stats['b2_ref_p95'] = output_zonal_stats['b2_ref_p95'] - 100
    output_zonal_stats['b2_ref_p99'] = output_zonal_stats['b2_ref_p99'] - 100
    output_zonal_stats['b2_ref_range'] = output_zonal_stats['b2_ref_range'] - 100

    # band 3
    output_zonal_stats['b3_ref_min'] = output_zonal_stats['b3_ref_min'] - 100
    output_zonal_stats['b3_ref_max'] = output_zonal_stats['b3_ref_max'] - 100
    output_zonal_stats['b3_ref_mean'] = output_zonal_stats['b3_ref_mean'] - 100
    output_zonal_stats['b3_ref_med'] = output_zonal_stats['b3_ref_med'] - 100
    output_zonal_stats['b2_ref_p25'] = output_zonal_stats['b3_ref_p25'] - 100
    output_zonal_stats['b3_ref_p50'] = output_zonal_stats['b3_ref_p50'] - 100
    output_zonal_stats['b3_ref_p75'] = output_zonal_stats['b3_ref_p75'] - 100
    output_zonal_stats['b3_ref_p95'] = output_zonal_stats['b3_ref_p95'] - 100
    output_zonal_stats['b3_ref_p99'] = output_zonal_stats['b3_ref_p99'] - 100
    output_zonal_stats['b3_ref_range'] = output_zonal_stats['b3_ref_range'] - 100

    return output_zonal_stats


def run_zonal(image, band, no_data, shape, uid, ref_temp_dir_bands, band_lbl):
    # cleans the file pathway (Windows)

    print('band: ', band)
    print("band label: ", band_lbl)
    image_s = image.rstrip()
    im_name_s = image_s[
                -43:-1]  # May need to change these values depending on whether there is a 2 or 3 in the
    # name.
    im_name = im_name_s + 'g'
    # print('Image name: ', im_name)
    im_date = image_s[
              -27:-19]  # May need to change these values depending on whether there is a 2 or 3 in the
    # name.

    # loops through each image
    with rasterio.open(image_s, nodata=no_data) as srci:
        image_results = 'image_' + im_name + '.csv'

        # runs the zonal stats function and outputs a csv in a band specific folder
        final_results, site_name = apply_zonal_stats_fn(image_s, no_data, band, shape, uid)
        print("final_results: ", final_results)
        #
        # ['count', 'min', 'max', 'mean', 'median', 'std', 'percentile_25', 'percentile_50',
        #  'percentile_75', 'percentile_95', 'percentile_99', 'range']

        header = ["b" + str(band_lbl) + '_uid', "b" + str(band_lbl) + '_site',
                   "b" + str(band_lbl) + '_min', "b" + str(band_lbl) + '_max',
                  "b" + str(band_lbl) + '_mean', "b" + str(band_lbl) + '_count',

                   "b" + str(band_lbl) + '_median', "b" + str(band_lbl) + '_std',
                  "b" + str(band_lbl) + '_p25', "b" + str(band_lbl) + '_p50', "b" + str(band_lbl) + '_p75',
                  "b" + str(band_lbl) + '_p95', "b" + str(band_lbl) + '_p99', "b" + str(band_lbl) + '_range']

        df = pd.DataFrame.from_records(final_results, columns=header)
        df['band'] = band_lbl
        df['image'] = im_name
        df['date'] = im_date
        df.to_csv(ref_temp_dir_bands + '//band' + str(band_lbl) + '//' + image_results, index=False)

        print("df: ", df)
        print("saved to: ", ref_temp_dir_bands + '//band' + str(band_lbl) + '//' + image_results)

    return df, header


#
# def run_zonal_no_bands(image, band, no_data, shape, uid, ref_temp_dir_bands):
#     # cleans the file pathway (Windows)
#     image_s = image.rstrip()
#     im_name_s = image_s[
#                 -43:-1]  # May need to change these values depending on whether there is a 2 or 3 in the
#     # name.
#     im_name = im_name_s + 'g'
#     # print('Image name: ', im_name)
#     im_date = image_s[
#               -27:-19]  # May need to change these values depending on whether there is a 2 or 3 in the
#     # name.
#
#     # loops through each image
#     with rasterio.open(image_s, nodata=no_data) as srci:
#         image_results = 'image_' + im_name + '.csv'
#
#         # runs the zonal stats function and outputs a csv in a band specific folder
#         final_results, site_name = apply_zonal_stats_fn(image_s, no_data, band, shape, uid)
#         print("final_results: ", final_results)
#         #
#         # ['count', 'min', 'max', 'mean', 'median', 'std', 'percentile_25', 'percentile_50',
#         #  'percentile_75', 'percentile_95', 'percentile_99', 'range']
#
#         dict_ = {"b" + str(band) + '_uid': np.nan, "b" + str(band) + '_site': np.nan , "b" + str(band) + '_count': np.nan ,
#         "b" + str(band) + '_min': np.nan , "b" + str(band) + '_max': np.nan ,
#         "b" + str(band) + '_mean': np.nan , "b" + str(band) + '_median': np.nan , "b" + str(band) + '_std': np.nan,
#         "b" + str(band) + '_p25': np.nan, "b" + str(band) + '_p50': np.nan, "b" + str(band) + '_p75': np.nan,
#         "b" + str(band) + '_p95': np.nan, "b" + str(band) + '_p99': np.nan, "b" + str(band) + '_range': np.nan}
#
#
#
#         df = pd.DataFrame.from_records(final_results, columns=header)
#         df['band'] = band
#         df['image'] = im_name
#         df['date'] = im_date
#         df.to_csv(ref_temp_dir_bands + '//band' + str(band) + '//' + image_results, index=False)
#
#     return df, header

def create_dir(dir_path, variable):
    # create temporary folders
    dir_ = os.path.join(dir_path, variable)
    os.makedirs(dir_)

    return dir_

def removel7t5_bands_fn(df):

    # define sensor type as a column
    sensor_list = []
    for i in df.ref_image.tolist():
        print(i)
        sensor = i[-44:-37]
        print(sensor)
        sensor_list.append(sensor)

    df['sensor'] = sensor_list
    
    # filter based on sensors
    df5 = df[df['sensor'] == 'l5tmre']
    df7 = df[df['sensor'] == 'l7tmre']
    df_other = df[(df["sensor"] != "l5tmre") & (df["sensor"] != "l7tmre")]

    # extract column list containing b9, b8 and b7
    col_list = []
    b9_cols = [col for col in df5.columns if 'b9' in col]
    b8_cols = [col for col in df5.columns if 'b8_' in col]
    b7_cols = [col for col in df5.columns if 'b7_' in col]
    col_list.extend(b9_cols)
    col_list.extend(b8_cols)
    col_list.extend(b7_cols)

    # replace column values with no_data value
    for i in col_list:
        df5[i] = 9999
        df7[i] = 9999

    df_final = pd.concat([df5, df7, df_other])

    return df_final


def main_routine(temp_dir_path, zonal_stats_ready_dir, no_data, tile, zonal_stats_output):
    """Restructure ODK 1ha geo-DataFrame to calculate the zonal statistics for each 1ha site per Landsat Fractional
    Cover image, per band (b1, b2 and b3). Concatenate and clean final output DataFrame and export to the Export
    directory/zonal stats."""

    no_data = 0
    # print('step1_6_dil_zonal_stats.py INITIATED.'

    # strip Landsat tile label from csv file name.
    print("tile: ", tile)
    tile_begin = tile[-33:-30]
    print("begin_tile: ", tile_begin)
    tile_end = tile[-29:-26]
    print("end_tile: ", tile_end)
    complete_tile = tile_begin + tile_end
    print('=' * 50)
    print('Working on tile: ', complete_tile)
    print('=' * 50)
    print('......')

    shapefile = os.path.join(zonal_stats_ready_dir, "{0}_by_tile.shp".format(complete_tile))
    df = gpd.read_file(shapefile)

    shape = shapefile
    # nodata = int(0)
    uid = 'uid'
    im_list = tile

    # specify the number of bands that zonal stats will be derived from (default is three -GDAL numbering)
    num_bands = [1, 2, 3, 4, 5, 6, 7] #, 8, 9]

    # create band temp directories
    dir_ = create_dir(temp_dir_path, 'ref_temp_individual_bands')

    for i in num_bands:
        dir_band = create_dir(dir_, 'band' + str(i))

    for band in num_bands:
        # open the list of imagery and read it into memory and call the apply_zonal_stats_fn function
        with open(im_list, 'r') as imagery_list:

            # Extract each image path from the image list
            for image in imagery_list:

                print(image)
                print("searching for: ", image[-44:-38])
                if image[-44:-38] == 'l5tmre':
                    if band == 9:
                        band_num = 1
                        band_lbl = 9
                        run_zonal(image, band_num, no_data, shape, uid, dir_, band_lbl)

                    elif band == 8:
                        band_num = 1
                        band_lbl = 8
                        run_zonal(image, band_num, no_data, shape, uid, dir_, band_lbl)

                    elif band == 7:
                        band_num = 1
                        band_lbl = 7
                        run_zonal(image, band_num, no_data, shape, uid, dir_, band_lbl)

                    else:
                        print("continue")
                        band_lbl = band
                        band_num = band
                        run_zonal(image, band_num, no_data, shape, uid, dir_, band_lbl)

                elif image[-44:-38] == 'l7tmre':
                    if band == 9:
                        band_num = 1
                        band_lbl = 9
                        run_zonal(image, band_num, no_data, shape, uid, dir_, band_lbl)

                    elif band == 8:
                            band_num = 1
                            band_lbl = 8
                            run_zonal(image, band_num, no_data, shape, uid, dir_, band_lbl)

                    elif band == 7:
                        band_num = 1
                        band_lbl = 7
                        run_zonal(image, band_num, no_data, shape, uid, dir_, band_lbl)

                    else:
                        print('continue')
                        band_lbl = band
                        band_num = band
                        run_zonal(image, band_num, no_data, shape, uid, dir_, band_lbl)
                else:
                    print("continue")
                    band_lbl = band
                    band_num = band
                    run_zonal(image, band_num, no_data, shape, uid, dir_, band_lbl)

    # -------------------------------------------------- Concatenate csv -----------------------------------------------

    print("all bands should have been collected")
    # for loops through the band folders and concatenates zonal stat outputs into a complete band specific csv
    for x in num_bands:
        location_output = dir_ + '//band' + str(x)
        band_files = glob.glob(os.path.join(location_output,
                                            '*.csv'))

        # advisable to use os.path.join as this makes concatenation OS independent
        df_from_each_band_file = (pd.read_csv(f) for f in band_files)
        concat_band_df = pd.concat(df_from_each_band_file, ignore_index=False, axis=0, sort=False)
        # export the band specific results to a csv file (i.e. three outputs)
        print("output csv to: ", dir_ + '//' + 'Band' + str(x) + '.csv')
        concat_band_df.to_csv(dir_ + '//' + 'Band' + str(x) + '.csv', index=False)

    # ----------------------------------------- Concatenate three bands together ---------------------------------------



    # Concatenate Three bands
    header_all = ['uid', 'site', 'b1_ref_min', 'b1_ref_max', 'b1_ref_mean', 'b1_ref_count',
                  'b1_ref_med', 'b1_ref_std', 'b1_ref_p25', 'b1_ref_p50', 'b1_ref_p75', 'b1_ref_p95', 'b1_ref_p99',
                  'b1_ref_range', 'band', 'ref_image', 'date',

                  'b2_uid', 'b2_site',  'b2_ref_min', 'b2_ref_max', 'b2_ref_mean', 'b2_ref_count',
                  'b2_ref_med', 'b2_ref_std', 'b2_ref_p25', 'b2_ref_p50', 'b2_ref_p75', 'b2_ref_p95', 'b2_ref_p99',
                  'b2_ref_range', 'b2_ref_band', 'b2_ref_im', 'b2_ref_date',

                  'b3_uid', 'b3_site',  'b3_ref_min', 'b3_ref_max', 'b3_ref_mean', 'b3_ref_count',
                  'b3_ref_med', 'b3_ref_std', 'b3_ref_p25', 'b3_ref_p50', 'b3_ref_p75', 'b3_ref_p95', 'b3_ref_p99',
                  'b3_ref_range', 'b3_ref_band', 'b3_ref_im', 'b3_ref_date',

                  'b4_uid', 'b4_site',  'b4_ref_min', 'b4_ref_max', 'b4_ref_mean', 'b4_ref_count',
                  'b4_ref_med', 'b4_ref_std', 'b4_ref_p25', 'b4_ref_p50', 'b4_ref_p75', 'b4_ref_p95', 'b4_ref_p99',
                  'b4_ref_range', 'b4_ref_band', 'b4_ref_im', 'b4_ref_date',

                  'b5_uid', 'b5_site',  'b5_ref_min', 'b5_ref_max', 'b5_ref_mean', 'b5_ref_count',
                  'b5_ref_med', 'b5_ref_std', 'b5_ref_p25', 'b5_ref_p50', 'b5_ref_p75', 'b5_ref_p95', 'b5_ref_p99',
                  'b5_ref_range', 'b5_ref_band', 'b5_ref_im', 'b5_ref_date',

                  'b6_uid', 'b6_site',  'b6_ref_min', 'b6_ref_max', 'b6_ref_mean', 'b6_ref_count',
                  'b6_ref_med', 'b6_ref_std', 'b6_ref_p25', 'b6_ref_p50', 'b6_ref_p75', 'b6_ref_p95', 'b6_ref_p99',
                  'b6_ref_range', 'b6_ref_band', 'b6_ref_im', 'b6_ref_date',

                  'b7_uid', 'b7_site',  'b7_ref_min', 'b7_ref_max', 'b7_ref_mean', 'b7_ref_count',
                  'b7_ref_med', 'b7_ref_std', 'b7_ref_p25', 'b7_ref_p50', 'b7_ref_p75', 'b7_ref_p95', 'b7_ref_p99',
                  'b7_ref_range', 'b7_ref_band', 'b7_ref_im', 'b7_ref_date',

                  # 'b8_uid', 'b8_site',  'b8_ref_min', 'b8_ref_max', 'b8_ref_mean', 'b8_ref_count',
                  # 'b8_ref_med', 'b8_ref_std', 'b8_ref_p25', 'b8_ref_p50', 'b8_ref_p75', 'b8_ref_p95', 'b8_ref_p99',
                  # 'b8_ref_range', 'b8_ref_band', 'b8_ref_im', 'b8_ref_date',
                  #
                  # 'b9_uid', 'b9_site', 'b9_ref_min', 'b9_ref_max', 'b9_ref_mean', 'b9_ref_count',
                  # 'b9_ref_med', 'b9_ref_std', 'b9_ref_p25', 'b9_ref_p50', 'b9_ref_p75', 'b9_ref_p95', 'b9_ref_p99',
                  # 'b9_ref_range', 'b9_ref_band', 'b9_ref_im', 'b9_ref_date'
                  ]

    all_files = glob.glob(os.path.join(dir_,
                                       '*.csv'))
    # advisable to use os.path.join as this makes concatenation OS independent
    df_from_each_file = (pd.read_csv(f) for f in all_files)
    output_zonal_stats = pd.concat(df_from_each_file, ignore_index=False, axis=1, sort=False)
    print("-" * 50)
    print(output_zonal_stats.shape)
    print(output_zonal_stats.columns)

    output_zonal_stats.columns = header_all

    # -------------------------------------------------- Clean dataframe -----------------------------------------------
    # output_zonal_stats.to_csv(r"Z:\Scratch\Rob\output_zonal_stats2.csv")
    # Convert the date to a time stamp
    time_stamp_fn(output_zonal_stats)

    # remove 100 from zone_stats
    # landsat_correction_fn(output_zonal_stats)

    output_zonal_stats = removel7t5_bands_fn(output_zonal_stats)

    # reshape the final dataframe
    output_zonal_stats = output_zonal_stats[
        ['uid', 'site', 'ref_image', 'year', 'month', 'day',
         'b1_ref_count', 'b1_ref_min',
         'b1_ref_max', 'b1_ref_mean', 'b1_ref_med', 'b1_ref_std',
         'b1_ref_p25', 'b1_ref_p50', 'b1_ref_p75', 'b1_ref_p95', 'b1_ref_p99', 'b1_ref_range',

         'b2_ref_count', 'b2_ref_min', 'b2_ref_max', 'b2_ref_mean', 'b2_ref_med', 'b2_ref_std',
         'b2_ref_p25', 'b2_ref_p50', 'b2_ref_p75', 'b2_ref_p95', 'b2_ref_p99',
         'b2_ref_range',

         'b3_ref_count', 'b3_ref_min', 'b3_ref_max', 'b3_ref_mean', 'b3_ref_med',
         'b3_ref_std', 'b3_ref_p25', 'b3_ref_p50', 'b3_ref_p75', 'b3_ref_p95', 'b3_ref_p99', 'b3_ref_range',

         'b4_ref_count', 'b4_ref_min', 'b4_ref_max', 'b4_ref_mean', 'b4_ref_med',
         'b4_ref_std', 'b4_ref_p25', 'b4_ref_p50', 'b4_ref_p75', 'b4_ref_p95', 'b4_ref_p99', 'b4_ref_range',

         'b5_ref_count', 'b5_ref_min', 'b5_ref_max', 'b5_ref_mean', 'b5_ref_med',
         'b5_ref_std', 'b5_ref_p25', 'b5_ref_p50', 'b5_ref_p75', 'b5_ref_p95', 'b5_ref_p99', 'b5_ref_range',

         'b6_ref_count', 'b6_ref_min', 'b6_ref_max', 'b6_ref_mean', 'b6_ref_med',
         'b6_ref_std', 'b6_ref_p25', 'b6_ref_p50', 'b6_ref_p75', 'b6_ref_p95', 'b6_ref_p99', 'b6_ref_range',

         'b7_ref_count', 'b7_ref_min', 'b7_ref_max', 'b7_ref_mean', 'b7_ref_med',
         'b7_ref_std', 'b7_ref_p25', 'b7_ref_p50', 'b7_ref_p75', 'b7_ref_p95', 'b7_ref_p99', 'b7_ref_range',

         # 'b8_ref_count', 'b8_ref_min', 'b8_ref_max', 'b8_ref_mean', 'b8_ref_med',
         # 'b8_ref_std', 'b8_ref_p25', 'b8_ref_p50', 'b8_ref_p75', 'b8_ref_p95', 'b8_ref_p99', 'b8_ref_range',
         #
         # 'b9_ref_count', 'b9_ref_min', 'b9_ref_max', 'b9_ref_mean', 'b9_ref_med',
         # 'b9_ref_std', 'b9_ref_p25', 'b9_ref_p50', 'b9_ref_p75', 'b9_ref_p95', 'b9_ref_p99', 'b9_ref_range',
         ]]

    site_list = output_zonal_stats.site.unique().tolist()
    print("length of site list: ", len(site_list))
    if len(site_list) >= 1:
        for i in site_list:
            out_df = output_zonal_stats[output_zonal_stats['site'] == i]

            out_path = os.path.join(zonal_stats_output, "{0}_{1}_ref_zonal_stats.csv".format(str(i), complete_tile))
            # export the pandas df to a csv file
            out_df.to_csv(out_path, index=False)


    else:
        out_path = os.path.join(zonal_stats_output,
                                "{0}_{1}_ref_zonal_stats.csv".format(str(site_list[0]), complete_tile))
        # export the pandas df to a csv file
        output_zonal_stats.to_csv(out_path, index=False)

    # ----------------------------------------------- Delete temporary files -------------------------------------------
    # remove the temp dir and single band csv files
    shutil.rmtree(dir_)

    print('=' * 50)

    return output_zonal_stats, complete_tile, tile, dir_


if __name__ == '__main__':
    main_routine()
