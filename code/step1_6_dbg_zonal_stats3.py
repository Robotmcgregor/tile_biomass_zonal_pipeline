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


def time_stamp_fn(output_zonal_stats):
    """Insert a timestamp into feature position 4, convert timestamp into year, month and day strings and append to
    dataframe.

    @param output_zonal_stats: dataframe object containing the Landsat tile Fractional Cover zonal stats
    @return output_zonal_stats: processed dataframe object containing the Landsat tile Fractional Cover zonal stats and
    updated features.
    """

    print(output_zonal_stats.date[:6])
    print(output_zonal_stats.date[6:])
    # Convert the date to a time stamp
    time_stamp_st = pd.to_datetime(output_zonal_stats.date[:6], format='%Y%m')
    time_stamp_end = pd.to_datetime(output_zonal_stats.date[6:], format='%Y%m')
    output_zonal_stats.insert(4, 'date_st', time_stamp_st)
    output_zonal_stats.insert(5, 'date_end', time_stamp_end)
    output_zonal_stats['year'] = output_zonal_stats['date'].map(lambda x: str(x)[:4])
    output_zonal_stats['month'] = output_zonal_stats['date'].map(lambda x: str(x)[4:6])
    output_zonal_stats['day'] = output_zonal_stats['date'].map(lambda x: str(x)[6:])

    return output_zonal_stats


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
            print("dbg - band", str(band), ": ", zs)
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

    print("init time stamp")
    # Convert the date to a time stamp
    time_stamp = pd.to_datetime(output_zonal_stats.date, format='%Y%m%d')
    output_zonal_stats.insert(4, 'time_stamp', time_stamp)
    output_zonal_stats['year'] = output_zonal_stats['date'].map(lambda x: str(x)[:4])
    output_zonal_stats['month'] = output_zonal_stats['date'].map(lambda x: str(x)[4:6])
    output_zonal_stats['day'] = output_zonal_stats['date'].map(lambda x: str(x)[6:])

    return output_zonal_stats




def landsat_correction_fn(output_zonal_stats, num_bands, var_):
    """ Replace specific 0 values with Null values and correct b1, b2 and b3 calculations
    (refer to Fractional Cover metadata)

    @param output_zonal_stats: dataframe object containing the Landsat tile Fractional Cover zonal stats.
    @return: processed dataframe object containing the Landsat tile Fractional Cover zonal stats and
    updated values.
    """
    print("var_: ", var_)
    for n in num_bands:
        i = str(n)
        output_zonal_stats['b{0}_{1}_min'.format(str(i), var_)] = \
            output_zonal_stats['b{0}_{1}_min'.format(str(i), var_)].replace(0, np.nan)

        output_zonal_stats['b{0}_{1}_min'.format(i, var_)] = output_zonal_stats['b{0}_{1}_min'.format(i, var_)] - 100
        output_zonal_stats['b{0}_{1}_max'.format(i, var_)] = output_zonal_stats['b{0}_{1}_max'.format(i, var_)] - 100
        output_zonal_stats['b{0}_{1}_mean'.format(i, var_)] = output_zonal_stats['b{0}_{1}_mean'.format(i, var_)] - 100
        output_zonal_stats['b{0}_{1}_med'.format(i, var_)] = output_zonal_stats['b{0}_{1}_med'.format(i, var_)] - 100
        output_zonal_stats['b{0}_{1}_p25'.format(i, var_)] = output_zonal_stats['b{0}_{1}_p25'.format(i, var_)] - 100
        output_zonal_stats['b{0}_{1}_p50'.format(i, var_)] = output_zonal_stats['b{0}_{1}_p50'.format(i, var_)] - 100
        output_zonal_stats['b{0}_{1}_p75'.format(i, var_)] = output_zonal_stats['b{0}_{1}_p75'.format(i, var_)] - 100
        output_zonal_stats['b{0}_{1}_p95'.format(i, var_)] = output_zonal_stats['b{0}_{1}_p95'.format(i, var_)] - 100
        output_zonal_stats['b{0}_{1}_p99'.format(i, var_)] = output_zonal_stats['b{0}_{1}_p99'.format(i, var_)] - 100
        #output_zonal_stats['b{0}_{1}_range'.format(i, var_)] = output_zonal_stats['b{0}_{1}_range'.format(i, var_)] - 100


    return output_zonal_stats


def main_routine(temp_dir_path, zonal_stats_ready_dir, no_data, tile, zonal_stats_output, shape, var_):
    """Restructure ODK 1ha geo-DataFrame to calculate the zonal statistics for each 1ha site per Landsat Fractional
    Cover image, per band (b1, b2 and b3). Concatenate and clean final output DataFrame and export to the Export
    directory/zonal stats."""

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

    # shapefile = os.path.join(zonal_stats_ready_dir, "{0}_by_tile.shp".format(complete_tile))
    # df = gpd.read_file(shapefile)
    #
    # shape = shapefile
    # nodata = int(0)
    uid = 'uid'
    im_list = tile

    # todo up to here

    # specify the number of bands that zonal stats will be derived from (default is three -GDAL numbering)
    num_bands = [1, 2, 3, 4, 5, 6]  # , 7, 8, 9]
    # create temporary folders
    dbg_temp_dir_bands = os.path.join(temp_dir_path, 'dbg_temp_individual_bands')
    os.makedirs(dbg_temp_dir_bands)

    for i in num_bands:
        band_dir = os.path.join(dbg_temp_dir_bands, 'band{0}'.format(str(i)))
        os.makedirs(band_dir)

    for band_ in num_bands:
        # open the list of imagery and read it into memory and call the apply_zonal_stats_fn function
        with open(im_list, 'r') as imagery_list:

            # Extract each image path from the image list
            for image in imagery_list:

                # cleans the file pathway (Windows)
                image_s = image.rstrip()
                path_, im_name = os.path.split(image_s)

                # Remove band 1 for L8 and L9
                if im_name[:1] == "l9olre" or im_name[:1] == "l8olre":
                    print("L8 or L9")
                    print("-" * 50)
                    band = band_ = 1
                else:
                    band = band_

                # print("im_name_s: ", im_name_s)
                print('Image name: ', im_name)

                image_name_split = im_name.split("_")

                if str(image_name_split[-3]).startswith("m"):
                    print("seasonal")
                    im_date = image_name_split[-3][1:]
                else:
                    print("single date")
                    im_date = image_name_split[-3]

                # im_date = image_name_split[-2][1:]
                # print("im_date: ", im_date)

                # loops through each image
                with rasterio.open(image_s, nodata=no_data) as srci:
                    image_results = 'image_' + im_name + '.csv'

                    # runs the zonal stats function and outputs a csv in a band specific folder
                    final_results, site_name = apply_zonal_stats_fn(image_s, no_data, band, shape, uid)
                    print("final_results: ", final_results)
                    #
                    # ['count', 'min', 'max', 'mean', 'median', 'std', 'percentile_25', 'percentile_50',
                    #  'percentile_75', 'percentile_95', 'percentile_99', 'range']

                    # header = ["b" + str(band) + '_dbg_uid', "b" + str(band) + '_dbg_site', "b" + str(band) + '_dbg_count',
                    #           "b" + str(band) + '_dbg_min', "b" + str(band) + '_dbg_max',
                    #           "b" + str(band) + '_dbg_mean',  "b" + str(band) + '_dbg_med', "b" + str(band) + '_dbg_std',
                    #           "b" + str(band) + '_dbg_p25', "b" + str(band) + '_dbg_p50', "b" + str(band) + '_dbg_p75',
                    #           "b" + str(band) + '_dbg_p95', "b" + str(band) + '_dbg_p99', "b" + str(band) + '_dbg_range']

                    header = ["b" + str(band) + '_uid', "b" + str(band) + '_site', "b" + str(band) + '_min',
                              "b" + str(band) + '_max', "b" + str(band) + '_mean', "b" + str(band) + '_count',
                              "b" + str(band) + '_std', "b" + str(band) + '_median', "b" + str(band) + '_range',
                              "b" + str(band) + '_p25', "b" + str(band) + '_p50', "b" + str(band) + '_p75',
                              "b" + str(band) + '_p95', "b" + str(band) + '_p99']

                    df = pd.DataFrame.from_records(final_results, columns=header)
                    df['band'] = band
                    df['image'] = im_name
                    df['date'] = im_date
                    df.to_csv(dbg_temp_dir_bands + '//band' + str(band) + '//' + image_results, index=False)

    # -------------------------------------------------- Concatenate csv -----------------------------------------------

    # for loops through the band folders and concatenates zonal stat outputs into a complete band specific csv
    for x in num_bands:
        location_output = dbg_temp_dir_bands + '//band' + str(x)
        band_files = glob.glob(os.path.join(location_output,
                                            '*.csv'))

        # advisable to use os.path.join as this makes concatenation OS independent
        df_from_each_band_file = (pd.read_csv(f) for f in band_files)
        concat_band_df = pd.concat(df_from_each_band_file, ignore_index=False, axis=0, sort=False)
        # export the band specific results to a csv file (i.e. three outputs)
        print("output csv to: ", dbg_temp_dir_bands + '//' + 'Band' + str(x) + '.csv')
        concat_band_df.to_csv(dbg_temp_dir_bands + '//' + 'Band' + str(x) + '.csv', index=False)

    # ----------------------------------------- Concatenate three bands together ---------------------------------------


    header_all = ['uid', 'site', 'b1_dbg_min', 'b1_dbg_max', 'b1_dbg_mean', 'b1_dbg_count',
                  'b1_dbg_std', 'b1_dbg_med', 'b1_dbg_range', 'b1_dbg_p25', 'b1_dbg_p50', 'b1_dbg_p75',
                  'b1_dbg_p95', 'b1_dbg_p99', 'band', 'image', 'date',

                  'b2_dbg_uid', 'b2_dbg_site', 'b2_dbg_min', 'b2_dbg_max', 'b2_dbg_mean', 'b2_dbg_count', 'b2_dbg_std',
                  'b2_dbg_med', 'b2_dbg_range', 'b2_dbg_p25', 'b2_dbg_p50', 'b2_dbg_p75',
                  'b2_dbg_p95', 'b2_dbg_p99', 'band2', 'image2', 'date2',

                  'b3_uid', 'b3_site', 'b3_dbg_min', 'b3_dbg_max', 'b3_dbg_mean',
                  'b3_dbg_count', 'b3_dbg_std', 'b3_dbg_med', 'b3_dbg_range', 'b3_dbg_p25', 'b3_dbg_p50', 'b3_dbg_p75',
                  'b3_dbg_p95', 'b3_dbg_p99', 'band3', 'image3', 'date3',

                  'b4_uid', 'b4_site', 'b4_dbg_min', 'b4_dbg_max', 'b4_dbg_mean',
                  'b4_dbg_count', 'b4_dbg_std', 'b4_dbg_med', 'b4_dbg_range', 'b4_dbg_p25', 'b4_dbg_p50', 'b4_dbg_p75',
                  'b4_dbg_p95', 'b4_dbg_p99', 'band3', 'image3', 'date3',

                  'b5_uid', 'b5_site', 'b5_dbg_min', 'b5_dbg_max', 'b5_dbg_mean',
                  'b5_dbg_count', 'b5_dbg_std', 'b5_dbg_med', 'b5_dbg_range', 'b5_dbg_p25', 'b5_dbg_p50', 'b5_dbg_p75',
                  'b5_dbg_p95', 'b5_dbg_p99', 'band3', 'image3', 'date3',

                  'b6_uid', 'b6_site', 'b6_dbg_min', 'b6_dbg_max', 'b6_dbg_mean',
                  'b6_dbg_count', 'b6_dbg_std', 'b6_dbg_med', 'b6_dbg_range', 'b6_dbg_p25', 'b6_dbg_p50', 'b6_dbg_p75',
                  'b6_dbg_p95', 'b6_dbg_p99', 'band3', 'image3', 'date3']

    # print("header length: ", len(header_all))

    all_files = glob.glob(os.path.join(dbg_temp_dir_bands,
                                       '*.csv'))
    # advisable to use os.path.join as this makes concatenation OS independent
    df_from_each_file = (pd.read_csv(f) for f in all_files)
    output_zonal_stats = pd.concat(df_from_each_file, ignore_index=False, axis=1, sort=False)
    # print("-"*50)
    # print(output_zonal_stats.shape)
    # print(output_zonal_stats.columns)
    # output_zonal_stats.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\non_rmb_fractional_cover_zonal_stats\six_band_test.csv")

    output_zonal_stats.columns = header_all
    # output_zonal_stats.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\non_rmb_fractional_cover_zonal_stats\six_band_test2.csv")
    # -------------------------------------------------- Clean dataframe -----------------------------------------------
    # output_zonal_stats.to_csv(r"Z:\Scratch\Rob\output_zonal_stats2.csv")
    # Convert the date to a time stamp
    output_zonal_stats = time_stamp_fn(output_zonal_stats)

    # remove 100 from zone_stats
    output_zonal_stats = landsat_correction_fn(output_zonal_stats, num_bands, var_)

    # reshape the final dataframe
    output_zonal_stats = output_zonal_stats[
        ['uid', 'site', 'image', 'year', 'month', 'day', 'b1_dbg_count', 'b1_dbg_min',
         'b1_dbg_max', 'b1_dbg_mean', 'b1_dbg_med', 'b1_dbg_std',
         'b1_dbg_p25', 'b1_dbg_p50', 'b1_dbg_p75', 'b1_dbg_p95', 'b1_dbg_p99', 'b1_dbg_range',
         'b2_dbg_count', 'b2_dbg_min', 'b2_dbg_max', 'b2_dbg_mean', 'b2_dbg_med', 'b2_dbg_std',
         'b2_dbg_p25', 'b2_dbg_p50', 'b2_dbg_p75', 'b2_dbg_p95', 'b2_dbg_p99',
         'b2_dbg_range', 'b3_dbg_count', 'b3_dbg_min', 'b3_dbg_max', 'b3_dbg_mean', 'b3_dbg_med',
         'b3_dbg_std', 'b3_dbg_p25', 'b3_dbg_p50', 'b3_dbg_p75', 'b3_dbg_p95', 'b3_dbg_p99', 'b3_dbg_range',

         'b4_dbg_count', 'b4_dbg_min', 'b4_dbg_max', 'b4_dbg_mean', 'b4_dbg_med',
         'b4_dbg_std', 'b4_dbg_p25', 'b4_dbg_p50', 'b4_dbg_p75', 'b4_dbg_p95', 'b4_dbg_p99', 'b4_dbg_range',

         'b5_dbg_count', 'b5_dbg_min', 'b5_dbg_max', 'b5_dbg_mean', 'b5_dbg_med',
         'b5_dbg_std', 'b5_dbg_p25', 'b5_dbg_p50', 'b5_dbg_p75', 'b5_dbg_p95', 'b5_dbg_p99', 'b5_dbg_range',

         'b6_dbg_count', 'b6_dbg_min', 'b6_dbg_max', 'b6_dbg_mean', 'b6_dbg_med',
         'b6_dbg_std', 'b6_dbg_p25', 'b6_dbg_p50', 'b6_dbg_p75', 'b6_dbg_p95', 'b6_dbg_p99', 'b6_dbg_range',
         ]]

    site_list = output_zonal_stats.site.unique().tolist()
    print("length of site list: ", len(site_list))
    if len(site_list) >= 1:
        for i in site_list:
            out_df = output_zonal_stats[output_zonal_stats['site'] == i]

            out_path = os.path.join(zonal_stats_output, "{0}_{1}_dbg_zonal_stats.csv".format(str(i), complete_tile))
            # export the pandas df to a csv file
            out_df.to_csv(out_path, index=False)


    else:
        out_path = os.path.join(zonal_stats_output,
                                "{0}_{1}_dbg_zonal_stats.csv".format(str(site_list[0]), complete_tile))
        # export the pandas df to a csv file
        output_zonal_stats.to_csv(out_path, index=False)

    # ----------------------------------------------- Delete temporary files -------------------------------------------
    # remove the temp dir and single band csv files
    shutil.rmtree(dbg_temp_dir_bands)

    print('=' * 50)

    return output_zonal_stats, complete_tile, tile, dbg_temp_dir_bands


if __name__ == '__main__':
    main_routine()
