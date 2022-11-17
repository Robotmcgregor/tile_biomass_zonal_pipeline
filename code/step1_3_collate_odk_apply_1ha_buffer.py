#!/usr/bin/env python

'''
step1_3_collate_odk_apply_1ha_buffer.py
=============================================

Description: This script searches through a directory for all ODK output csv files.
Once located, all odk outputs are concatenated, based on their type(i.e integrated / RAS) and outputted to the
export_dir_path (odk_int_output.csv and odk_ras_output.csv) and (odk_int_output.shp and odk_ras_output.shp
(geographic (GCS_GDA_1994)).
Additionally, the attributes are removed excluding {'PROP_NAME', property name
(i.e. Nuthill Downs : 'SITE_NAME', site name (i.e. NTH01A) : 'DATE', DATETIME (i.e. 14/07/2020 9:35:00 AM)}
for data configuration / consistency and the integrated and RAS data is concatenated and output as a .shp
(odk_all_output.shp)

The script also re-projects the odk_all_output.shp to WGS_1984_UTM_Zone_52S and WGS_1984_UTM_Zone_53S, and a 1ha square
buffer is applied to each site, culminating in the output of two shapefiles compGeoDF_1ha_WHS84z52.shp and
compGeoDF_1ha_WHS84z53.shp.

All integrated and RAS outputs and exports a csv and a projected shapefiles (WGS52 and 53).
This script also applies a 1ha square buffer to each site and outputs a csv, projected shapefiles and a complete
(cleaned) shapefile for executing step1_4_landsat_tile_grid_identify.py.

Note: Ras assessment have been turned off.

Author: Rob McGregor
email: Robert.Mcgregor@nt.gov.au
Date: 27/10/2020
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


========================================================================================================
'''

# Import modules
from __future__ import print_function, division
import os
from shapely.geometry import Point
import geopandas as gpd
from geopandas import GeoDataFrame
import pandas as pd
import glob
import sys

import warnings

warnings.filterwarnings("ignore")


def os_walk_odk_fn(directory_odk):
    """ Walks through the ODK output directory and appends file paths to one of two lists.

    @param directory_odk: string object containing the path to the property odk paths (--directory_odk).
    @return list_csv: list object containing all located ras output file paths.
    @return list_integrated: list object containing all located integrated star transect output file paths.
    """

    list_csv = []
    list_site_names = []

    for root, dirs, files in os.walk(directory_odk):

        for file in files:
            print('file: ', file)

            if file.endswith('c_bio_site_totals_v4_edit.csv'):
                # Separate integrated/Tier 1 outputs
                #site_name = extract_site_fn(file)
                csv_file = (os.path.join(root, file))
                print("located: ", csv_file)
                list_csv.append(csv_file)
                #list_site_names.append(site_name)

            else:
                pass

    print('The following csv files were identified and processing has been initiated.')

    print(' - Number of allometry biomass files: ', len(list_csv))
    print('list_csv: ', list_csv)

    return list_csv, #list_site_names


def extract_site_fn(file_name):
    """ Extract the site name from the csv file name (assuming site name is before the first underscore).

    :param file_name: string object containing the csv file name
    :return: site_name: string object containing the site name (head of string before first underscore).
    """
    # split the string and extract the head of the string
    site_name = file_name.split("_", 1)[0]

    return site_name


def concatenate_df_list(list_input): #, list_name):
    """ Concatenate ODK csv outputs into a Pandas DataFrame.

    @param list_input: list object containing all located allometry biomass file paths.
    @return output_df: Pandas dataframe containing the concatenated csv files from the input list.
    """

    list_df = []
    # list_length = len(list_input)

    # for i in list_input:# ,  list_name):
    #     df = pd.read_csv(i)
    #
    #     #df.insert(0, "site", name, allow_duplicates=False)
    #     # print(df)
    #
    #     list_df.append(df)
    # # Concatenate list of DataFrames into a single DataFrame.
    output_df = pd.concat(list_df)

    return output_df


def single_csv_fn(list_input): #, list_name):
    """ Create a Pandas DataFrame from a list with only one list element (csv path).

    @param list_input: list object containing all located integrated star transect OR RAS output file paths.
    @return df: Pandas dataframe containing the concatenated csv files from the input list
    """
    for i in list_input: #, list_name):
        print('148: ', i)
        df = pd.read_csv(i)
        # df.insert(0, "site", name, allow_duplicates=False)

    return df


def projection_file_name_fn(epsg, allometry_biomass_gdf):
    """ Project a geo-dataframe with the input epsg param and return several crs specific string and integer outputs.

    @param epsg: integer object containing required crs for the current geo-dataframe.
    @param allometry_biomass_gdf: geo-dataframe object which is to be re-projected.
    @return crs_name: string object containing the output crs in a standardised file naming convention.
    @return crs_output: dictionary object containing crs information used for older versions of GDAL.
    @return projected_df: geo-dataframe object projected to the input crs.
    """
    epsg_int = int(epsg)
    if epsg_int == 28352:
        crs_name = 'GDA94z52'
        crs_output = {'init': 'EPSG:28352'}
    elif epsg_int == 28353:
        crs_name = 'GDA94z53'
        crs_output = {'init': 'EPSG:28353'}
    elif epsg_int == 4283:
        crs_name = 'GDA94'
        crs_output = {'init': 'EPSG:4283'}
    elif epsg_int == 32752:
        crs_name = 'WGS84z52'
        crs_output = {'init': 'EPSG:32752'}
    elif epsg_int == 32753:
        crs_name = 'WGS84z53'
        crs_output = {'init': 'EPSG:32753'}
    elif epsg_int == 3577:
        crs_name = 'Albers'
        crs_output = {'init': 'EPSG:3577'}
    elif epsg_int == 4326:
        crs_name = 'GCS_WGS84'
        crs_output = {'init': 'EPSG:4326'}
    else:
        crs_name = 'not_defined'
        new_dict = {'init': 'EPSG:' + str(epsg_int)}
        crs_output = new_dict

    # Project DF to epsg value
    projected_df = allometry_biomass_gdf.to_crs(epsg)

    return crs_name, crs_output, projected_df


def square_buffer_fn(projected_df, prime_temp_buffer_dir, crs_name):
    """ Separate each point, apply a 1ha square buffer and export shapefiles.

    @param projected_df: Pandas dataframe in the relevant projection (WGSz52 or WGSz53).
    @param prime_temp_buffer_dir: directory to the temporary sub-directory (temp_1ha_buffer).
    @param crs_name: string object containing the crs name for file naming.
    @return buffer_temp_dir: string object containing the path to the final output subdirectory titled after the crs name.
    """

    buffer_temp_dir = os.path.join(prime_temp_buffer_dir, 'sites_1ha' + crs_name)
    if not os.path.exists(buffer_temp_dir):
        os.makedirs(buffer_temp_dir)
    else:
        pass

    # print(projected_df)
    for i in projected_df.site.unique():
        projected_df2 = projected_df.loc[projected_df.site == i]
        single_site = projected_df2.head(1)
        # print("single_site: ", single_site)

        projected_df3 = single_site.buffer(50, cap_style=3)

        export_file = os.path.join(buffer_temp_dir, "{0}_1ha_{1}.shp".format(i, crs_name))
        # print("1ha buffer: ", export_file)
        # print(projected_df3.shape)
        projected_df3.to_file(export_file, driver="ESRI Shapefile")

    return buffer_temp_dir


def add_site_attribute_fn(prime_temp_buffer_dir, buffer_temp_dir, crs_name):
    """ Retrieve file path for each 1ha shapefiles and add SITE_NAME and PROP_CODE attributes.

    @param prime_temp_buffer_dir: string object containing the path to a sub-directory within the temporary directory.
    @param buffer_temp_dir: string object containing the path to the sub-directory containing the 1ha site shapefiles.
    @param crs_name: string object containing the crs name for file naming.
    @return prime_temp_buffer_dir: string object containing the path to a sub-directory within the temporary directory.
    """

    # Create a string path to a sub-directory
    attribute_temp_dir = os.path.join(prime_temp_buffer_dir, '1ha_attribute', crs_name)
    # print("attribute_temp_dir: ", attribute_temp_dir)
    # Check if the sub-directory already exists and create if if does not.
    if not os.path.exists(attribute_temp_dir):
        os.makedirs(attribute_temp_dir)

    # search for shapefiles that meet the search criteria
    for root, dirs, files in os.walk(buffer_temp_dir):
        for file in files:
            # print("file", file)
            ends_with = '.shp'
            if file.endswith(ends_with):
                # print("file ends with ", ends_with)
                # split file name
                list_file_variables = file.split('_')
                # print(list_file_variables)
                site = list_file_variables[0]

                shp = os.path.join(root, file)
                geo_df = gpd.read_file(shp, driver="ESRI Shapefile")

                # add required attributes to the geo-dataframe from previously defined variables.
                geo_df.insert(1, 'site_name', str(site))
                # print(geo_df.columns)
                # export finalised geo-dataframe as a shapefile.
                export_file = os.path.join(attribute_temp_dir, "{0}_1ha_attrib_{1}.shp".format(str(site), crs_name))
                # print("export_file: ", export_file)
                geo_df.to_file(export_file, driver="ESRI Shapefile")

    return prime_temp_buffer_dir


def concatenate_df_fn(prime_temp_buffer_dir, export_dir_path, crs_name):
    """  Concatenate attributed shapefiles and export completed shapefile.

    @param prime_temp_buffer_dir: string object containing the path to a sub-directory within the temporary directory.
    @param export_dir_path: string object containing the path to the export directory.
    @param crs_name: string object containing the standardised crs information to be used as part of the file/sub-dir.
    @return comp_geo_df: geo-dataframe created by the concatenation of all shapefiles located in the specified directory.
    @return crs_name: string object containing the standardised crs information to be used as part of the file/sub-dir.
    """

    list_df2 = []
    search_term = '*.shp'

    for file in glob.glob(prime_temp_buffer_dir + '\\1ha_attribute\\' + crs_name + '\\' + search_term):
        geo_df = gpd.read_file(file)
        list_df2.append(geo_df)

    if len(list_df2) >= 1:

        comp_geo_df = gpd.GeoDataFrame(pd.concat(list_df2, ignore_index=True), crs=list_df2[0].crs)
        comp_geo_df.to_file(export_dir_path + '\\comp_geo_df_1ha_' + crs_name + '.shp')

    else:

        print('There are no shapefiles to concatenate: ', crs_name)
        sys.exit()
        print('There are no shapefiles to concatenate: ', crs_name)
        comp_geo_df = None

    return comp_geo_df, crs_name


def prop_code_extraction_fn(prop, pastoral_estate):
    """ Extract the property tag from the Pastoral Estate shapefile using the property name.

    @param prop: string object containing the current property name.
    @param pastoral_estate: geo-dataframe object created from the Pastoral Estate shapefile
    @return prop_code: string object extracted from the Pastoral Estate based on the property name.
    """
    property_list = pastoral_estate.PROPERTY.tolist()

    prop_upper = prop.upper().replace('_', ' ')
    if prop_upper in property_list:

        prop_code = pastoral_estate.loc[pastoral_estate['PROPERTY'] == prop_upper, 'PROP_TAG'].iloc[0]
    else:
        prop_code = ''

    return prop_code


def main_routine(data, export_dir_path, prime_temp_buffer_dir):
    # ------------------------------------------- ODK csv collation --------------------------------------------------

    # Call the os_walk_odk_fn function to append all csv files with the required search criteria into one of two lists
    # list_integrated or list_ras depending on the type of site.
    # list_input = os_walk_odk_fn(directory_odk)
    # print("returned: ", list_input[0])

    list_input = [data]

    if len(list_input) >= 2:

        print("df has more than 2 records")

        # Call the concatenate_df_list_fn function - input = list_input - output = integrated_df.

        allometry_biomass_df = concatenate_df_list(list_input) #, list_site_name)

        # print(allometry_biomass_df.columns)
        file_output = os.path.join(export_dir_path, 'allometry_biomass_tower_data_multi.csv')
        allometry_biomass_df.to_csv(file_output)

        # ------------------------------------------- Convert to geo-DataFrame -----------------------------------------

        # Create a geometry column to convert the df into a geo_df.
        geometry = [Point(xy) for xy in zip(allometry_biomass_df.longitude, allometry_biomass_df.latitude)]

        allometry_biomass_gdf = GeoDataFrame(allometry_biomass_df, crs='EPSG:4283', geometry=geometry)

        # Export shapefile.
        file_export = os.path.join(export_dir_path, 'allometry_biomass_output.shp')
        # print("shapefile exported: ", file_export)
        allometry_biomass_gdf.to_file(file_export, driver='ESRI Shapefile')

        # print("shapefile exported: ", file_export)

        # ----------------------------------------------- EPSG: 32752 --------------------------------------------------
        # set epsg to WGSz52.
        epsg = 32752

        # Project allometry_biomass_gdf to WGSz52.
        crs_name, crs_output, projected_df = projection_file_name_fn(epsg, allometry_biomass_gdf)

        # Apply a 1ha square buffer to each point.
        buffer_temp_dir = square_buffer_fn(projected_df, prime_temp_buffer_dir, crs_name)

        # Add attributes (SITE_NAME and PROP_CODE) to geo-DataFrame.
        prime_temp_buffer_dir = add_site_attribute_fn(prime_temp_buffer_dir, buffer_temp_dir, crs_name)

        # ------------------------------------------------ EPSG: 32753 -------------------------------------------------

        # set epsg to WGSz52
        epsg = 32753

        # Project allometry_biomass_gdf to WGSz52.
        crs_name, crs_output, projected_df = projection_file_name_fn(epsg, allometry_biomass_gdf)

        # Apply a 1ha square buffer to each point.
        buffer_temp_dir = square_buffer_fn(projected_df, prime_temp_buffer_dir, crs_name)

        # Add attributes (SITE_NAME and PROP_CODE) to geo-DataFrame.
        prime_temp_buffer_dir = add_site_attribute_fn(prime_temp_buffer_dir, buffer_temp_dir, crs_name)

    elif len(list_input) == 1:

        print("list_input[0]: ", list_input[0])
        csv_file = list_input[0]
        allometry_biomass_df = pd.read_csv(csv_file[0])

        #allometry_biomass_df = single_csv_fn(list_input) #, list_site_name)

        print(allometry_biomass_df.columns)

        file_output = os.path.join(export_dir_path, 'allometry_biomass_data_single.csv')
        allometry_biomass_df.to_csv(file_output)

        # -------------------------------------- Convert to geo-DataFrame ----------------------------------------------

        # Create a geometry column to convert the df into a geo_df.
        geometry = [Point(xy) for xy in zip(allometry_biomass_df.lon_gda94, allometry_biomass_df.lat_gda94)]

        allometry_biomass_gdf = GeoDataFrame(allometry_biomass_df, crs='EPSG:4283', geometry=geometry)

        # Export shapefile.
        file_export = os.path.join(export_dir_path, 'allometry_biomass_output.shp')
        # print("shapefile exported: ", file_export)
        allometry_biomass_gdf.to_file(file_export, driver='ESRI Shapefile')

        # print("shapefile exported: ", file_export)

        # ---------------------------------------------- EPSG: 32752 ---------------------------------------------------
        # set epsg to WGSz52.
        epsg = 32752

        # Project allometry_biomass_gdf to WGSz52.
        crs_name, crs_output, projected_df = projection_file_name_fn(epsg, allometry_biomass_gdf)

        # Apply a 1ha square buffer to each point.
        buffer_temp_dir = square_buffer_fn(projected_df, prime_temp_buffer_dir, crs_name)

        # Add attributes (SITE_NAME and PROP_CODE) to geo-DataFrame.
        prime_temp_buffer_dir = add_site_attribute_fn(prime_temp_buffer_dir, buffer_temp_dir, crs_name)

        # ------------------------------------------------ EPSG: 32753 -------------------------------------------------

        # set epsg to WGSz52
        epsg = 32753

        # Project allometry_biomass_gdf to WGSz52.
        crs_name, crs_output, projected_df = projection_file_name_fn(epsg, allometry_biomass_gdf)

        # Apply a 1ha square buffer to each point.
        buffer_temp_dir = square_buffer_fn(projected_df, prime_temp_buffer_dir, crs_name)

        # Add attributes (SITE_NAME and PROP_CODE) to geo-DataFrame.
        prime_temp_buffer_dir = add_site_attribute_fn(prime_temp_buffer_dir, buffer_temp_dir, crs_name)

    else:

        print('There are no integrated  sites to be processed.')

    # Concatenate, clean and export geo_df_52
    crs_name = 'WGS84z52'
    geo_df_52, crs_name_52 = concatenate_df_fn(prime_temp_buffer_dir, export_dir_path, crs_name)

    # Concatenate, clean and export geo_df_53
    crs_name = 'WGS84z53'
    geo_df_53, crs_name_53 = concatenate_df_fn(prime_temp_buffer_dir, export_dir_path, crs_name)

    print("here: ", type(geo_df_52))
    geo_df_52.to_file(r"Z:\Scratch\Rob\tern\tree_biomass_field_data\geo_df_52.shp", driver = "ESRI Shapefile")
    geo_df_53.to_file(r"Z:\Scratch\Rob\tern\tree_biomass_field_data\geo_df_53.shp", driver="ESRI Shapefile")


    return geo_df_52, crs_name_52, geo_df_53, crs_name_53


if __name__ == '__main__':
    main_routine()
