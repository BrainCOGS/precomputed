#! /bin/env python

import os, sys
import glob
from concurrent.futures import ProcessPoolExecutor

import numpy as np
# from PIL import Image
import SimpleITK as sitk

from cloudvolume import CloudVolume
from cloudvolume.lib import mkdir, touch

import logging
import argparse
import time
import pickle

from taskqueue import LocalTaskQueue
import igneous.task_creation as tc

def make_info_file(volume_size,resolution,layer_dir,commit=True,atlas_name='allen'):
    """ 
    ---PURPOSE---
    Make the cloudvolume info file.
    ---INPUT---
    volume_size     [Nx,Ny,Nz] in voxels, e.g. [2160,2560,1271]
    pix_scale_nm    [size of x pix in nm,size of y pix in nm,size of z pix in nm], e.g. [5000,5000,10000]
    commit          if True, will write the info/provenance file to disk. 
                    if False, just creates it in memory
    atlas_name      'allen', 'paxinos','princeton'
    """
    if atlas_name == 'paxinos':
        info = CloudVolume.create_new_info(
        num_channels = 1,
        layer_type = 'image', # 'image' or 'segmentation'
        data_type = 'uint16', # 
        encoding = 'raw', # other options: 'jpeg', 'compressed_segmentation' (req. uint32 or uint64)
        resolution = resolution, # Size of X,Y,Z pixels in nanometers, 
        voxel_offset = [ 0, 0, 0 ], # values X,Y,Z values in voxels
        chunk_size = [1024,1,1024 ], 
        volume_size = volume_size, # X,Y,Z size in voxels
        )
    else:
        info = CloudVolume.create_new_info(
            num_channels = 1,
            layer_type = 'image', # 'image' or 'segmentation'
            data_type = 'uint16', # 
            encoding = 'raw', # other options: 'jpeg', 'compressed_segmentation' (req. uint32 or uint64)
            resolution = resolution, # Size of X,Y,Z pixels in nanometers, 
            voxel_offset = [ 0, 0, 0 ], # values X,Y,Z values in voxels
            chunk_size = [ 1024,1024,1 ], # rechunk of image X,Y,Z in voxels -- only used for downsampling task I think
            volume_size = volume_size, # X,Y,Z size in voxels
            )

    vol = CloudVolume(f'file://{layer_dir}', info=info)
    vol.provenance.description = "Test on spock for profiling precomputed creation"
    vol.provenance.owners = ['ahoag@princeton.edu'] # list of contact email addresses
    if commit:
        vol.commit_info() # generates info json file
        vol.commit_provenance() # generates provenance json file
        print("Created CloudVolume info file: ",vol.info_cloudpath)
    return vol

def process_slice(z):
    if os.path.exists(os.path.join(progress_dir, str(z))):
        print(f"Slice {z} already processed, skipping ")
        return
    if z >= z_dim:
        print("Index {z} >= z_dim of volume, skipping")
        return
    print('Processing slice z=',z)
    array = registered_vol[z].reshape((1,y_dim,x_dim)).T
    vol[:,:, z] = array
    touch(os.path.join(progress_dir, str(z)))
    return "success"

def process_paxinos_slice(y):
    if os.path.exists(os.path.join(progress_dir, str(y))):
        print(f"Slice {y} already processed, skipping ")
        return
    if y >= y_dim:
        print("Index {y} >= y_dim of volume, skipping")
        return
    print('Processing slice y=',y)
    array = registered_vol[:,y,:].reshape((z_dim,1,x_dim)).T
    vol[:,y,:] = array
    touch(os.path.join(progress_dir, str(y)))
    return "success"

if __name__ == "__main__":
    """ First command line arguments """
    step = sys.argv[1]
    viz_dir = sys.argv[2]
    """ Load param dictionary """
    param_file = viz_dir + '/precomputed_params.p'
    with open(param_file,'rb') as pkl_file:
        param_dict = pickle.load(pkl_file)
    slurmjobfactor = param_dict['slurmjobfactor']
    atlas_name = param_dict['atlas_name']
    registered_data_path = param_dict['registered_data_path']
    rawdata_subfolder = param_dict['rawdata_subfolder'] # needed to get path to registered vol for non registration channel
    channel_index = param_dict['channel_index'] # needed to get path to registered vol for non registration channel
    channel_index_padded = '0'*(2-len(str(channel_index)))+str(channel_index) # "01", e.g.

    lightsheet_channel_str = param_dict['lightsheet_channel_str']
    channel_name = param_dict['channel_name']

    layer_name = param_dict['layer_name']
    layer_dir = os.path.join(viz_dir,layer_name)
    mkdir(layer_dir)
    """ Make progress dir """
    progress_dir = mkdir(viz_dir + f'/progress_{layer_name}') # unlike os.mkdir doesn't crash on prexisting 

    """ Figure out path to registered volume.
    This depends on whether it was the registration channel
    or cell channel, e.g.
    """
    if lightsheet_channel_str == 'regch':
        path_to_registered_volume = os.path.join(
            registered_data_path,'result.1.tif')
    else:
        path_to_registered_volume = os.path.join(
            registered_data_path,rawdata_subfolder+f'_resized_ch{channel_index_padded}','result.tif')
    """ Figure out volume size in pixels and in nanometers """
    registered_vol = np.array(sitk.GetArrayFromImage(sitk.ReadImage(path_to_registered_volume)),
            dtype=np.uint16,order='F')
    z_dim,y_dim,x_dim = registered_vol.shape
    print(x_dim,y_dim,z_dim)

    # atlas is a fixed resolution, depending on which one it is
    if 'princeton' in atlas_name.lower():
        x_scale_nm, y_scale_nm, z_scale_nm = 20000,20000,20000
    elif 'allen' in atlas_name.lower():
        x_scale_nm, y_scale_nm, z_scale_nm = 25000,25000,25000
    elif 'paxinos' in atlas_name.lower():
        x_scale_nm, y_scale_nm, z_scale_nm = 10000,100000,10000 # 10x100x10 micron resolution 
    
    """ Handle the different steps """
    if step == 'step0':
        print("step 0")
        volume_size = (x_dim,y_dim,z_dim)
        resolution = (x_scale_nm,y_scale_nm,z_scale_nm)

        vol = make_info_file(volume_size=volume_size,
            layer_dir=layer_dir,
            resolution=resolution,
            atlas_name=atlas_name.lower())
    elif step == 'step1':
        print("step 1")
        vol = CloudVolume(f'file://{layer_dir}')
        if atlas_name == 'paxinos':
            done_files = set([ int(y) for y in os.listdir(progress_dir) ])
            all_files = set(range(vol.bounds.minpt.y, vol.bounds.maxpt.y)) 
            to_upload = [ int(y) for y in list(all_files.difference(done_files)) ]
        done_files = set([ int(z) for z in os.listdir(progress_dir)])
        all_files = set(range(vol.bounds.minpt.z,vol.bounds.maxpt.z))
        to_upload = [ int(z) for z in list(all_files.difference(done_files)) ]
        to_upload.sort()
        print("Have {len(to_upload)} slices remaining to upload",to_upload)
        if atlas_name == 'paxinos':
            with ProcessPoolExecutor(max_workers=4) as executor:
                for job in executor.map(process_paxinos_slice,to_upload):
                    try:
                        print(job)
                    except Exception as exc:
                        print(f'generated an exception: {exc}')
        else:
            with ProcessPoolExecutor(max_workers=4) as executor:
                for job in executor.map(process_slice,to_upload):
                    try:
                        print(job)
                    except Exception as exc:
                        print(f'generated an exception: {exc}')

