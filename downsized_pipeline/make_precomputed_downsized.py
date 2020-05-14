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

def make_info_file(volume_size,resolution,layer_dir,commit=True):
    """ 
    ---PURPOSE---
    Make the cloudvolume info file.
    ---INPUT---
    volume_size     [Nx,Ny,Nz] in voxels, e.g. [2160,2560,1271]
    pix_scale_nm    [size of x pix in nm,size of y pix in nm,size of z pix in nm], e.g. [5000,5000,10000]
    commit          if True, will write the info/provenance file to disk. 
                    if False, just creates it in memory
    """
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
    array = downsized_vol[z].reshape((1,y_dim,x_dim)).T
    vol[:,:, z] = array
    touch(os.path.join(progress_dir, str(z)))
    return "success"

if __name__ == "__main__":
    """ First command line arguments """
    step = sys.argv[1]
    viz_dir = sys.argv[2]
    """ Job id """
    """ Load param dictionary """
    param_file = viz_dir + '/precomputed_params.p'
    with open(param_file,'rb') as pkl_file:
        param_dict = pickle.load(pkl_file)
    downsized_data_path = param_dict['downsized_data_path']
    layer_name = param_dict['layer_name']
    channel_index = param_dict['channel_index']
    channel_index_padded = '0'*(2-len(str(channel_index)))+str(channel_index) # "01", e.g.

    rawdata_subfolder = param_dict['rawdata_subfolder']
    layer_dir = os.path.join(viz_dir,layer_name)
    """ Make progress dir """
    progress_dir = mkdir(viz_dir + f'/progress_{layer_name}') # unlike os.mkdir doesn't crash on prexisting 
    """ Figure out volume dimensions in pixels and in nanometers """
    path_to_downsized_volume = os.path.join(
            downsized_data_path,
            f'{rawdata_subfolder}_resized_ch{channel_index_padded}_resampledforelastix.tif')
    downsized_vol = np.array(sitk.GetArrayFromImage(sitk.ReadImage(path_to_downsized_volume)),
            dtype=np.uint16,order='F')
    z_dim,y_dim,x_dim = downsized_vol.shape
    print(x_dim,y_dim,z_dim)
    
    # downsized volume is a fixed resolution, 1.3x smaller than 
    # whatever atlas was used
    atlas_name = param_dict['atlas_name']
    if 'allen' in atlas_name.lower():
        iso_resolution = int(round(25000/1.3,1))
    elif 'princeton' in atlas_name.lower():
        iso_resolution = int(round(20000/1.3,1))
    x_scale_nm, y_scale_nm, z_scale_nm = iso_resolution,iso_resolution,iso_resolution
        
    """ Handle the different steps """
    if step == 'step0':
        print("step 0")
        volume_size = (x_dim,y_dim,z_dim)
        resolution = (x_scale_nm,y_scale_nm,z_scale_nm)
        vol = make_info_file(volume_size=volume_size,layer_dir=layer_dir,resolution=resolution)
    elif step == 'step1':
        print("step 1")
        vol = CloudVolume(f'file://{layer_dir}')
        
        done_files = set([ int(z) for z in os.listdir(progress_dir)])
        all_files = set(range(vol.bounds.minpt.z,vol.bounds.maxpt.z))
        to_upload = [ int(z) for z in list(all_files.difference(done_files)) ]
        to_upload.sort()
        print(f"Have {len(to_upload)} slices remaining to upload",to_upload)
        with ProcessPoolExecutor(max_workers=4) as executor:
            for job in executor.map(process_slice,to_upload):
                try:
                    print(job)
                except Exception as exc:
                    print(f'generated an exception: {exc}')

