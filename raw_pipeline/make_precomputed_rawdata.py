#! /bin/env python

import os, sys
import glob
from concurrent.futures import ProcessPoolExecutor

import numpy as np
from PIL import Image

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

def make_downsample_tasks(vol,mip_start=0,num_mips=3):
    """ 
    ---PURPOSE---
    Make downsamples of the precomputed data
    ---INPUT---
    vol             The cloudvolume.Cloudvolume() object
    mip_start       The mip level to start at with the downsamples
    num_mips        The number of mip levels to create, starting from mip_start
    """
    # cloudpath = 'file:///home/ahoag/ngdemo/demo_bucket/m61467_demons_20190702/190821_647'
    cloudpath = vol.cloudpath
    # with LocalTaskQueue(parallel=8) as tq:
    tasks = tc.create_downsampling_tasks(
        cloudpath, 
        mip=mip_start, # Start downsampling from this mip level (writes to next level up)
        fill_missing=False, # Ignore missing chunks and fill them with black
        axis='z', 
        num_mips=num_mips, # number of downsamples to produce. Downloaded shape is chunk_size * 2^num_mip
        chunk_size=[ 128, 128, 64 ], # manually set chunk size of next scales, overrides preserve_chunk_size
        preserve_chunk_size=True, # use existing chunk size, don't halve to get more downsamples
      )
    return tasks

def process_slice(z):
    if os.path.exists(os.path.join(progress_dir, str(z))):
        print(f"Slice {z} already processed, skipping ")
        return
    if z > (len(sorted_files) - 1):
        print("Index {z} is larger than (number of slices - 1), skipping")
        return
    print('Processing slice z=',z)
    img_name = sorted_files[z]
    image = Image.open(img_name)
    width, height = image.size 
    array = np.array(image, dtype=np.uint16, order='F')
    array = array.reshape((1, height, width)).T
    vol[:,:, z] = array
    image.close()
    touch(os.path.join(progress_dir, str(z)))
    print("success")
    return

if __name__ == "__main__":
    """ First command line arguments """
    step = sys.argv[1]
    viz_dir = sys.argv[2]
    """ Load param dictionary """
    param_file = viz_dir + '/precomputed_params.p'
    with open(param_file,'rb') as pkl_file:
        param_dict = pickle.load(pkl_file)
    slurmjobfactor = param_dict['slurmjobfactor']
    rawdata_path = param_dict['rawdata_path']
    layer_name = param_dict['layer_name']
    image_resolution = param_dict['image_resolution']
    channel_index = param_dict['channel_index']
    lightsheet = param_dict['lightsheet']
    z_scale_nm = int(float(param_dict['z_step'])*1000) # to convert from microns to nm
    layer_dir = os.path.join(viz_dir,layer_name)
    """ Make progress dir """
    progress_dir = mkdir(viz_dir + f'/progress_{layer_name}') # unlike os.mkdir doesn't crash on prexisting 
    """ Figure out volume size in pixels and in nanometers """
    x_dim = 2160
    y_dim = 2560
    z_dim = param_dict['number_of_z_planes']
    if image_resolution == '4x':
        x_scale_nm, y_scale_nm = 1630,1630
    elif image_resolution == '1.3x':
        x_scale_nm, y_scale_nm = 5000,5000
    elif image_resolution == '1.1x':
        x_scale_nm, y_scale_nm = 5909,5909 # hardcode default. Need to fix!
    else:
        sys.exit(f"image resolution {image_resolution} Not supported")

    """ Handle the different steps """
    if step == 'step0':
        print("step 0")
        volume_size = (x_dim,y_dim,z_dim)
        resolution = (x_scale_nm,y_scale_nm,z_scale_nm)
        vol = make_info_file(volume_size=volume_size,layer_dir=layer_dir,resolution=resolution)
    elif step == 'step1':
        print("step 1")
        # Look for 00 x 00 tiles since this pipeline is only for non-tiled images with Filter000{channel_index} since there could be multi-channel imaging
        if lightsheet == 'left':
            all_slices = glob.glob(f"{rawdata_path}/*RawDataStack[00 x 00*C00*Filter000{channel_index}*tif") 
        elif lightsheet == 'right':
            all_slices = glob.glob(f"{rawdata_path}/*RawDataStack[00 x 00*C01*Filter000{channel_index}*tif") 
        else:
            sys.exit("'lightsheet' parameter in param_dict not 'left' or 'right' ")
        sorted_files = sorted(all_slices)
        vol = CloudVolume(f'file://{layer_dir}')
        done_files = set([ int(z) for z in os.listdir(progress_dir)])
        all_files = set(range(vol.bounds.minpt.z,vol.bounds.maxpt.z))
        to_upload = [ int(z) for z in list(all_files.difference(done_files)) ]
        to_upload.sort()
        print(f"Have {len(to_upload)} slices remaining to upload:",to_upload)
        with ProcessPoolExecutor(max_workers=12) as executor:
            for job in executor.map(process_slice,to_upload):
                try:
                    print(job)
                except Exception as exc:
                    print(f'generated an exception: {exc}')
    elif step == 'step2': # downsampling
        print("step 2")
        vol = CloudVolume(f'file://{layer_dir}')
        with LocalTaskQueue(parallel=12) as tq:
            tasks = make_downsample_tasks(vol,mip_start=0,num_mips=4)
            tq.insert_all(tasks)

