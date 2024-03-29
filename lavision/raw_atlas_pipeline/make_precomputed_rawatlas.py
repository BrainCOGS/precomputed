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
from precomputed_utils import calculate_chunks, calculate_factors

def make_info_file(volume_size,resolution,layer_dir,commit=True,atlas_type=None):
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
        layer_type = 'segmentation', # 'image' or 'segmentation'
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
    if atlas_type:
        assert atlas_type in ['Princeton','Allen']
        info_dict = vol.info
        info_dict['atlas_type'] = atlas_type
        print(info_dict)
        info_filename = '/'.join(vol.info_cloudpath.split('/')[2:]) 
        with open(info_filename,'w') as outfile:
            json.dump(info_dict,outfile,sort_keys=True,indent=2)
        print("ammended info file to include 'atlas_type' key")
    return vol

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
    array = np.flipud(np.array(image, dtype=np.uint16, order='F'))
    array = array.reshape((1, height, width)).T
    vol[:,:, z] = array
    image.close()
    touch(os.path.join(progress_dir, str(z)))
    print("success")
    return

if __name__ == "__main__":
    """ First command line arguments """
    step = sys.argv[1] # "step0", "step1", "step2" or "step3"
    raw_atlas_dir = sys.argv[2] # path to the single_tifs/ directory where the raw atlas planes are saved
    viz_dir = sys.argv[3] # path to lightserv/$username/$request_name/.../viz/raw_atlas
    z_step_microns = sys.argv[4]
    image_resolution="1.3x" # Hardcoded. The algorithm is optimized for 1.3x and not tested on the other LaVision resolutions: 1.1x, 2x or 4x
    mkdir(viz_dir) # does not crash on prexisting
    cpus = os.cpu_count()
    if cpus > 16:
        cpus = 16

    layer_name = f"raw_atlas"
    # Make directories for orig layer, destination layer 
    # orig - just for uploading mip=-1
    orig_layer_name = layer_name + '_rechunkme'
    orig_layer_dir = os.path.join(viz_dir,orig_layer_name)
    mkdir(orig_layer_dir)
    progress_dir = mkdir(viz_dir + f'/progress_{orig_layer_name}') # unlike os.mkdir doesn't crash on prexisting 

    # dest - where the rechunked layer will live
    dest_layer_dir = os.path.join(viz_dir,layer_name)
    mkdir(dest_layer_dir)
    rechunked_cloudpath = f'file://{dest_layer_dir}'

    # Figure out volume size in pixels and in nanometers
    all_slices = glob.glob(f"{raw_atlas_dir}/*tif")  
    assert len(all_slices) > 0
    random_slice = all_slices[0]
    first_im = Image.open(random_slice)
    x_dim,y_dim = first_im.size
    first_im.close()
    z_dim = len(all_slices)    
    z_scale_nm = int(z_step_microns)*1000
    if image_resolution == "1.3x":
        x_scale_nm,y_scale_nm = 5000,5000
    else:
        sys.exit("Resolution must be 1.3x for the time being. Algorithm is not tested on the other resolutions")

    # Handle the different steps 
    if step == 'step0':
        print("step 0, making info file")
        volume_size = (x_dim,y_dim,z_dim)
        resolution = (x_scale_nm,y_scale_nm,z_scale_nm)
        vol = make_info_file(volume_size=volume_size,
            layer_dir=orig_layer_dir,
            resolution=resolution)
    elif step == 'step1':
        print("step 1, making full resolution layer at orig chunk size")
        sorted_files = sorted(all_slices)
        vol = CloudVolume(f'file://{orig_layer_dir}')
        done_files = set([ int(z) for z in os.listdir(progress_dir) ])
        all_files = set(range(vol.bounds.minpt.z, vol.bounds.maxpt.z))

        to_upload = [ int(z) for z in list(all_files.difference(done_files)) ]
        to_upload.sort()
        print(f"Have {len(to_upload)} planes to upload")
        with ProcessPoolExecutor(max_workers=cpus) as executor:
            for job in executor.map(process_slice,to_upload):
                try:
                    print(job)
                except Exception as exc:
                    print(f'generated an exception: {exc}')
    elif step == 'step2': # transfer tasks
        orig_vol = CloudVolume(f'file://{orig_layer_dir}')
        first_chunk = calculate_chunks(downsample='full',mip=0)
        first_chunk = [int(x) for x in first_chunk]
        tq = LocalTaskQueue(parallel=cpus)

        tasks = tc.create_transfer_tasks(orig_vol.cloudpath, dest_layer_path=rechunked_cloudpath, 
            chunk_size=first_chunk, mip=0, skip_downsamples=True)
        print(len(tasks))
        tq.insert(tasks)
        tq.execute()

    elif step == 'step3': # downsampling
        print("step 3, downsampling")
        tq = LocalTaskQueue(parallel=cpus)
        downsample="full"
        mips = [0,1,2,3,4]
        for mip in mips:
            print(f"Mip: {mip}")
            cv = CloudVolume(rechunked_cloudpath, mip)
            chunks = calculate_chunks(downsample, mip)
            factors = calculate_factors(downsample, mip)
            print(f"Chunk size: {chunks}")
            print(f"Downsample factors: {factors}")
            tasks = tc.create_downsampling_tasks(cv.layer_cloudpath, 
                mip=mip, num_mips=1, factor=factors, preserve_chunk_size=False,
                compress=True, chunk_size=chunks)
            tq.insert(tasks)
            tq.execute()
            print()


