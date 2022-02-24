import os, json
from concurrent.futures import ProcessPoolExecutor

import numpy as np
from PIL import Image
import tifffile

from cloudvolume import CloudVolume
from cloudvolume.lib import mkdir, touch

from taskqueue import LocalTaskQueue
import igneous.task_creation as tc

home_dir = '/home/ahoag/ngdemo'
atlas_file = os.path.join('/home/ahoag/ngdemo/data',
	'allen_atlas/annotation_2017_25um_sagittal_16bit_hierarch_labels.tif')

def make_info_file(volume_size,resolution,layer_dir,commit=True,atlas_type=None):
	""" 
	---PURPOSE---
	Make the cloudvolume info file.
	---INPUT---
	volume_size     [Nx,Ny,Nz] in voxels, e.g. [2160,2560,1271]
	pix_scale_nm    [size of x pix in nm,size of y pix in nm,size of z pix in nm], e.g. [5000,5000,10000]
	commit          if True, will write the info/provenance file to disk. 
					if False, just creates it in memory
	atlas_type      if provided, will add a key to the info file: 
					'atlas_type': atlas_type
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
	if z >= z_dim:
		print("Index {z} >= z_dim of volume, skipping")
		return
	print('Processing slice z=',z)
	array = image[z].reshape((1,y_dim,x_dim)).T
	vol[:,:, z] = array
	touch(os.path.join(progress_dir, str(z)))
	return "success"

def make_demo_mesh(vol):
	# Mesh on 8 cores, use True to use all cores
	cloudpath = vol.cloudpath
	with LocalTaskQueue(parallel=8) as tq:
	  tasks = tc.create_meshing_tasks(cloudpath, mip=0, shape=(256, 256, 256))
	  tq.insert_all(tasks)
	  tasks = tc.create_mesh_manifest_tasks(cloudpath)
	  tq.insert_all(tasks)
	print("Done!")	

if __name__ == '__main__':
	""" Fill the CloudVolume() instance with data from the tif slices """
	resolution = (25000,25000,25000) # 25 micron isotropic
	viz_dir = '/home/ahoag/ngdemo/demo_bucket/atlas'
	progress_parentdir = '/home/ahoag/ngdemo/progress_dirs'
	layer_name = 'allenatlas_2017_16bit_hierarch_labels'
	layer_dir = os.path.join(viz_dir,layer_name)
	""" Now load the tifffile in its entirety """
	image = np.array(tifffile.imread(atlas_file),dtype=np.uint16, order='F') # F stands for fortran order
	z_dim,y_dim,x_dim = image.shape
	volume_size = (x_dim,y_dim,z_dim)

	vol = make_info_file(
		volume_size=volume_size,
		layer_dir=layer_dir,
		resolution=resolution,
		atlas_type='Allen')
	
	progress_dir = mkdir(progress_parentdir + f'/progress_{layer_name}') # unlike os.mkdir doesn't crash on prexisting 

	done_files = set([ int(z) for z in os.listdir(progress_dir) ])
	all_files = set(range(vol.bounds.minpt.z, vol.bounds.maxpt.z)) 

	to_upload = [ int(z) for z in list(all_files.difference(done_files)) ]
	to_upload.sort()
	print("Remaining slices to upload are:",to_upload)

	with ProcessPoolExecutor(max_workers=10) as executor:
		for job in executor.map(process_slice,to_upload):
			try:
				print(job)
			except Exception as exc:
				print(f'generated an exception: {exc}')
		
	make_demo_mesh(vol)


