#! /bin/env python

import os, sys

from cloudvolume import CloudVolume
from cloudvolume.lib import mkdir, touch
import shutil
import json

src_seg_props_info = '/jukebox/LightSheetData/atlas/neuroglancer/atlas/princetonmouse/segment_properties/info'
viz_dir="/jukebox/LightSheetData/lightserv_testing/neuroglancer/jess_cfos" # bucket dir, so layers will be in $BUCKET/$DATASET/$LAYER 

if __name__ == "__main__":
    animal_dataset_dict = {
        '201904_ymaze_cfos':
            ['an4','an5','an6','an7','an8','an11',
            'an12','an15','an16','an18','an19','an20',
            'an22','an23','an24','an25','an26','an27'],
        '201810_adultacutePC_ymaze_cfos':
            ['dadult_pc_crus1_1','dadult_pc_crus1_2',
            'dadult_pc_crus1_3','dadult_pc_crus1_4',
            'dadult_pc_crus1_5'],
        '202002_cfos':
            ['an2_vecctrl_ymaze','an3_vecctrl_ymaze',
             'an4_vecctrl_ymaze','an9_vecctrl_ymaze',
             'an10_vecctrl_ymaze','an1_crus1_lat','an2_crus1_lat',
             'an4_crus1_lat','an5_crus1_lat','an6_crus1_lat',
             'an7_crus1_lat','an10_crus1_lat','an11_crus1_lat',
             'an13_crus1_lat','an19_crus1_lat','an4_saline','an5_cno']
    }
    for dataset in animal_dataset_dict.keys():
        animal_id_list = animal_dataset_dict[dataset]
        for animal_id in animal_id_list:
            # Update the info file and save it
            print(f"Dataset: {dataset}, animal_id: {animal_id}")
            layer_dir = os.path.join(viz_dir,dataset,f'rawatlas_{animal_id}_iso')
            vol = CloudVolume(f'file://{layer_dir}')
            info_dict = vol.info
            info_dict['segment_properties'] = "segment_properties"
            info_filename = '/'.join(vol.info_cloudpath.split('/')[2:]) 
            with open(info_filename,'w') as outfile:
                json.dump(info_dict,outfile,sort_keys=True,indent=2)
            print(f"ammended info file to include 'segment_properties' key: {info_filename}")
            # copy over the segment_properties directory
            seg_props_dir = os.path.join(layer_dir,'segment_properties')
            mkdir(seg_props_dir)
            dest_seg_props_info = os.path.join(seg_props_dir,'info')
            shutil.copyfile(src_seg_props_info,dest_seg_props_info)
            print("copied over segment_properties info file")