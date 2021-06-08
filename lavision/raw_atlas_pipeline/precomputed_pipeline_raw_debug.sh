#!/bin/env bash
#
# --- PURPOSE ---
# Pipeline to make precomputed (i.e. Neuroglancer-friendly) 
# volumes for raw light sheet images with the Princeton Mouse Atlas
# aligned to the raw LaVision space.
# Run on each channel separately.

# author: Austin Hoag
# date: 06/08/2021

raw_atlas_dir=/jukebox/LightSheetData/lightserv/oostland/MO_May2021_Tsc1_part1/MO_May2021_Tsc1_part1-520/imaging_request_1/output/processing_request_1/resolution_1.3x/raw_atlas/transformed_annotations/single_tifs
viz_dir=/jukebox/LightSheetData/lightserv/oostland/MO_May2021_Tsc1_part1/MO_May2021_Tsc1_part1-520/imaging_request_1/viz
z_step_microns=10 # spacing between the z planes -- see request__imaging_table in u19lightserv_lightsheet database


# Now raw atlas
echo "Jobids for raw atlas precomputed steps 0-3:"

# Step 0: Make info file and layer directory
OUT0=$(sbatch --parsable --export=ALL,raw_atlas_dir=${raw_atlas_dir},viz_dir=${viz_dir},z_step_microns=${z_step_microns} precomputed_atlas_step0.sh) 
echo $OUT0

# # # Step 1: Upload raw data to vol (writes precomputed data to disk)
# OUT1=$(sbatch --parsable --dependency=afterok:${OUT0##* } --export=ALL,raw_atlas_dir=${raw_atlas_dir},viz_dir=${viz_dir},z_step_microns=${z_step_microns} precomputed_atlas_step1.sh) 
# echo $OUT1

# #Step 2: Transfer tasks -- rechunks the volume more efficiently
# OUT2=$(sbatch --parsable --dependency=afterok:${OUT1##* } --export=ALL,raw_atlas_dir=${raw_atlas_dir},viz_dir=${viz_dir},z_step_microns=${z_step_microns} precomputed_atlas_step2.sh) 
# echo $OUT2

# #Step 2: Make downsamples (higher mips) 
# OUT3=$(sbatch --parsable --dependency=afterok:${OUT2##* } --export=ALL,raw_atlas_dir=${raw_atlas_dir},viz_dir=${viz_dir},z_step_microns=${z_step_microns} precomputed_atlas_step3.sh) 
# echo $OUT3


# Dependency Usage notes:
# after = go once the specified job starts
# afterany = go if the specified job finishes, regardless of success
# afternotok = go if the specified job fails
# afterok = go if the specified job completes successfully
