#!/bin/env bash
#
# --- PURPOSE ---
# Pipeline to make precomputed (i.e. Neuroglancer-friendly) 
# volumes for raw atlas 

# author: Austin Hoag
# date: 04/08/2020

brain=zimmerman_01-001
raw_atlas_dir=/jukebox/LightSheetData/lightserv/cz15/zimmerman_01/zimmerman_01-001/imaging_request_1/rawdata/resolution_3.6x/raw_atlas/transformed_annotations/single_tifs
viz_dir=/jukebox/LightSheetData/lightserv/cz15/zimmerman_01/zimmerman_01-001/imaging_request_1/viz/raw_atlas
# dataset=201810_adultacutePC_ymaze_cfos
# animal_id=dadult_pc_crus1_5
# dataset=202002_cfos
# animal_id=an5_cno
# dataset=201904_ymaze_cfos
# animal_id=an27 


# echo "Jobids for raw atlas precomputed steps 0, 1 and 2:"

# Step 0: Make info file and layer directory
OUT0=$(sbatch --parsable --export=ALL,brain=${brain} precomputed_atlas_step0.sh) 
echo $OUT0

# # Step 1: Upload raw data to vol (writes precomputed data to disk)
OUT1=$(sbatch --parsable --dependency=afterok:${OUT0##* } --export=ALL,brain=${brain},raw_atlas_dir=${raw_atlas_dir},viz_dir=${viz_dir} precomputed_atlas_step1.sh) 
echo $OUT1

#Step 2: Transfer tasks 
OUT2=$(sbatch --parsable --dependency=afterok:${OUT1##* } --export=ALL,brain=${brain},raw_atlas_dir=${raw_atlas_dir},viz_dir=${viz_dir} precomputed_atlas_step2.sh) 
echo $OUT2

#Step 3: Make downsamples (higher mips) 
OUT3=$(sbatch --parsable --dependency=afterok:${OUT2##* } --export=ALL,brain=${brain},raw_atlas_dir=${raw_atlas_dir},viz_dir=${viz_dir} precomputed_atlas_step3.sh) 
echo $OUT3

# Usage notes:
# after = go once the specified job starts
# afterany = go if the specified job finishes, regardless of success
# afternotok = go if the specified job fails
# afterok = go if the specified job completes successfully
