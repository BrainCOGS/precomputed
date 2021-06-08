#!/bin/env bash
#

viz_dir='/jukebox/LightSheetData/lightserv/oostland/MO_May2021_Tsc1_part1/MO_May2021_Tsc1_part1-520/imaging_request_1/viz/processing_request_1/registered/channel_555_injch/'

# echo "In the directory: `pwd` "
# echo "As the user: `whoami` "
# echo "on host: `hostname` "
# echo "n_array_jobs_step1: ${n_array_jobs_step1}"
# echo "n_array_jobs_step2: ${n_array_jobs_step2}"
# echo "Viz_dir: ${viz_dir}"

# Make info file and layer directory
OUT0=$(sbatch --parsable --export=ALL,viz_dir=${viz_dir} precomputed_step0.sh) 
echo $OUT0

# Upload data to vol (writes precomputed data to disk)
OUT1=$(sbatch --parsable --dependency=afterok:${OUT0##* } --export=ALL,viz_dir=${viz_dir} precomputed_step1.sh) 
echo $OUT1



# Usage notes:
# after = go once the specified job starts
# afterany = go if the specified job finishes, regardless of success
# afternotok = go if the specified job fails
# afterok = go if the specified job completes successfully
