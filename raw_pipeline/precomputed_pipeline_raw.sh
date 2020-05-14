#!/bin/env bash
#

n_array_jobs_step1=$1
n_array_jobs_step2=$2
viz_dir=$3

# echo "In the directory: `pwd` "
# echo "As the user: `whoami` "
# echo "on host: `hostname` "
# echo "n_array_jobs_step1: ${n_array_jobs_step1}"
# echo "n_array_jobs_step2: ${n_array_jobs_step2}"
# echo "Viz_dir: ${viz_dir}"
#Make info file and layer directory
OUT0=$(sbatch --parsable --export=ALL,viz_dir=${viz_dir} --array=0 precomputed_step0.sh) 
echo $OUT0

# # Upload raw data to vol (writes precomputed data to disk)
max_array_jobs_step1=$(echo ${n_array_jobs_step1} | awk '{print $0-1}')
OUT1=$(sbatch --parsable --dependency=afterok:${OUT0##* } --export=ALL,viz_dir=${viz_dir} --array=0-${max_array_jobs_step1} precomputed_step1.sh) 
echo $OUT1

# # Upload raw data to vol (writes precomputed data to disk)
max_array_jobs_step2=$(echo $n_array_jobs_step2 | awk '{print $0-1}')
OUT2=$(sbatch --parsable --dependency=afterok:${OUT1##* } --export=ALL,viz_dir=${viz_dir} --array=0-${max_array_jobs_step2} precomputed_step2.sh) 
echo $OUT2 


# Usage notes:
# after = go once the specified job starts
# afterany = go if the specified job finishes, regardless of success
# afternotok = go if the specified job fails
# afterok = go if the specified job completes successfully
