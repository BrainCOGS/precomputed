#!/bin/env bash
#

viz_dir=$1

# echo "In the directory: `pwd` "
# echo "As the user: `whoami` "
# echo "on host: `hostname` "
# echo "Viz_dir: ${viz_dir}"
# Make info file and layer directory
OUT0=$(sbatch --parsable --export=ALL,viz_dir=${viz_dir} precomputed_step0.sh) 
echo $OUT0

# # Upload raw data to vol (writes precomputed data to disk)
OUT1=$(sbatch --parsable --dependency=afterok:${OUT0##* } --export=ALL,viz_dir=${viz_dir} precomputed_step1.sh) 
echo $OUT1

# # Upload raw data to vol (writes precomputed data to disk)
OUT2=$(sbatch --parsable --dependency=afterok:${OUT1##* } --export=ALL,viz_dir=${viz_dir} precomputed_step2.sh) 
echo $OUT2 


# Usage notes:
# after = go once the specified job starts
# afterany = go if the specified job finishes, regardless of success
# afternotok = go if the specified job fails
# afterok = go if the specified job completes successfully
