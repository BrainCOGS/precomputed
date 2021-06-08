# precomputed

These are pipelines used by [lightserv](https://github.com/BrainCOGS/lightserv) for converting light sheet data at various steps in the image processing pipeline [brainpipe](https://github.com/BrainCOGS/brainpipe) to precomputed format, one of the formats that Neuroglancer can read. These scripts are set up to run on a slurm-based high performance computer, and they are actively used on PNI's spock as part of lightserv.

They can also be run on a local machine as well by just executing the python scripts in each folder if one provides command line arguments. 

## Requirements
I use a conda environment set up in the following way:
```
conda create --name precomputed python=3.8 
conda activate precomputed # go into the environment
pip install numpy cloud-volume neuroglancer
# get igneous - needed for meshing and downsampling, not available via pip
git clone https://github.com/seung-lab/igneous.git
cd igneous/
pip install -r requirements.txt
python setup.py develop
# Get jupyter notebooks to work with this environment
pip install --user ipykernel
python -m ipykernel install --user --name=precomputed
```

## Description of folder contents

### lavision/
- raw_pipeline/: The pipeline for converting raw light sheet data (at the individual light sheet level, before blending) to precomputed format. Only used on single tile data.

- stitched_pipeline/: The counterpart to raw_pipeline for multi-tile data. This precomputed pipeline runs after multi-tile data has been stitched in the image processing pipeline, but before the blending occurs.

- blended_pipeline/: The pipeline for converting blended (i.e. once both left and right light sheets are combined into a single dataset) light sheet data to precomputed format. Does not matter if data was stitched or not. 

- downsized_pipeline/: The pipeline for converting downsized light sheet data to precomputed format. Downsizing happens after blending and before registration to an atlas. 

- registered_pipeline/: The pipeline for converting light sheet data that has been registered to an atlas to precomputed format.  

### smartspim/
