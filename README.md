# Snakemake executor plugin: slurm-gustave-roussy

Snakemake plugin executor designed to match [Gustave Roussy](https://www.gustaveroussy.fr/en) computing cluster
specificities: automatic partition seleciton, and default resources value.

This plugin was adapated from 

Check the profile at <https://github.com/ypradat/snakemake_slurm_profile> to set your up.
This plugin should be installed as a python package alongside your snakemake comman

As this plugin was not deposited on `pip` you will have to locally install it via `pip install -e
/path/to/plugin` from the environment where snakemake is installed.
