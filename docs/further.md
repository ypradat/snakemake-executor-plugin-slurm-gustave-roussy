## Automatic partition selection

This executor automatically selects the best queue on `Flamingo` computing
cluster at [Gustave Roussy](https://www.gustaveroussy.fr/en).

In order not to break pipelines running on `Colibri`, and other (old) clusters,
this executor selects the best queue if, and only if the host name startswith
"`flamingo`".

GPU queue is automatically selected once `job.resources.gres` is not null.
One can find examples in [official Snakemake documentation](https://snakemake.readthedocs.io/en/stable/snakefiles/rules.html#resources-and-remote-execution) and expecially about
[gpu resources](https://snakemake.readthedocs.io/en/stable/snakefiles/rules.html#gpu-resources)

## Default values

By default, according to `Flamingo` defaults behavior, `--mem` is set to
1024 bytes, and `--time` to 6 hours.

As described in offcial Snakemake documentation, one can change these 
values, respectively through `job.resources.mem_mb` as
described [here](https://snakemake.readthedocs.io/en/stable/snakefiles/rules.html#resources),
and through `job.threads` as described in 
[there](https://snakemake.readthedocs.io/en/stable/snakefiles/rules.html#threads).

## Additional arguments

Additional Slurm arguments can be provided through Snakemake command line,
using `--slurm_gustave_roussy_args "..."`