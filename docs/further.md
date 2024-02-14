## Usage

While running Snakemake, use this executor with:

``` console
$ snakemake --executor slurm-gustave-roussy
```

## Partition auto-selection ([`--partition`](https://slurm.schedmd.com/sbatch.html#OPT_partition))

No need to define the partition: it is done based on the
`runtime`, and `mem_mb` values defined in your rules and
profiles.

## Default resources auto-selection

No need to define default `runtime` and `mem_mb` values.
On [Gustave Roussy](https://www.gustaveroussy.fr/en) computing
cluster, 6 hours of time and 1Gb of ram are automatically
allocated to your job.

Please not that falling back to defautl values won't let
snakemake re-schedule your job with better resources in
case of `OUT_OF_MEMORY` and `TIMEOUT` errors.

## Specify [`--cpus-per-task`](https://slurm.schedmd.com/sbatch.html#OPT_cpus-per-task)

While writing your snakemake rules, use the `threads`
section to define the number of cpus reserved in your job
submussion.

Example:

``` python
rule a:
    input: ...
    output: ...
    threads: 2
```

Have a look at the official snakemake documentation about 
[threads reservations](https://snakemake.readthedocs.io/en/latest/snakefiles/rules.html#threads).

## Specify [`--mem`](https://slurm.schedmd.com/sbatch.html#OPT_mem)


While writing your snakemake rules, use the `resource`
section to define the amout of memory reseved under the
name `mem_mb`:

Example:

``` python
rule a:
    input: ...
    output: ...
    threads: 2
    resources:
        mem_mb=1024
```

By default, this number is in _megabytes_, have a look at the official
snakemake documentation about [memory reservations](https://snakemake.readthedocs.io/en/latest/snakefiles/rules.html#resources).

## Specify [`--time`](https://slurm.schedmd.com/sbatch.html#OPT_time)


While writing your snakemake rules, use the `resource`
section to define the amout of memory reseved under the
name `runntime`:

Example:

``` python
rule a:
    input: ...
    output: ...
    threads: 2
    resources:
        mem_mb=1024
        runtime=45
```

By default, this number is in _minutes_, have a look at the official
snakemake documentation about [time reservations](https://snakemake.readthedocs.io/en/latest/snakefiles/rules.html#resources).


## Advanced Resource Specifications

A workflow rule may support a number of
[resource specifications](https://snakemake.readthedocs.io/en/latest/snakefiles/rules.html#resources).
For a SLURM cluster, a mapping between Snakemake and SLURM needs to be performed.

You can use the following specifications:

| SLURM             | Snakemake         | Description                                                    |
| ----------------- | ----------------- | -------------------------------------------------------------- |
| `--partition`     | `slurm_partition` | the partition a rule/job is to use                             |
| `--time`          | `runtime`         | the walltime per job in minutes                                |
| `--constraint`    | `constraint`      | may hold features on some clusters                             |
| `--mem`           | `mem`, `mem_mb`   | memory a cluster node must                                     |
|                   |                   | provide (`mem`: string with unit), `mem_mb`: i                 |
| `--mem-per-cpu`   | `mem_mb_per_cpu`  | memory per reserved CPU                                        |
| `--ntasks`        | `tasks`           | number of concurrent tasks / ranks                             |
| `--cpus-per-task` | `cpus_per_task`   | number of cpus per task (in case of SMP, rather use `threads`) |
| `--nodes`         | `nodes`           | number of nodes                                                |

Each of these can be part of a rule, e.g.:

``` python
rule:
    input: ...
    output: ...
    resources:
        partition=<partition name>
        runtime=<some number>
```

Please note: as `--mem` and `--mem-per-cpu` are mutually exclusive on
SLURM clusters, their corresponding resource flags `mem`/`mem_mb` and
`mem_mb_per_cpu` are mutually exclusive, too. You can only reserve
memory a compute node has to provide or the memory required per CPU
(SLURM does not make any distinction between real CPU cores and those
provided by hyperthreads). SLURM will try to satisfy a combination of
`mem_mb_per_cpu` and `cpus_per_task` and `nodes`, if `nodes` is not
given.

Note that it is usually advisable to avoid specifying SLURM (and compute
infrastructure) specific resources (like `constraint`) inside of your
workflow because that can limit the reproducibility on other systems.
Consider using the `--default-resources` and `--set-resources` flags to specify such resources
at the command line or (ideally) within a [profile](https://snakemake.readthedocs.io/en/latest/executing/cli.html#profiles).

## Additional custom job configuration

SLURM installations can support custom plugins, which may add support
for additional flags to `sbatch`. In addition, there are various
`sbatch` options not directly supported via the resource definitions
shown above. You may use the `slurm_extra` resource to specify
additional flags to `sbatch`:

``` python
rule myrule:
    input: ...
    output: ...
    resources:
        slurm_extra="--qos=long --mail-type=ALL --mail-user=<your email>"
```

Again, rather use a [profile](https://snakemake.readthedocs.io/en/latest/executing/cli.html#profiles) to specify such resources.