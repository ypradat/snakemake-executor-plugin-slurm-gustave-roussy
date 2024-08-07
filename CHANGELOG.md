# 0.4

## Features:

*  Change naming of HPC output file to match the name of the rule log for which `.log` is replaced by "\_%j-%N.oe"
   allowing to see the HPC job and node ids.
*  Allow the user to specify runtime via the resources keywords `runtime`, `walltime`, `time_min` and the memory via the
   keywords `mem_mb` or `mem`
*  Remove condition preventing the user from running the pipeline on the login node.


# 0.3.2

## Features:

* Don't use node names

# 0.3.1

## Features:

* Use `nodename` instead of `nodes`

# 0.3.0

## Features:

* gpu queues and nodes handled

# 0.2.2

## Features:

* Warning on login-node submission

# 0.2.0

## Features:

* Update executor dependencies

# 0.1.0

## Features:

* Initial release: execute Snakemake on Flamingo, Gustave Roussy's computing cluster.
