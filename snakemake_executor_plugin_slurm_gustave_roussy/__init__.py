__author__ = "Thibault Dayris"
__copyright__ = "Copyright 2024, Thibault Dayris"
__email__ = "thibault.dayris@gustaveroussy.fr"
__license__ = "MIT"

# This executor is highly based on snakemake-executor-plugin-slurm
# its purpose is to be used on and only on Gustave Roussy's computing
# cluster. (Flamingo, not the old ones)

import os
import subprocess
import uuid

from dataclasses import dataclass, field
from typing import Optional
from snakemake_interface_executor_plugins.executors.base import SubmittedJobInfo
from snakemake_interface_executor_plugins_slurmimport import Executor as SlurmExecutor
from snakemake_interface_executor_plugins.settings import (
    ExecutorSettingsBase,
    CommonSettings,
)
from snakemake_interface_executor_plugins.jobs import (
    JobExecutorInterface,
)
from snakemake_interface_common.exceptions import WorkflowError  # noqa


# Optional:
# Define additional settings for your executor.
# They will occur in the Snakemake CLI as --<executor-name>-<param-name>
# Omit this class if you don't need any.
# Make sure that all defined fields are Optional and specify a default value
# of None or anything else that makes sense in your case.
@dataclass
class ExecutorSettings(ExecutorSettingsBase):
    args: Optional[str] = field(
        default=None,
        metadata={"help": "Optional arguments passed to slurm"},
    )


# Required:
# Specify common settings shared by various executors.
common_settings = CommonSettings(
    # define whether your executor plugin executes locally
    # or remotely. In virtually all cases, it will be remote execution
    # (cluster, cloud, etc.). Only Snakemake's standard execution
    # plugins (snakemake-executor-plugin-dryrun, snakemake-executor-plugin-local)
    # are expected to specify False here.
    non_local_exec=True,
    # Whether the executor implies to not have a shared file system
    implies_no_shared_fs=False,
    # whether to deploy workflow sources to default storage provider before execution
    job_deploy_sources=False,
    # whether arguments for setting the storage provider shall be passed to jobs
    pass_default_storage_provider_args=True,
    # whether arguments for setting default resources shall be passed to jobs
    pass_default_resources_args=True,
    # whether environment variables shall be passed to jobs (if False, use
    # self.envvars() to obtain a dict of environment variables and their values
    # and pass them e.g. as secrets to the execution backend)
    pass_envvar_declarations_to_cmd=True,
    # whether the default storage provider shall be deployed before the job is run on
    # the remote node. Usually set to True if the executor does not assume a shared fs
    auto_deploy_default_storage_provider=True,
    # specify initial amount of seconds to sleep before checking for job status
    init_seconds_before_status_checks=1,
)


# Required:
# Implementation of your executor
class Executor(SlurmExecutor):
    def __post_init__(self):
        self.run_uuid: str = str(uuid.uuid4())
        self.logger.info(f"SLURM run ID: {self.run_uuid}")
        self.default_partition: str = "shortq"
        self.default_runtime: int = 360
        self.default_mem: int = 1024
        self._fallback_account_arg: Optional[str] = None
        self._fallback_partition: str = "shortq"

        # access workflow
        # self.workflow
        # access executor specific settings
        # self.workflow.executor_settings

        # IMPORTANT: in your plugin, only access methods and properties of
        # Snakemake objects (like Workflow, Persistence, etc.) that are
        # defined in the interfaces found in the
        # snakemake-interface-executor-plugins and the
        # snakemake-interface-common package.
        # Other parts of those objects are NOT guaranteed to remain
        # stable across new releases.

        # To ensure that the used interfaces are not changing, you should
        # depend on these packages as >=a.b.c,<d with d=a+1 (i.e. pin the
        # dependency on this package to be at least the version at time
        # of development and less than the next major version which would
        # introduce breaking changes).

        # In case of errors outside of jobs, please raise a WorkflowError

    def additional_general_args(self) -> str:
        return "--executor slurm-jobstep --jobs 1"

    def get_partition(self, runtime: int = 360, gres: Optional[str] = None) -> str:
        """
        Return best partition based on runtime reservation
        and possible GPU resources reservation.
        """
        if gres:
            return "gpuq"

        if runtime <= 360:
            return "shortq"

        if runtime <= 60 * 24:
            return "mediumq"

        if runtime <= 60 * 24 * 7:
            return "longq"

        if runtime <= 60 * 24 * 60:
            return "verylongq"

        self.logger.warning(
            "Could not select a correct partition. "
            f"Falling back to default: {self.default_partiton}"
        )
        return self.default_partition

    def run_job(self, job: JobExecutorInterface):
        # Implement here how to run a job.
        # You can access the job's resources, etc.
        # via the job object.
        # After submitting the job, you have to call
        # self.report_job_submission(job_info).
        # with job_info being of type
        # snakemake_interface_executor_plugins.executors.base.SubmittedJobInfo.
        # If required, make sure to pass the job's id to the job_info object, as keyword
        # argument 'external_job_id'.

        log_folder: str = f"group_{job.name}" if job.is_group() else f"rule_{job.name}"
        slurm_logfile: str = os.path.abspath(
            f".snakemake/slurm_logs/{log_folder}/%j.log"
        )
        os.makedirs(os.path.dirname(slurm_logfile), exist_ok=True)

        # generic part of a submission string:
        # we use a run_uuid as the job-name, to allow `--name`-based
        # filtering in the job status checks (`sacct --name` and `squeue --name`)
        call: str = (
            f"sbatch --job-name {self.run_uuid} --output {slurm_logfile} --export=ALL "
            f"--comment {job.name} "
        )

        runtime: int = job.resources.get("runtime", self.default_runtime)
        partition: str = self.get_partition(
            runtime=runtime, gres=self.resources.get("gres")
        )
        call += (
            f" --partition {partition} "
            f"--time {runtime} "
            f"--cpus-per-task {job.threads}"
        )

        # ensure that workdir is set correctly
        # use short argument as this is the same in all slurm versions
        # (see https://github.com/snakemake/snakemake/issues/2014)
        call += f" -D {self.workflow.workdir_init}"
        # and finally the job to execute with all the snakemake parameters
        exec_job: str = self.format_job_exec(job)
        call += f' --wrap="{exec_job}"'

        self.logger.debug(f"sbatch call: {call}")
        try:
            out: str = subprocess.check_output(
                call, shell=True, text=True, stderr=subprocess.STDOUT
            ).strip()
        except subprocess.CalledProcessError as e:
            raise WorkflowError(
                f"SLURM job submission failed. The error message was {e.output}"
            )

        slurm_jobid: str = out.split(" ")[-1]
        slurm_logfile: str = slurm_logfile.replace("%j", slurm_jobid)
        self.logger.info(
            f"Job {job.jobid} has been submitted with SLURM jobid {slurm_jobid} "
            f"(log: {slurm_logfile})."
        )
        self.report_job_submission(
            SubmittedJobInfo(
                job, external_jobid=slurm_jobid, aux={"slurm_logfile": slurm_logfile}
            )
        )
