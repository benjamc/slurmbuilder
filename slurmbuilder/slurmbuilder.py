from argparse import ArgumentParser
import numpy as np
from pathlib import Path
import subprocess
import os


class AbstractSlurmBuilder(object):
    def __init__(
            self,
            job_name: str,
            mail_user: str,
            base_command: str,
            pre_command: str,
            post_command: str,
            partition: str = "cpu_normal",
            time: str = "48:00:00",
            mem_per_cpu: str = "1000",
            mail_type: str = "ALL",
            runscript_outdir: str =  "runscripts/generated",
            **kwargs
    ):
        self.mail_user = mail_user
        self.mail_type = mail_type
        self.partition = partition
        self.job_name = job_name
        self.time = time
        self.mem_per_cpu = mem_per_cpu
        self.pre_command = pre_command
        self.post_command = post_command

        self.template_slurm_config = "#SBATCH --{command_name}={command_value}\n"
        self.runscript_outdir = runscript_outdir
        self.runlist_fname = Path(self.runscript_outdir) / "runcommands.sh"

        self.slurm_config = {
            "mail-user": self.mail_user,
            "mail-type": self.mail_type,
            "partition": self.partition,
            "job-name": self.job_name,
            "time": self.time,
            "mem-per-cpu": self.mem_per_cpu,
        }

        self.main_command_template = " --{name} {value}"
        self.main_arg_names = []
        self.shfilenames = []

    def build_slurm_header(self, job_name_identifier: str = ""):
        header = ""
        header += "#!/bin/bash\n"
        header += "#\n"

        for command_name, command_value in self.slurm_config.items():
            if command_name == "job-name":
                command_value += f"_{job_name_identifier}"
            header += self.template_slurm_config.format(command_name=command_name, command_value=command_value)
        header += "\n"
        return header

    def build_precommands(self):
        commands = ""
        commands += self.pre_command
        return commands

    def build_postcommands(self):
        commands = ""
        commands += self.post_command
        return commands

    def build_shfilename(self, job_name_identifier: str = ""):
        return f"{self.runscript_outdir}/run_{self.job_name}_{job_name_identifier}.sh"

    def build_spawn_command(self, shfilename: str, to_args: bool = True):
        # shfilename = Path(os.getcwd()) / shfilename
        spawncommand = f"sbatch {shfilename}"
        if to_args:
            cmd = spawncommand.split(" ")  # split at whitespaces to create args for subprocess.run
        else:
            cmd = spawncommand
        return cmd

    def write_spawnlist(self):
        content = ""
        for shfilename in self.shfilenames:
            spawncommand = self.build_spawn_command(shfilename=shfilename, to_args=False)
            content = content + spawncommand + "\n"
        with open(self.runlist_fname, 'w') as file:
            file.write(content)

    def build_job_name_identifier_postfix(self, **kwargs):
        raise NotImplementedError

    def spawn(self):
        for shfilename in self.shfilenames:
            spawncommand = self.build_spawn_command(shfilename=shfilename)
            subprocess.run(spawncommand)
            print(f"Spawned {spawncommand}")

    def build_shfile_body(self, job_name_identifier, **kwargs):
        header = self.build_slurm_header(job_name_identifier=job_name_identifier)
        precommands = self.build_precommands()
        maincommand = self.build_maincommand(**kwargs)
        fullcommand = header + precommands + maincommand
        return fullcommand


class SlurmBuilder(AbstractSlurmBuilder):
    def __init__(
            self,
            job_name: str,
            mail_user: str,
            base_command: str,
            partition: str = "cpu_normal",
            time: str = "48:00:00",
            mem_per_cpu: str = "1000",
            mail_type: str = "ALL",
            seeds: int = np.arange(0, 10, dtype=np.int),
            num_episodes: int = 10,
            outdir: str = "data/output",
            act_conda: str = "",  # "source /opt/conda/etc/profile.d/conda.sh\nconda activate py38\n",
            runscript_outdir: str = "runscripts/generated",
            **kwargs
    ):
        pre_command = act_conda
        post_command = ""
        super().__init__(
            job_name=job_name,
            mail_user=mail_user,
            base_command=base_command,
            pre_command=pre_command,
            post_command=post_command,
            partition=partition,
            time=time,
            mem_per_cpu=mem_per_cpu,
            mail_type=mail_type,
            runscript_outdir=runscript_outdir,
            ** kwargs
        )
        self.seeds = seeds
        self.num_episodes = num_episodes
        self.mail_user = mail_user
        self.mail_type = mail_type
        self.partition = partition
        self.job_name = job_name
        self.time = time
        self.mem_per_cpu = mem_per_cpu
        self.outdir = outdir
        self.base_command = base_command
        self.act_conda = act_conda

    def build_file(self):
        pass

    def register_maincommand(self, name):
        if type(name) == str:
            self.main_arg_names.append(name)

    def build_maincommand(self, seed: int, num_episodes: int, outdir: str):
        maincommand = ""
        maincommand += self.base_command
        maincommand += f" --seeds {seed}"
        maincommand += f" --num_episodes {num_episodes}"
        maincommand += f" --outdir {outdir}"
        return maincommand

    def build_job_name_identifier_postfix(self, seed: int, **kwargs):
        return f"neps{self.num_episodes}_s{seed}"

    def build_shfiles(self):
        for seed in self.seeds:
            # build bash file
            job_name_identifier = self.build_job_name_identifier_postfix(seed=seed)
            fullcommand = self.build_shfile_body(job_name_identifier=job_name_identifier, seed=seed, outdir=self.outdir, num_episodes=self.num_episodes)

            # write bash file
            shfilename = self.build_shfilename(job_name_identifier=job_name_identifier)
            shfilename = Path(shfilename)
            shfilename.parent.mkdir(parents=True, exist_ok=True)
            with open(shfilename, 'w') as file:
                file.write(fullcommand)
            self.shfilenames.append(shfilename)
            print(f"Built '{shfilename}'")

        self.write_spawnlist()


def get_parser():
    parser = ArgumentParser()

    parser.add_argument(
        "--seeds",
        nargs="+",
        type=int,
        default=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        help="Seeds for evaluation",
    )

    parser.add_argument(
        "--num_episodes",
        type=int,
        default=10,
        help="Number of episodes to evaluate policy on",
    )

    parser.add_argument(
        "--mail_user",
        default="benjamin@tnt.uni-hannover.de",
        type=str,
        help="Email for slurm"
    )

    parser.add_argument(
        "--mail_type",
        type=str,
        default="ALL",
    )

    parser.add_argument(
        "--partition",
        type=str,
        default="cpu_normal",
    )

    parser.add_argument(
        "--job_name",
        type=str,
    )

    parser.add_argument(
        "--time",
        type=str,
        default="48:00:00",
    )

    parser.add_argument(
        "--mem_per_cpu",
        type=str,
        default="1000M",
        help="memory per CPU in MB"
    )

    parser.add_argument(
        "--outdir",
        type=str,
        default="output",
    )

    parser.add_argument(
        "--base_command",
        type=str
    )

    return parser


def main(args=None):
    parser = get_parser()
    args = parser.parse_args(args)

    slurmbuilder = SlurmBuilder(
        job_name=args.job_name,
        mail_user=args.mail_user,
        seeds=args.seeds,
        num_episodes=args.num_episodes,
        partition=args.partition,
        time=args.time,
        mem_per_cpu=args.mem_per_cpu,
        mail_type=args.mail_type,
        outdir=args.outdir,
        base_command=args.base_command
    )
    slurmbuilder.build_shfiles()
    # slurmbuilder.spawn()


if __name__ == "__main__":
    basecommands = {
        "autosklearn_NASA": "python train.py --dataset_names NASA --budgets 600 --exp_id automl_600 --nested_resampling 10 --n_folds_cv 10  --output_dir results --n_jobs 8 ",
    }
    for job_name, basecommand in basecommands.items():
        args = [
            "--job_name", job_name,
            "--outdir", "data/results/baselines",
            "--seeds", "0", "1", "2", "3", "4", #"5", "6", "7", "8", "9",  # "10",
            "--num_episodes", "10",
            "--base_command", basecommand,
            "--time", "24:00:00",
            "--mem_per_cpu", "8000M",
        ]
        main(args)
