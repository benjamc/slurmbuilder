import numpy as np
from pathlib import Path
import subprocess
import itertools


class SlurmBuilder(object):
    def __init__(
            self,
            job_name: str,
            mail_user: str,
            base_command: str,
            pre_command: str = "",
            post_command: str = "",
            partition: str = "cpu_normal",
            time: str = "48:00:00",
            mem_per_cpu: str = "1000",
            mail_type: str = "ALL",
            runscript_outdir: str = "runscripts/generated",
            iteration_list: list = [],
            **kwargs
    ):
        self.mail_user = mail_user
        self.base_command = base_command
        self.mail_type = mail_type
        self.partition = partition
        self.job_name = job_name
        self.time = time
        self.mem_per_cpu = mem_per_cpu
        self.pre_command = pre_command
        self.post_command = post_command
        self.iteration_list = iteration_list

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

    def build_maincommand(self, **kwargs):
        maincommand = ""
        maincommand += self.base_command
        for key, val in kwargs.items():
            maincommand += f" --{key} {val}"
        return maincommand

    def build_shfiles(self):
        arg_names = [v["name"] for v in self.iteration_list]
        arg_ids = [v["id"] for v in self.iteration_list]
        arg_vals = [v["values"] for v in self.iteration_list]
        print(arg_names, arg_ids, arg_vals)

        job_name_id_postfix_template = "{}_" * len(self.iteration_list)
        job_name_id_postfix_template = job_name_id_postfix_template[:-1]  # remove last underscore
        print(job_name_id_postfix_template)
        fstr_templ = "{}_"

        all_combos = itertools.product(*arg_vals)
        for combo in all_combos:
            job_id = ""
            for arg_id, value in zip(arg_ids, combo):
                if isinstance(value, (list, tuple, np.ndarray)):
                    v = value[0]
                else:
                    v = value
                job_id += f"{arg_id}{v}_"
            job_id = job_id[:-1]  # remove last underscore
            print(job_id)
            kwargs = {arg_name: value for arg_name, value in zip(arg_names, combo)}
            print(kwargs)
            fullcommand = self.build_shfile_body(job_name_identifier=job_id, **kwargs)
            print(fullcommand)

            # write bash file
            shfilename = self.build_shfilename(job_name_identifier=job_id)
            shfilename = Path(shfilename)
            shfilename.parent.mkdir(parents=True, exist_ok=True)
            with open(shfilename, 'w') as file:
                file.write(fullcommand)
            self.shfilenames.append(shfilename)
            print(f"Built '{shfilename}'")

        self.write_spawnlist()


if __name__ == "__main__":
    sbuilder = SlurmBuilder(
        job_name="my_slurm_job",
        mail_user="some_mail@bubbib.com",
        base_command="echo Hello World",
        iteration_list=[
            {
                "name": "seeds",
                "id": "s",
                "values": [0, 4, 6, 8]
            },
            {
                "name": "num_episodes",
                "id": "neps",
                "values": [14, 16]
            }
        ]
    )
    sbuilder.build_shfiles()
