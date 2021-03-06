import numpy as np
from pathlib import Path
import subprocess
import itertools
from typing import Dict

class SlurmBuilder(object):
    """
    Build slurm files containing commands and slurm options.

    The slurm bash files are save in `runscript_outdir`. In this directory a file with name `runcommands.sh` is generated.
    It contains the commands to start each generated bash file. The slurm jobs can thus be started as follows for all
    generated files: `bash runcommands.sh`.

    A generated bash file can look as follows::

    #!/bin/bash
    #
    #SBATCH --mail-user=some_mail@bubbib.com
    #SBATCH --mail-type=ALL
    #SBATCH --job-name=my_slurm_job_s6_neps16
    #SBATCH --time=48:00:00
    #SBATCH --partition=cpu_normal      # Partition auf der gerechnet werden soll. Ohne Angabe des Parameters wird auf der
                                        #   Default-Partition gerechnet. Es können mehrere angegeben werden, mit Komma getrennt.
    #SBATCH --nodes=2                   # Reservierung von 2 Rechenknoten
                                        #   alle nachfolgend reservierten CPUs müssen sich auf den reservierten Knoten befinden
    #SBATCH --tasks-per-node=4          # Reservierung von 4 CPUs pro Rechenknoten
    #SBATCH --mem-per-cpu=1000
    #
    echo precommand
    echo basecommand: Hello World --seeds 6 --num_episodes 16
    echo postcommand


    """
    def __init__(
            self,
            slurm_config: Dict[str, str],
            base_command: str,
            pre_command: str = "",
            post_command: str = "",
            output_filename: str = "",
            runscript_outdir: str = "runscripts/generated",
            runcommands_file_precommand: str = "",
            iteration_list: list = [],
    ):
        """
        Get information.
        For more information on slurm parameters see the
        `slurm wiki page <https://tntintern/wikitnt/index.php/TNT_Cluster_%C3%9Cbersicht>`.
        Parameters with empty values will not be written to the slurm file.

        Parameters
        ----------
        slurm_config : Dict[str, str]
            Dictionary containing the slurm configuration. E.g.:

                    {
                    'job-name': "my_slurm_job",
                    "mail-user": "hihi@hehe.hoho",
                    "mail-type": 'ALL',
                    "array": "0-4",
                    "time": "24:00:00",
                    "cpus-per-task": "1",
                    }
        base_command : str
            Command which should be executed. Appended with names and values from `iteration_list`.
        pre_command : str, default=""
            Everything that should be executed before `base_command`. Should contain intermediate newlines if applicable.
        post_command : str, default=""
            Everything that should be executed after `base_command`. Should contain intermediate newlines if applicable.
        output_filename : str, default=""
            STDOUT and STDERR will be directed to this filename. The placeholder %j will be replaced by the slurm
            job-id. If this parameter is not set, all output will be directed into the file slurm-<JobID>.out in the
            ROOT directory. If the slurm logs should be written to a subdirectory of root, the subdirectories must
            exist.
        runscript_outdir : str, default="runscripts/generated"
            Where to save the slurm scripts.
        iteration_list : list[dict], default=[]
            Build a seperate bash file based on each entry in this list. The entries are command line arguments. An
            entry should be of type `dict` and look as follows::

                entry = {
                    "name": "seeds",  # string
                    "id": "s",  # string
                    "values": [0, 1, 2, 3, 4]   # list; elements must be convertible to string
                }

            This example entry would generate 5 scripts with ' --seeds entry["values"][i]' appended to the basecommand.
            The "id" field is used to add the information to the filename. For seed 3 this would add "_s3" to the file-
            name and job name.
        runcommands_file_precommand : str, default=""
            Will be written before the sbatch commands in runcommands.sh. E.g., on LUIS you could write 'git pull\n'.

        """
        self.base_command = base_command
        self.output_filename = output_filename
        self.pre_command = pre_command
        self.post_command = post_command
        self.iteration_list = iteration_list
        self.runcommands_file_precommand = runcommands_file_precommand

        self.template_slurm_config = "#SBATCH --{command_name}={command_value}\n"
        self.runscript_outdir = runscript_outdir
        self.runlist_fname = Path(self.runscript_outdir) / "runcommands.sh"

        self.slurm_config = slurm_config

        self.job_name = self.slurm_config.get("job-name", "myjob")

        self.main_command_template = " --{name} {value}"
        self.main_arg_names = []
        self.shfilenames = []
        self.maincommands = []

    def build_slurm_header(self, job_name_identifier: str = ""):
        """
        Build slurm header specifying slurm options.

        Parameters
        ----------
        job_name_identifier : str, default=""
            Is appended to job_name.

        Returns
        -------
        header : str
            slurm header with slurm options

        """
        header = ""
        header += "#!/bin/bash\n"
        header += "#\n"

        for command_name, command_value in self.slurm_config.items():
            if command_name == "job-name" and len(job_name_identifier) > 0:
                command_value += f"_{job_name_identifier}"
            if command_value:
                header += self.template_slurm_config.format(command_name=command_name, command_value=command_value)
        header += "\n"
        return header

    def build_precommands(self):
        """
        Build precommands.

        Returns
        -------
        str
            precommands
        """
        commands = ""
        commands += self.pre_command
        return commands

    def build_postcommands(self):
        """
        Build postcommands.

        Returns
        -------
        str
            postcommands
        """
        commands = ""
        commands += self.post_command
        return commands

    def build_shfilename(self, job_name_identifier: str = ""):
        """
        Build bash filenames.

        Format of filename: `f"{self.runscript_outdir}/run_{self.job_name}_{job_name_identifier}.sh"`

        Parameters
        ----------
        job_name_identifier : str, default=""
            Job name id specific to run.

        Returns
        -------
        str
            bash filename

        """
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
        content += self.runcommands_file_precommand
        for shfilename in self.shfilenames:
            spawncommand = self.build_spawn_command(shfilename=shfilename, to_args=False)
            content = content + spawncommand + "\n"
        with open(self.runlist_fname, 'w') as file:
            file.write(content)

    def spawn(self):
        for shfilename in self.shfilenames:
            spawncommand = self.build_spawn_command(shfilename=shfilename)
            subprocess.run(spawncommand)
            print(f"Spawned {spawncommand}")

    def build_shfile_body(self, job_name_identifier, **kwargs):
        """
        Build complete body of bash file.

        Header, precommands, basecommand with optional command line arguments, postcommands.

        Parameters
        ----------
        job_name_identifier : str, default=""
            Job name id specific to run.
        kwargs : dict
            Keyword arguments that are going be rendered as command line arguments for the basecommand as follows:
            `f"--{key} {value}"`.

        Returns
        -------
        str
            Formatted content of the bash file.

        """
        header = self.build_slurm_header(job_name_identifier=job_name_identifier)
        precommands = self.build_precommands()
        maincommand = self.build_maincommand(**kwargs)
        postcommands = self.build_postcommands()
        fullcommand = header + "\n" + precommands + "\n" + maincommand + "\n" + postcommands
        return fullcommand

    def build_maincommand(self, **kwargs):
        """
        Build main command which is the `base_command` with optional command line arguments.

        Parameters
        ----------
        kwargs : dict
            Keyword arguments that are going be rendered as command line arguments for the basecommand as follows:
            `f"--{key} {value}"`.

        Returns
        -------
        str
            main command

        """
        maincommand = ""
        maincommand += self.base_command
        for key, val in kwargs.items():
            maincommand += f" --{key} {val}"
        self.maincommands.append(maincommand)
        return maincommand

    def build_shfiles(self):
        """
        Build all bash files and save them to given output directory.

        Build all possible combinations of values of `iteration_list` as single bash files. A file `runcommands.sh` is
        written to the output directory containing all commands to slurmstart all single bash files.

        Returns
        -------
        None

        """
        arg_names = [v["name"] for v in self.iteration_list]
        arg_ids = [v["id"] for v in self.iteration_list]
        arg_vals = [v["values"] for v in self.iteration_list]

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
            job_id = job_id.replace(" ", "__")
            kwargs = {arg_name: value for arg_name, value in zip(arg_names, combo)}
            fullcommand = self.build_shfile_body(job_name_identifier=job_id, **kwargs)

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
        slurm_config={
            'job-name': "my_slurm_job",
            "mail-user": "hihi@hehe.hoho",
            "mail-type": 'ALL',
        },
        # job_name="my_slurm_job",
        # mail_user="some_mail@bubbib.com",
        pre_command="echo setting env",
        base_command="echo Hello World",
        post_command="echo cleaning up",
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
