from setuptools import setup

setup(
    name='slurmbuilder',
    version='0.0.0',
    description='Quickly build slurm batch files',
    #url='https://github.com/shuds13/pyexample',
    author='Carolin Benjamins',
    author_email='benjamins@tnt.uni-hannover.de',
    #license='BSD 2-clause',
    packages=['slurmbuilder'],
    install_requires=[
        'numpy',
        # 'itertools',
        # 'pathlib',
        # 'subprocess',
        ],

    # classifiers=[
    #     'Development Status :: 1 - Planning',
    #     'Intended Audience :: Science/Research',
    #     'License :: OSI Approved :: BSD License',
    #     'Operating System :: POSIX :: Linux',
    #     'Programming Language :: Python :: 2',
    #     'Programming Language :: Python :: 2.7',
    #     'Programming Language :: Python :: 3',
    #     'Programming Language :: Python :: 3.4',
    #     'Programming Language :: Python :: 3.5',
    # ],
)
