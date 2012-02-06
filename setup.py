from distutils.core import setup

setup(
    name='pypipegraph',
    version='0.128',
    packages=['pypipegraph',],
    license='MIT',
    url='http://code.google.com/p/pypipegraph/',
    author='Florian Finkernagel',
    author_email='finkernagel@imt.uni-marburg.de',
    description = "A workflow (job) engine/pipeline for bioinformatics.",
    long_description=open('README.txt').read(),
)
