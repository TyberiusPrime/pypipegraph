from distutils.core import setup

setup(
    name='pypipegraph',
    version='0.151',
    packages=['pypipegraph',],
    license='MIT',
    url='http://code.google.com/p/pypipegraph/',
    author='Florian Finkernagel',
    author_email='finkernagel@imt.uni-marburg.de',
    description = "A workflow (job) engine/pipeline for bioinformatics and scientific computing.",
    long_description=open('README.txt').read(),
    classifirs = [
      'Programming Language :: Python',
      'Programming Language :: Python :: 3'
      ]
)
