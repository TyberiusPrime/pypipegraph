# -*- coding: utf-8 -*-

__version__ = "0.190"  # test enforces this to be in sync with setup.cfg

from .graph import run_pipegraph, new_pipegraph
from .ppg_exceptions import (
    RuntimeError,
    RuntimeException,
    CycleError,
    JobContractError,
    PyPipeGraphError,
    JobDiedException,
    NothingChanged,
)
from . import util

inside_ppg = util.inside_ppg

from .job import (
    Job,
    JobList,
    FileGeneratingJob,
    MultiFileGeneratingJob,
    DataLoadingJob,
    AttributeLoadingJob,
    TempFileGeneratingJob,
    TempFilePlusGeneratingJob,
    CachedAttributeLoadingJob,
    CachedDataLoadingJob,
    PlotJob,
    CombinedPlotJob,
    FunctionInvariant,
    ParameterInvariant,
    FileTimeInvariant,
    FileChecksumInvariant,
    RobustFileChecksumInvariant,
    FileInvariant,
    MultiFileInvariant,
    JobGeneratingJob,
    DependencyInjectionJob,
    FinalJob,
    MemMappedDataLoadingJob,
    MultiTempFileGeneratingJob,
    NotebookJob,
)

assert_uniqueness_of_object = util.assert_uniqueness_of_object

all = [
    run_pipegraph,
    new_pipegraph,
    RuntimeError,
    RuntimeException,
    CycleError,
    JobContractError,
    PyPipeGraphError,
    JobDiedException,
    NothingChanged,
    Job,
    JobList,
    FileGeneratingJob,
    MultiFileGeneratingJob,
    DataLoadingJob,
    AttributeLoadingJob,
    TempFileGeneratingJob,
    TempFilePlusGeneratingJob,
    CachedAttributeLoadingJob,
    CachedDataLoadingJob,
    PlotJob,
    CombinedPlotJob,
    FunctionInvariant,
    ParameterInvariant,
    FileTimeInvariant,
    FileChecksumInvariant,
    RobustFileChecksumInvariant,
    MultiFileInvariant,
    FileInvariant,
    JobGeneratingJob,
    DependencyInjectionJob,
    FinalJob,
    MemMappedDataLoadingJob,
    util,
    inside_ppg,
    MultiTempFileGeneratingJob,
    NotebookJob,
]
