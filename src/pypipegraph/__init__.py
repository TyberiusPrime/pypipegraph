# -*- coding: utf-8 -*-
from pkg_resources import get_distribution, DistributionNotFound

try:
    # Change here if project is renamed and does not equal the package name
    dist_name = __name__
    __version__ = get_distribution(dist_name).version
except DistributionNotFound:  # pragma: no cover
    __version__ = "unknown"
finally:
    del get_distribution, DistributionNotFound

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
    verify_job_id
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
