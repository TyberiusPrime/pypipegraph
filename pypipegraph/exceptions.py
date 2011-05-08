
class PyPipelineGraphError(ValueError):
    """Base class for all PyPipelineGraph exceptions"""
    pass

class CycleError(PyPipelineGraphError):
    """You created a cycle in your pipegraph, 
    this is not supported"""
    pass

class RuntimeError(PyPipelineGraphError):
    """A job died"""
    pass

class JobContractError(PyPipelineGraphError):
    """One of the jobs did not confirm to it's supposed behaviour""" 
    pass
