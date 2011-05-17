
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

class RuntimeException(PyPipelineGraphError):
    """Something went wrong with the pipegraph, a bug"""

class JobContractError(PyPipelineGraphError):
    """One of the jobs did not confirm to it's supposed behaviour""" 
    pass

class JobDied(PyPipelineGraphError):
    """A job went away without signing off"""
    def __init__(self, exit_code):
        self.exit_code = exit_code
        msg = "Exitcode was %s" % self.exit_code
        PyPipelineGraphError.__init__(self, msg)

class CommunicationFailure(PyPipelineGraphError):
    """something went wrong talking to a slave"""
    pass
