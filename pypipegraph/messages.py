from twisted.protocols import amp

class TransmitPipegraph(amp.Command):
    arguments = [
            ('jobs', amp.String())
            ]
    response = [('ok', amp.Boolean()), ('exception', amp.String())]

class StartJob(amp.Command):
    arguments = [
            ('job_id', amp.String())
            ]
    response = []

class ShutDown(amp.Command):
    arguments = []
    response = []

class JobEnded(amp.Command):
    arguments = [
            ('arg_tuple_pickle', amp.String())
            ]
    response = [('ok', amp.Boolean())]


