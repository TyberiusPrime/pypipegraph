import multiprocessing
try:
    import Queue
    queue = Queue
except:
    import queue
q = multiprocessing.Queue()
q.put("Hello")
while True:
	try:
		q.get(timeout=5)
		break
	except (queue.Empty):
		print ('timeout')
print ('leaving - all OK')
