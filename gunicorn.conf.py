# Fix: Force 1 worker + multiple threads so scanner and HTTP share the same memory.
# With multiple workers, each worker has its own scan_state/signals → dashboard is dead.

workers = 1
worker_class = "gthread"
threads = 4
bind = "0.0.0.0:8000"
timeout = 120
keepalive = 5
