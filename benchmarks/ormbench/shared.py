"""Slots of the shared memory the race processes coordinate through, one int64 each."""

# The version the writer last committed.
COMMITTED = 0
# WAIT, RUN or STOP.
PHASE = 1
# One flag per process from here on, set when the process is ready to run.
READY = 2

WAIT, RUN, STOP = 0, 1, 2
