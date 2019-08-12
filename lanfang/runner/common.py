import enum


class TaskStatus(enum.Enum):
  DISABLED = 0
  WAITING  = 1
  READY    = 2
  RUNNING  = 3
  DONE     = 4
  FAILED   = 5
  KILLED   = 6
  CANCELED = 7

