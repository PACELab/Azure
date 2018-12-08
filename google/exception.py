class AlgoException(Exception):
    """Exception is raised if scheduling algorithm fails"""

class OutOfResourceException(AlgoException):
    """Exception is raised if there is not enough resources to allocate on ony of the servers"""