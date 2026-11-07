"""service access entry points."""

from .authorization import PermissionService
from .sharing import ShareService

__all__ = ['PermissionService', 'ShareService']
