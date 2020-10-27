# -------- DEPRECATED -----------
#
# This package has been deprecated and will be removed soon. Everything in it
# is now part of web-monitoring-diff.
#
#   https://github.com/edgi-govdata-archiving/web-monitoring-diff
#   https://pypi.org/project/web-monitoring-diff/
#
# See also: https://github.com/edgi-govdata-archiving/web-monitoring-processing/issues/638
#

class UndiffableContentError(ValueError):
    """
    Raised when the content provided to a differ is incompatible with the
    diff algorithm. For example, if a PDF was provided to an HTML differ.
    """

class UndecodableContentError(ValueError):
    """
    Raised when the content downloaded for diffing could not be decoded.
    """
