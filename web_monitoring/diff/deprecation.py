import warnings


def warn_deprecation():
    warnings.warn('The web_monitoring.diff package has been deprecated and '
                  'will no longer be updated! Please switch to the new '
                  '`web-monitoring-diff` package at: '
                  'https://pypi.org/project/web-monitoring-diff/',
                  DeprecationWarning,
                  stacklevel=2)
