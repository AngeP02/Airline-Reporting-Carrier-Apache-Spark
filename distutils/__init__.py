# distutils/__init__.py

try:
    from setuptools._distutils import *
except ImportError:
    raise ImportError("setuptools._distutils non è disponibile. Assicurati di avere installato setuptools aggiornato.")
