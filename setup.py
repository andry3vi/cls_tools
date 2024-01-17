from setuptools import setup

setup(name='clstools',
      version='0.2',
      description='set of tool to load and sort IGISOL cls data',
      url='https://gitlab.jyu.fi/araggio/cls_tools',
      author='Oiggart',
      author_email='andrea.a.raggio@jyu.fi',
      license='GPL-3.0',
      packages=['clstools'],
      install_requires=[
          'pandas',
          'scipy',
          'dask[complete]'
      ],
      zip_safe=False)