from setuptools import setup

setup(name='jpbber',
      version='0.1',
      description='Finer grain job management for HPC',
      url='',
      author='Eric Kort',
      author_email='eric.kort@vai.org',
      license='MIT',
      packages=['jobber'],
      install_requires=[
          'watchdog',
      ],
      zip_safe=False)