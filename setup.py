from setuptools import setup, find_packages

setup(
    name='esprit',
    version='0.0.2',
    packages=find_packages(),
    install_requires=[
        "requests",
    ],
    url='http://cottagelabs.com/',
    author='Cottage Labs',
    author_email='us@cottagelabs.com',
    description='esprit - ElasticSearch: Put Records In There!',
    license='Copyheart',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: Copyheart',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ],
)
