import os
from setuptools import setup, find_packages # type: ignore

setup(
    name="query-sheriff",
    version="1.0.0",
    description="A tool to inspect and optimize Django ORM queries.",
    long_description=open('README.md').read() if os.path.exists('README.md') else '',
    long_description_content_type="text/markdown",
    author="Rabut Christopher",
    author_email="n42611750@gmail.com",
    url="https://github.com/Topherchriss/query-sheriff",
    packages=find_packages(include=["query_sheriff", "query_sheriff.*"], exclude=["test_query_sheriff*", "tests*"]),
    include_package_data=True,
    install_requires=[
        'Django>=3.2',
    ],

    tests_require=[
        'pytest',
        'pytest-django',
    ],
    entry_points={
        'console_scripts': [
            'run_tests = pytest_runner:main',
            'query-inspector = query_sheriff.inspector.cli:cli',
        ],
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Framework :: Django',
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.7',
)
