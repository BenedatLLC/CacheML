import setuptools

with open("README.rst", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="directml",
    version="0.0.1",
    author="Jeff Fischer",
    author_email="jeff.fischer@benedat.com",
    description="Enhanced caching and encryption of ML source data",
    long_description=long_description,
    long_description_content_type="text/restructuredtext",
    url="https://github.com/BenedatLLC/DirectML",
    packages=setuptools.find_packages(),
    #scripts=['bin/dml'],
    classifiers=[
        "Programming Language :: Python :: 3",
        #"License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
