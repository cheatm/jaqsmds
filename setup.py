from setuptools import setup, find_packages


REQUIRES = []

for line in open("requirements.txt").read().split("\n"):
    if "git+" not in line:
        REQUIRES.append(line)


setup(
    name="jaqsmds",
    version="0.0.1",
    packages=find_packages(),
    install_requires=REQUIRES,
    entry_points={"console_scripts": ["jaqsmds = jaqsmds.entry_point:group"]},
    license="Apache License v2",
    author="Cam",
    author_email="cam@fxdayu.com"
)
