from setuptools import setup

with open("README.md", "r") as readme:
    long_description = readme.read()


setup(
    # long_description=long_description, package_dir={"": "src"}, py_modules=["aioconveyor"]
    long_description=long_description
)
