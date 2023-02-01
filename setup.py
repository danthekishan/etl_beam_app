import setuptools


REQUIRED_PACKAGES = [
    "pydantic",
]

setuptools.setup(
    name="etl_beam_app",
    version="0.0.1",
    author="Daniel Kishan",
    description="Basic ETL framework",
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
)
