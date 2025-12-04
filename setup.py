from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="atscale-gatling",
    version="1.0.0",
    author="Rudy WIdjaja",
    author_email="rwidjaja@hotmail.com",
    description="AtScale Gatling Performance Testing Tool",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/rwidjaja/atscale-gatling",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Java",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Testing",
        "Topic :: System :: Benchmark",
    ],
    python_requires=">=3.7",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "atscale-gatling=main:main",
            "atscale-check=utils.dependency_checker:main",
        ],
    },
    include_package_data=True,
)