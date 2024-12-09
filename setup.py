from setuptools import find_packages, setup

setup(
    name="lichess_live_win_probability",
    packages=find_packages(exclude=["lichess_live_win_probability_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
