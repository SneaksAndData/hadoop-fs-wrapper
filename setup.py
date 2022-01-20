import subprocess
import setuptools


def get_version():
    base_version = subprocess.check_output(["git", "describe", "--tags", "--abbrev=7"]).strip().decode("utf-8")
    # have to follow PEP440 religious laws here
    parts = base_version.split('-')
    if len(parts) == 1:
        return parts[0]
    else:
        (semantic, commit_number, commit_id) = parts
        return f"{semantic}+{commit_number}.{commit_id}"


setuptools.setup(name='hadoop-fs-wrapper',
                 version=get_version(),
                 description='Python Wrapper for Hadoop Java API',
                 author='ECCO Sneaks & Data',
                 author_email='esdsupport@ecco.com',
                 classifiers=[
                     "Programming Language :: Python :: 3",
                     "License :: OSI Approved :: MIT License",
                     "Operating System :: OS Independent",
                 ],
                 python_requires='>=3.8',
                 package_dir={"": "src"},
                 packages=setuptools.find_packages(where="src"), )
