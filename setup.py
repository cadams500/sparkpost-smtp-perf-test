from setuptools import setup, find_packages

setup(
    name="smtp-perf-test",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "python-dotenv>=0.19.0",
    ],
    python_requires=">=3.7",
    author="Your Name",
    author_email="your.email@example.com",
    description="A tool for testing SMTP performance with SparkPost",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
) 