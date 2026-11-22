from setuptools import setup
import pathlib

here = pathlib.Path(__file__).parent.resolve()

setup(
    name='orthanc_tools',
    version='0.23.3',
    description='Python Orthanc Tools',
    long_description=(here / 'README.md').read_text(encoding='utf-8'),
    long_description_content_type='text/markdown',
    url='https://github.com/rczobor/python-orthanc-tools',
    author='Orthanc Team',
    author_email='info@orthanc.team',
    license_expression='LGPL-3.0-only',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',
        'Programming Language :: Python :: 3.14',
        'Programming Language :: Python :: 3.15',
        'Programming Language :: Python :: 3 :: Only',
    ],
    keywords='orthanc, dicom, rest api, hl7',
    packages=["orthanc_tools", "orthanc_tools.hl7Lib", "orthanc_tools.helpers"],
    python_requires='>=3.11, <4',
    install_requires=(here / 'requirements.txt').read_text(encoding='utf-8').splitlines(),
    project_urls={
        'Bug Reports': 'https://github.com/rczobor/python-orthanc-tools/issues',
        'Funding': 'https://orthanc.team',
        'Source': 'https://github.com/rczobor/python-orthanc-tools/',
    },
)
