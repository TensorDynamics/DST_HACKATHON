import pathlib
from setuptools import setup
import pkg_resources


HERE = pathlib.Path(__file__).parent
with pathlib.Path('requirements.txt').open() as requirements_txt:
    install_requires = [str(requirement)
                        for requirement
                        in pkg_resources.parse_requirements(requirements_txt)]

setup(
    name='TD-Satellite',
    version='0.1.0',
    description='Satellite based nowcasting',
    author='Vasudev Gupta',
    author_email='guptavasudelhi@gmail.com',
    license='MIT',
    # packages=['src'],
    zip_safe=False,
    install_requires=[
        install_requires
    ],
)
