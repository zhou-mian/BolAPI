import setuptools

setuptools.setup(
    name='BolAPI',
    version='1.0.0',
    author='Miranda Zhou',
    author_email='mzhou.ai@gmail.com',
    url='https://github.com/zhou-mian/BolAPI',
    packages=['BolAPI'],
    install_requires=[
        'pandas',
        'requests',
        'numpy',
        'Pillow',
        'openpyxl'
    ]
)