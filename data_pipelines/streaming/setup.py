import setuptools

setuptools.setup(
    name='real_time_fraud_detection_pipeline',
    version='0.0.1',
    install_requires=[
        'apache-beam[gcp]==2.66.0',
        'pyarrow==18.0.0',
        'google-cloud-firestore==2.21.0',
        'google-cloud-storage==2.19.0',
        'google-cloud-pubsub==2.19.0',
        'geopy==2.4.1',
        'requests'
    ],
    packages=setuptools.find_packages(),
    # entry_points={
    #     'console_scripts': [
    #         'run_pipeline=src.pipeline:main',  # REQUIRED for Flex Templates
    #     ]
    # }
)