# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import shutil
import os
from datetime import timedelta

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet.encryption as pe
from pyarrow.tests.parquet.encryption import InMemoryKmsClient

""" A sample to demonstrate parquet dataset encryption and decryption"""

# create a list of dictionaries that will represent our dataset
table = pa.table({'year': [2020, 2022, 2021, 2022, 2019, 2021],
                  'n_legs': [2, 2, 4, 4, 5, 100],
                  'animal': ["Flamingo", "Parrot", "Dog", "Horse",
                             "Brittle stars", "Centipede"]})

# create a PyArrow dataset from the table
dataset = ds.dataset(table)

FOOTER_KEY = b"0123456789112345"
FOOTER_KEY_NAME = "footer_key"
COL_KEY = b"1234567890123450"
COL_KEY_NAME = "col_key"

encryption_config = pe.EncryptionConfiguration(
    footer_key=FOOTER_KEY_NAME,
    plaintext_footer=False,
    # Use COL_KEY_NAME to encrypt `n_legs` and `animal` columns.
    column_keys={
        COL_KEY_NAME: ["n_legs", "animal"],
    },
    encryption_algorithm="AES_GCM_V1",
    # requires timedelta or an assertion is raised
    cache_lifetime=timedelta(minutes=5.0),
    data_key_length_bits=256)

kms_connection_config = pe.KmsConnectionConfig(
    custom_kms_conf={
        FOOTER_KEY_NAME: FOOTER_KEY.decode("UTF-8"),
        COL_KEY_NAME: COL_KEY.decode("UTF-8"),
    }
)

decryption_config = pe.DecryptionConfiguration(cache_lifetime=300)


def kms_factory(kms_connection_configuration):
    return InMemoryKmsClient(kms_connection_configuration)


crypto_factory = pe.CryptoFactory(kms_factory)
parquet_encryption_cfg = ds.ParquetEncryptionConfig(
    crypto_factory, kms_connection_config, encryption_config)
parquet_decryption_cfg = ds.ParquetDecryptionConfig(crypto_factory,
                                                    kms_connection_config,
                                                    decryption_config)

# set encryption config for parquet fragment scan options
pq_scan_opts = ds.ParquetFragmentScanOptions()
pq_scan_opts.parquet_decryption_config = parquet_decryption_cfg
pformat = pa.dataset.ParquetFileFormat(default_fragment_scan_options=pq_scan_opts)

if os.path.exists('sample_dataset'):
    shutil.rmtree('sample_dataset')

write_options = pformat.make_write_options(
    encryption_config=parquet_encryption_cfg)

ds.write_dataset(data=dataset, base_dir="sample_dataset",
                 partitioning=['year'], format=pformat, file_options=write_options)
# read the dataset back
dataset = ds.dataset('sample_dataset', format=pformat)

# print the dataset
print(dataset.to_table())
