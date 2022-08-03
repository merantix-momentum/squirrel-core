from typing import List, Optional, Tuple

import numpy as np
import nvidia.dali.fn as fn
import pytest
import torch
from nvidia.dali import pipeline_def
from nvidia.dali.pipeline import DataNode
from nvidia.dali.plugin.pytorch import DALIGenericIterator
from squirrel.iterstream.dali_composables import (
    DaliExternalSource,
    default_dict_collate,
)
from squirrel.iterstream.source import IterableSource

NUM_SAMPLES = 1000
BATCH_SIZE = 16
NUM_THREADS = 2
DATA_KEY = "img"
LABEL_KEY = "label"
DATA_DIM = (100, 100, 3)
DATA_RANGE = (0, 255)
LABEL_RANGE = (0, 1)


@pytest.fixture(scope="module", autouse=True)
def samples() -> List[int]:
    """Fixture for this module's test data"""
    data = []
    for _ in range(NUM_SAMPLES):
        data.append(
            {
                DATA_KEY: np.random.randint(*DATA_RANGE, size=DATA_DIM),
                LABEL_KEY: np.random.randint(*LABEL_RANGE),
            }
        )
    return data


@pipeline_def
def pipeline(it: DaliExternalSource, device: str) -> Tuple[DataNode]:
    """A dummy DALI classification pipeline defining the data processing graph.

    Returns:
        Tuple[DataNode]: The outputs of the operators.
    """
    img, label = fn.external_source(source=it, num_outputs=2, device=device)
    enhanced = fn.brightness_contrast(img, contrast=2)
    return enhanced, label


def get_classification_dali_it(
    it: DaliExternalSource, device: str, device_id: Optional[int] = None
) -> DALIGenericIterator:
    pipe = pipeline(it, device, batch_size=BATCH_SIZE, num_threads=NUM_THREADS, device_id=device_id)
    pipe.build()
    return DALIGenericIterator([pipe], [DATA_KEY, LABEL_KEY])


def check_dali_iterator(dali_iter: DALIGenericIterator, device: str):
    counter = 0
    for item in dali_iter:
        # access data of first pipeline
        data = item[0]
        counter += data[DATA_KEY].shape[0]
        effective_batch_size = data[LABEL_KEY].shape[0]
        # check format and if on correct device
        assert type(data) == dict
        assert type(data[DATA_KEY]) == torch.Tensor
        assert data[DATA_KEY].shape[1:] == DATA_DIM
        assert data[DATA_KEY].device == torch.device(device)
        assert effective_batch_size == BATCH_SIZE or effective_batch_size == NUM_SAMPLES % BATCH_SIZE
    assert counter == NUM_SAMPLES


def test_dali_external_source_gpu(samples: List[int]) -> None:
    """Test for DaliExternalSource composable on gpu"""
    it = IterableSource(samples).compose(DaliExternalSource, BATCH_SIZE, default_dict_collate)
    dali_iter = get_classification_dali_it(it, device="gpu", device_id=0)
    check_dali_iterator(dali_iter, "cuda:0")


def test_to_dali_external_source_gpu(samples: List[int]) -> None:
    """Test for to_dali_external_source composable on gpu"""
    it = IterableSource(samples).to_dali_external_source(BATCH_SIZE, default_dict_collate)
    dali_iter = get_classification_dali_it(it, device="gpu", device_id=0)
    check_dali_iterator(dali_iter, "cuda:0")


def test_to_dali_external_source_cpu(samples: List[int]) -> None:
    """Test for to_dali_external_source composable on cpu"""
    it = IterableSource(samples).to_dali_external_source(BATCH_SIZE, default_dict_collate)
    dali_iter = get_classification_dali_it(it, device="cpu")
    check_dali_iterator(dali_iter, "cpu")


def test_to_dali_external_source_gpu_multi_epoch(samples: List[int]) -> None:
    """Test for to_dali_external_source composable on cpu and multi-epoch loading"""
    it = IterableSource(samples).to_dali_external_source(BATCH_SIZE, default_dict_collate)
    dali_iter = get_classification_dali_it(it, device="gpu", device_id=0)

    num_epochs = 10
    counter = 0
    for _ in range(num_epochs):
        # iterable needs to be re-created explicitly
        dali_iter = get_classification_dali_it(it, device="gpu", device_id=0)
        for item in dali_iter:
            data = item[0]  # first pipeline
            counter += data[DATA_KEY].shape[0]

    assert counter == num_epochs * NUM_SAMPLES
