<div align="center">

# <img src="https://raw.githubusercontent.com/merantix-momentum/squirrel-core/main/docs/_static/logo.png" width="150px"> Squirrel Core

**Share, load, and transform data in a collaborative, flexible, and efficient way**

[![Python](https://img.shields.io/pypi/pyversions/squirrel-core.svg?style=plastic)](https://badge.fury.io/py/squirrel-core)
[![PyPI](https://img.shields.io/pypi/v/squirrel-core?label=pypi%20package)](https://pypi.org/project/squirrel-core/)
[![Conda](https://img.shields.io/conda/vn/conda-forge/squirrel-core)](https://anaconda.org/conda-forge/squirrel-core)
[![Documentation Status](https://readthedocs.org/projects/squirrel-core/badge/?version=latest)](https://squirrel-core.readthedocs.io/en/latest)
[![Downloads](https://static.pepy.tech/personalized-badge/squirrel-core?period=total&units=international_system&left_color=grey&right_color=blue&left_text=Downloads)](https://pepy.tech/project/squirrel-core)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://raw.githubusercontent.com/merantix-momentum/squirrel-core/main/LICENSE)
[![DOI](https://zenodo.org/badge/458099869.svg)](https://zenodo.org/badge/latestdoi/458099869)
[![Generic badge](https://img.shields.io/badge/Website-Merantix%20Momentum-blue)](https://merantix-momentum.com)
[![Slack](https://img.shields.io/badge/slack-chat-green.svg?logo=slack)](https://join.slack.com/t/squirrel-core/shared_invite/zt-14k6sk6sw-zQPHfqAI8Xq5WYd~UqgNFw)

</div>

---

## What is Squirrel?

Squirrel is a Python library that enables ML teams to share, load, and transform data in a collaborative, flexible, and efficient way.

1. **SPEED:** Avoid data stall, i.e. the expensive GPU will not be idle while waiting for the data.

2. **COSTS:** First, avoid GPU stalling, and second allow to shard & cluster your data and store & load it in bundles, decreasing the cost for your data bucket cloud storage.

3. **FLEXIBILITY:** Work with a flexible standard data scheme which is adaptable to any setting, including multimodal data.

4. **COLLABORATION:** Make it easier to share data & code between teams and projects in a self-service model.

Stream data from anywhere to your machine learning model as easy as:
```python
it = (
    Catalog.from_plugins()["imagenet"]
    .get_driver()
    .get_iter("train")
    .map(lambda r: (augment(r["image"]), r["label"]))
    .batched(100)
)
```

Check out our full [getting started](https://github.com/merantix-momentum/squirrel-datasets-core/blob/main/examples/01.Getting_Started.ipynb) tutorial notebook. If you have any questions or would like to contribute, join our [Slack community](https://join.slack.com/t/squirrel-core/shared_invite/zt-14k6sk6sw-zQPHfqAI8Xq5WYd~UqgNFw).

## Installation
You can install `squirrel-core` by
```shell
pip install squirrel-core
```

To install all features and functionalities:

```shell
pip install "squirrel-core[all]"
```

Or select the dependencies you need:

```shell
pip install "squirrel-core[gcs,torch]"
```

Please refer to the [installation](https://squirrel-core.readthedocs.io/en/latest/getting_started/installation.html) 
section of the documentation for a complete list of supported dependencies.

## Documentation

Read our documentation at [ReadTheDocs](https://squirrel-core.readthedocs.io/en/latest)

## Squirrel Datasets

[Squirrel-datasets-core](https://github.com/merantix-momentum/squirrel-datasets-core) is an accompanying Python package that does three things.
1. It extends the Squirrel platform for data transform, access, and discovery through custom drivers for public datasets. 
2. It also allows you to tap into the vast amounts of open-source datasets from [Huggingface](https://huggingface.co/), [Activeloop Hub](https://www.activeloop.ai/) and [Torchvision](https://pytorch.org/vision/stable/datasets.html), and you'll get all of Squirrel's functionality on top!
3. It provides open-source and community-contributed [tutorials and example notebooks](https://github.com/merantix-momentum/squirrel-datasets-core/tree/main/examples) for using Squirrel.

## Contributing
Squirrel is open source and community contributions are welcome!

Check out the [contribution guide](https://squirrel-core.readthedocs.io/en/latest/developer/contribute.html) to learn how to get involved.

## The Humans Behind Squirrel
We are [Merantix Momentum](https://merantix-momentum.com/), a team of ~30 machine learning engineers, developing machine learning solutions for industry and research. Each project comes with its own challenges, data types and learnings, but one issue we always faced was scalable data loading, transforming and sharing. We were looking for a solution that would allow us to load the data in a fast and cost-efficient way, while keeping the flexibility to work with any possible dataset and integrate with any API. That's why we build Squirrel – and we hope you'll find it as useful as we do! By the way, [we are hiring](https://merantix-momentum.com/about#jobs)!


## Citation

If you use Squirrel in your research, please cite it using:
```bibtex
@article{2022squirrelcore,
  title={Squirrel: A Python library that enables ML teams to share, load, and transform data in a collaborative, flexible, and efficient way.},
  author={Squirrel Developer Team},
  journal={GitHub. Note: https://github.com/merantix-momentum/squirrel-core},
  doi={10.5281/zenodo.6418280},
  year={2022}
}
```
