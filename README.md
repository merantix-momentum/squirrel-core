<div align="center">
  
# Squirrel Core
  
**Share, load, and transform data in a collaborative, flexible, and efficient way**

[![Python](https://img.shields.io/pypi/pyversions/squirrel-core.svg?style=plastic)](https://badge.fury.io/py/squirrel-core)
[![PyPI](https://badge.fury.io/py/squirrel-core.svg)](https://badge.fury.io/py/squirrel-core)
[![Downloads](https://pepy.tech/badge/squirrel-core)](https://pepy.tech/project/squirrel-core)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Documentation Status](https://readthedocs.org/projects/squirrel-core/badge/?version=latest)](https://squirrel-core.readthedocs.io)
[![Generic badge](https://img.shields.io/badge/Website-Merantix%20Momentum-blue.svg)](https://www.merantixlabs.com/)
[![Slack](https://img.shields.io/badge/slack-chat-green.svg?logo=slack)](https://join.slack.com/t/squirrel-core/shared_invite/zt-14k6sk6sw-zQPHfqAI8Xq5WYd~UqgNFw)

</div>

---

# What is Squirrel?

Squirrel is a Python library that enables ML teams to share, load, and transform data in a collaborative, flexible, and efficient way.

1. **SPEED:** Avoid data stall, i.e. the expensive GPU will not be idle while waiting for the data. 

2. **COSTS:** First, avoid GPU stalling, and second allow to shard & cluster your data and store & load it in bundles, decreasing the cost for your data bucket cloud storage.

3. **FLEXIBILITY:** Work with a flexible standard data scheme which is adaptable to any setting, including multimodal data.

4. **COLLABORATION:** Make it easier to share data & code between teams and projects in a self-service model.

If you have any questions or would like to contribute, join our [Slack community](https://join.slack.com/t/squirrel-core/shared_invite/zt-14k6sk6sw-zQPHfqAI8Xq5WYd~UqgNFw).

# Installation
You can install the latest stable version of Squirrel via pip:

```shell
pip install squirrel-core
```

Install the Squirrel [public dataset collection](https://github.com/merantix-momentum/squirrel-datasets-core) via:

```shell
pip install squirrel-datasets-core
```

# Documentation

You can visit https://docs.squirrel.merantixlabs.cloud/ to access the documentation of squirrel (login with your @merantix e-mail account).

The documentation is built & deployed automatically via cloudbuild for the master-branch and tags. Please find more information on that topic [here](https://docs.squirrel.merantixlabs.cloud/usage/document.html).

Alternatively, build the documentation locally:
```
cd squirrel/
mx build_docs
```

# Examples
Check out the [Squirrel-datasets repository](https://github.com/merantix-momentum/squirrel-datasets-core/tree/main/examples) for open source and community-contributed examples of using Squirrel.

# Contributing
Squirrel is open source and community contributions are welcome!

Check out the [contribution guide](https://docs.squirrel.merantixlabs.cloud/usage/contribute.html) to learn how to get involved.

# The humans behind Squirrel
We are [Merantix Labs](https://merantixlabs.com/), a team of ~30 machine learning engineers, developing machine learning solutions for industry and research. Each project comes with its own challenges, data types and learnings, but one issue we always faced was scalable data loading, transforming and sharing. We were looking for a solution that would allow us to load the data in a fast and cost-efficient way, while keeping the flexibility to work with any possible dataset and integrate with any API. That's why we build Squirrel – and we hope you'll find it as useful as we do! By the way, [we are hiring](https://www.merantixlabs.com/career)!


# Citation

If you use Squirrel in your research, please cite it using:
```bibtex
@article{2022squirrelcore,
  title={Squirrel: A Python library that enables ML teams to share, load, and transform data in a collaborative, flexible, and efficient way.},
  author={Squirrel Developer Team},
  journal={GitHub. Note: https://github.com/merantix-momentum/squirrel-core},
  year={2022}
}
```
