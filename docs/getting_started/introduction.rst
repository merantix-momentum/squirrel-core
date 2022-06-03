Introduction
=======
What is Squirrel
-------
Squirrel is a data infrastructure library which enables ML teams to share, load, and transform data in a collaborative, flexible, and efficient way.

Why use Squirrel
-------

1. *Speed*
    Squirrel helps to avoid data stall, i.e. the expensive GPU will not be idle while waiting for the data. We use a couple of techniques, such as pre-fetching and distributed data loading to accelerate loading times. In addition, we support a cluster mode, allowing you to bundle the data and make loading even faster.
2. *Cost*
    Squirrel reduces cost mainly in 2 ways: First, it avoids GPU stalling, and second, it allows you to shard & cluster your data and store & load it in bundles, decreasing the cost for your data bucket cloud storage (S3, GCS).
3. *Flexibility*
    Squirrel works with a flexible standard data scheme which is adaptable to any setting, including multimodal data. Squirrel provides generic interfaces for easy integration with other Python tools & libraries.
4. *Collaboration*
    Squirrel makes it easier to share data & code between teams and projects in a self-service model. This way, it works really well with data mesh set-ups where individual teams own and serve their data. The data catalog is the entry point for accessing data & drivers for loading it

Squirrel Benchmarks
-------
We ran a few benchmarks to showcase Squirrel's competitive performance. No matter if you load image or text data, from your local storage or a remote bucket, Squirrel unfolds its speed reliably.
The results show the mean of 5 experiments. Error bars represent two standard deviations. The loading process was executed on an NVIDIA Tesla T4 GPU for image data (CIFAR-100) and on a CPU for text data (WikiText103-v1) with a batch size of 256 for all experiments.

