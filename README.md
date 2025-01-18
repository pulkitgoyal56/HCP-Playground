# HCP-Playground

This project explores the [Human Connectome Project](http://www.humanconnectomeproject.org/data/) dataset using latest libraries and software (as of September 2022).

- The [prfresults](./prfresults.ipynb) notebook is the main file, which contains analysis and graphs related to study the structural mapping of visual receptive fields in the occipital lobe.
- The [utils](./utils.py) file defined the `HCP_S3` class that can be used to explore and download datasets from the HCP AWS S3 buckets neatly.
- The [playground][./playground.ipynb] notebook shows a minimal example of using the `HCP_S3` class.
