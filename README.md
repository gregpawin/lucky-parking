# Lucky Parking Car Make Prediction Project

Prototype car make prediction project as offshoot of [Hack for LA's Lucky Parking Project](https://www.hackforla.org/projects/lucky-parking). A Plotly-based webapp that predicts a user's car make from user input
based on parking citation data collected by the [City of Los Angeles](https://data.lacity.org/A-Well-Run-City/Parking-Citations/wjz9-h9np).


## Instructions on setting up the project enviornment and how to download the original dataset

- Run `make create_environment`
- Make sure that your new lucky-parking-analysis environment is activated
- If you're using a Mac, you might have to install some certificates first: https://stackoverflow.com/questions/52805115/certificate-verify-failed-unable-to-get-local-issuer-certificate
- Run `make data` to download raw citation data
	- Raw data will be in data/raw
	- Sampled data will be in data/interim (default 10% sampling from full dataset)
	- Cleaned data will be in data/processed
- To create a smaller or larger sample from full dataset use: `make sample frac={your fraction here} clean=False`
    - Use `make sample frac={your fraction here} clean=True`, for cleaned dataset that ends up in processed
    - For 15% sample that is cleaned: `make sample frac=0.15 clean=True`

Project Organization
------------

    ├── LICENSE
    ├── Makefile           <- Makefile with commands like `make data` or `make train`
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── external       <- Data from third party sources.
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- The final, canonical data sets for modeling.
    │   └── raw            <- The original, immutable data dump.
    │
    ├── docs               <- A default Sphinx project; see sphinx-doc.org for details
    │
    ├── models             <- Trained and serialized models, model predictions, or model summaries
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `1.0-jqp-initial-data-exploration`.
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   ├── data           <- Scripts to download or generate data
    │   │   └── make_dataset.py
    │   │
    │   ├── features       <- Scripts to turn raw data into features for modeling
    │   │   └── build_features.py
    │   │
    │   ├── models         <- Scripts to train models and then use trained models to make
    │   │   │                 predictions
    │   │   ├── predict_model.py
    │   │   └── train_model.py
    │   │
    │   └── visualization  <- Scripts to create exploratory and results oriented visualizations
    │       └── visualize.py
    │
    └── tox.ini            <- tox file with settings for running tox; see tox.readthedocs.io
    
--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>



