# credmark-risk-and-research

This respository is used for resources for Credmark's Research and Risk Analysis Team

## Developing with Poetry

[Poetry](https://python-poetry.org/) is a very useful and modern package and dependency management tool. Installation instructions can be found [here](https://python-poetry.org/docs/#installation). Once `poetry` is installed, to install this project locally, run the following:

```shell
poetry install
```

This will install all the package dependencies as defined in [pyproject.toml](./pyproject.toml). If you need to add a new package, instead of using `pip` as you normally would you would run the following:

```shell
poetry add numpy
```

> *Note: this is the equivalent of running `pip install numpy` but the package dependencies will be added to [pyproject.toml](./pyproject.toml) and any conflicts with other packages will be identified and/or resolved automatically for you.

### Virtual environment

`poetry` will create a separate environment per project. If you want to run the code locally using the packages installed in the project, you need to activate the environment by running:

```shell
poetry shell
```

This will activate the environment where all the project dependencies are installed. To exit the environment, just run `exit` in the terminal.

### requirements.txt

Finally, the [Dockerfile](./Dockerfile) that builds the image used for the lambdas requires a `requirements.txt` file (for now; will look to make this a smoother workflow in the future). If new project dependencies are added to the [pyproject.toml](./pyproject.toml) file by running `poetry add <PACKAGE>` then you will need to update the `requirements.txt` file by running:

```shell
poetry export -f requirements.txt > requirements.txt --without-hashes
```

## Using Serverless to Deploy

In order to use serverless and deploy a new lambda, you will need both serverless and docker installed.

### Installation

The easiest way to install `serverless` is by running the following command:

```shell
npm install -g serverless
```

For other methods of installing `serverless` please refer to the [docs](https://www.serverless.com/framework/docs/getting-started).

Next, you will need to ensure you have docker installed. Instructions on how to install docker can be found [here](https://docs.docker.com/get-docker/).

### Deploying

To deploy all the lambdas defined in [serverless.yml](./serverless.yml), simply run:

```shell
serverless deploy --aws-profile PROFILE_NAME
```

where "PROFILE_NAME" is the name of a profile defined in `~/.aws/credentials`. For help setting up a new profile, take a look [here](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html).

By default, `serverless` will try to deploy the lambdas to "us-west-2". The region can be overriden by passing in the `--region` option like so:

```shell
serverless deploy --aws-profile PROFILE_NAME --region REGION_NAME
```

### Adding a new lambda

Adding a new lambda for deployment via `serverless` is simple. First, you will need to add a new handler to the [handlers.py](./src/handlers.py) file by importing the lambda handler from the correct path. For example, if you add a new VAR model for Cream Finance, you would add a new file called `src/protocol/cream/var.py` which could then be imported like this:

```python
from protocol.cream.var import lambda_handler as cream_var
```

And then simply define a new handler that executes `cream_var`:

```python
def cream_var_handler(event, context):
    cream_var(event, context)
```

Finally, you just need to add the new function in [serverless.yml](./serverless.yml) under the "functions" key like so:

```yaml
functions:
  cream-var:
    image:
      name: "baseRiskMetrics"
      command: ["handlers.cream_var_handler"]  # Change the command to use the name of the handler defined in handlers.py
    events:
      - http:
          path: /lcr/cream  # Update the path to reflect the correct protocol and metric being used
          method: GET
```

> *Note: adding the `events` key will create a new API gateway endpoint where the lambda can be triggered. In the case above, this would create a new endpoint with the path `some-url.com/dev/lcr/cream`*

Finally, run the `serverless deploy` command shown in the "Deploying" section.

## Reserach Development

This contains code and research resources currently at development stage.
