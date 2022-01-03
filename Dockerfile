# Define custom function directory
ARG FUNCTION_DIR="/function"

FROM python:3.8-slim-buster as build-image

# Include global arg in this stage of the build
ARG FUNCTION_DIR

# Install aws-lambda-cpp build dependencies
RUN apt-get update && \
  apt-get install -y \
  g++ \
  make \
  cmake \
  unzip \
  libcurl4-openssl-dev

# Install the function's dependencies
RUN pip install \
    --target ${FUNCTION_DIR} \
        awslambdaric

# Copy function code
RUN mkdir -p ${FUNCTION_DIR}
COPY src/* ${FUNCTION_DIR}
COPY requirements.txt ${FUNCTION_DIR}

RUN pip install \
    --target ${FUNCTION_DIR} \
    -r ${FUNCTION_DIR}/requirements.txt

FROM python:3.8-slim-buster

# Include global arg in this stage of the build
ARG FUNCTION_DIR
# Set working directory to function root directory
WORKDIR ${FUNCTION_DIR}

# Copy in the built dependencies
COPY --from=build-image ${FUNCTION_DIR} ${FUNCTION_DIR}

ENTRYPOINT [ "/usr/local/bin/python", "-m", "awslambdaric" ]

# You can overwrite command in `serverless.yml` template
CMD ["handlers.aave_lcr_handler"]
