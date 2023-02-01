FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

ARG WORKDIR=/dataflow/template
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

COPY . .

# Do not include `apache-beam` in requirements.txt
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="${WORKDIR}/setup.py"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/etl_beam_app/dataflows/sample_dataflow/sample_dataflow.py"

# Install apache-beam and other dependencies to launch the pipeline
RUN pip install --upgrade pip
RUN pip install apache-beam[gcp]
RUN pip install .
