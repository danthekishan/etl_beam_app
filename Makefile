
GCP_PROJECT ?= learn-de-370612
GCP_REGION ?= asia-southeast1
TEMPLATE_IMAGE ?= ${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT}/etl_beam_app/dataflow/sample_dataflow:latest

test-template: ## Test the Integrity of the Flex Container
	@gcloud config set project ${GCP_PROJECT}
	@gcloud auth configure-docker  ${GCP_REGION}-docker.pkg.dev
	@docker pull ${TEMPLATE_IMAGE}
	# @echo "Checking if ENV Var FLEX_TEMPLATE_PYTHON_PY_FILE is Available" && docker run --rm --entrypoint /bin/bash ${TEMPLATE_IMAGE} -c 'env|grep -q "FLEX_TEMPLATE_PYTHON_PY_FILE" && echo ✓'
	# @echo "Checking if ENV Var FLEX_TEMPLATE_PYTHON_SETUP_FILE is Available" && docker run --rm --entrypoint /bin/bash ${TEMPLATE_IMAGE} -c 'env|grep -q "FLEX_TEMPLATE_PYTHON_PY_FILE" && echo ✓'
	# @echo "Checking if Driver Python File (main.py) Found on Container" && docker run --rm --entrypoint /bin/bash ${TEMPLATE_IMAGE} -c "/usr/bin/test -f ${FLEX_TEMPLATE_PYTHON_PY_FILE} && echo ✓"
	# @echo "Checking if setup.py File Found on Container" && docker run --rm --entrypoint /bin/bash ${TEMPLATE_IMAGE} -c 'test -f ${FLEX_TEMPLATE_PYTHON_SETUP_FILE} && echo ✓'
	# @echo "Checking if Package Installed on Container" && docker run --rm --entrypoint /bin/bash ${TEMPLATE_IMAGE} -c 'python -c "import beam_flex_demo" && echo ✓'
	# @echo "Checking if UDFs Installed on Container" && docker run --rm --entrypoint /bin/bash ${TEMPLATE_IMAGE} -c 'python -c "from beam_flex_demo.utils.common_functions import split_element" && echo ✓'
	@echo "Running Pipeline Locally..." && docker run --rm --entrypoint /bin/bash ${TEMPLATE_IMAGE} -c "python ${FLEX_TEMPLATE_PYTHON_PY_FILE} --runner DirectRunner --input_file etl_beam_app/data/input_data.json --output_file output.txt && cat output.txt*"
