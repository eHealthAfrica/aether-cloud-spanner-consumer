FROM python:3.8-slim-buster

################################################################################
## setup container
################################################################################

################################################################################
## install app
## copy files one by one and split commands to use docker cache
################################################################################

RUN apt-get update -qq > /dev/null && \
    apt-get -qq \
        --yes \
        --allow-downgrades \
        --allow-remove-essential \
        --allow-change-held-packages \
        install gcc build-essential > /dev/null && \
    pip install -q --upgrade pip

WORKDIR /code

COPY ./conf/pip/requirements.txt /code/conf/pip/requirements.txt
RUN pip3 install -r /code/conf/pip/requirements.txt

COPY ./ /code

################################################################################
## last setup steps
################################################################################

# create user to run container (avoid root user)
RUN useradd -ms /bin/false aether
RUN chown -R aether: /code

ENTRYPOINT ["/code/entrypoint.sh"]
