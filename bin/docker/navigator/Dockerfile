FROM python:3.10.0b2-slim-buster
RUN ln -fs /usr/share/zoneinfo/America/New_York /etc/localtime && dpkg-reconfigure --frontend noninteractive tzdata
RUN apt-get -y update && apt-get -y install git  make automake gcc g++ subversion libpq-dev postgresql-server-dev-11 \
    zlib1g-dev libblas-dev  liblapack-dev gfortran libxml2-dev libxslt1-dev nginx supervisor
RUN useradd --create-home troc
WORKDIR /home/troc
USER troc
ENV VIRTUAL_ENV=/home/troc/venv
RUN python -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
RUN python -m pip install --upgrade pip
COPY --chown=troc ./ /home/troc/code/
RUN python -m pip install --upgrade pip && python -m pip install -r /home/troc/code/requirements/requirements.txt
RUN cd /home/troc/code/extensions/asyncdb/ && python setup.py install && cd /home/troc/code/extensions/query_api/ && python setup.py install
CMD ["python", "/home/troc/code/manage.py", "runserver", "0.0.0.0:8000"]
