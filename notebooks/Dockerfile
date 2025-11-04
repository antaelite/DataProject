FROM jupyter/datascience-notebook

RUN pip install --upgrade pip

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

ENTRYPOINT ["jupyter","notebook","--NotebookApp.token=''", "--NotebookApp.password=''"]