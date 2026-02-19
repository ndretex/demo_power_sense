FROM python:3.11-slim

WORKDIR /usr/app
ENV DBT_PROFILES_DIR=/usr/app

COPY requirements.txt /usr/app/requirements.txt
RUN pip install --no-cache-dir -r /usr/app/requirements.txt

COPY . /usr/app

ENTRYPOINT ["dbt"]
CMD ["build", "--project-dir", "/usr/app"]
