FROM ghcr.io/hcdp/task-base:latest
LABEL org.opencontainers.image.source="https://github.com/ikewai/c14n"
LABEL org.opencontainers.image.description="Ingestion container for station values."

# Install curl, prerequisite for get_auth_token.py.
RUN apt update
RUN apt install -y curl

RUN mkdir -p /home/hcdp_tapis_ingestor
RUN mkdir /actor

ADD src /home/hcdp_tapis_ingestor

# Copy the file downloader, make /ingest directory
ADD /utils/* /actor

CMD [ "/bin/bash", "/actor/task.sh" ]