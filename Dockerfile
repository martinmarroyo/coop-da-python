FROM python:3.9
# Set working directory
WORKDIR /src/python-env
# Set python path to import from src 
ENV PYTHONPATH=/src
# Set up environment
RUN apt update
RUN apt upgrade -y
RUN apt install nano -y
# Generate SSH key to link w/ Github
RUN ssh-keygen -t rsa -f ~/.ssh/git-key -q -N '""'
# Copy code files
COPY ../.. /src
COPY initial_etl.py .
COPY etl.py .
COPY tables.yml .
COPY .env .
COPY sql.py .
RUN pip install -r ../requirements.txt
# Set access token for Jupyter server
ENV JUPYTER_TOKEN=coopda
# Keep our container running
CMD ["tail", "-f", "/dev/null"]
