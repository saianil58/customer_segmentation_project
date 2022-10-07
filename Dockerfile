# Get the python image
FROM python:3.8

# make the direcroty inside the container
WORKDIR /code/app

# copy the requirments file from local to container
COPY ./requirements.txt /code/requirements.txt

# install all the libraries from the requirements file
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

# Copy the code for the api and pipelines
COPY ./app /code/app

# Run the server on the container for making requests
CMD ["uvicorn", "main:app",  "--host", "0.0.0.0", "--port", "80"]
# docker build -t myimage .
# docker run -d --name mycontainer -p 80:80 myimage