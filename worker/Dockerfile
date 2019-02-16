FROM python:3.7-alpine
ENV MASTER_SOCKET_PORT=5000
ADD worker /code
WORKDIR /code
ADD data /data
ADD output /output
RUN pip install -r requirements.txt
CMD ["python", "-u", "app.py"]